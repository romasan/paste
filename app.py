#!/usr/bin/env python3
"""s.tw14.ru - minimal paste service with 2-character IDs and TTL.

Endpoints:
  GET  /               -> form
  POST /api/share      -> JSON {text, ttl, burn} or multipart (text, ttl, burn, files)
                          => {id, url, expires_at, delete_url, files}
  GET  /<id>           -> rendered paste
  GET  /<id>/raw       -> text/plain
  GET  /<id>/f/<n>     -> attachment download
  GET  /d/<token>      -> delete paste (one-time)
  GET  /healthz        -> ok
"""

import email.policy
import html
import json
import os
import random
import re
import secrets
import sqlite3
import string
import threading
import time
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, urlsplit, parse_qs

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "paste.db")
HOST = "127.0.0.1"
PORT = int(os.environ.get("PASTE_PORT", "8091"))

ALPHABET = string.digits + string.ascii_letters  # 62 -> 62^2 = 3844 ids
ID_LEN = int(os.environ.get("PASTE_ID_LEN", "2"))
MAX_BYTES = 1024 * 1024          # 1 MiB per paste
MAX_UPLOAD_BYTES = 25 * 1024 * 1024  # whole multipart request
MAX_FILE_BYTES = 10 * 1024 * 1024    # one attachment
MAX_FILES = 5                        # attachments per paste
MAX_FILES_TOTAL = 20 * 1024 * 1024   # all attachments together
MAX_STORAGE_BYTES = 20 * 1024 * 1024 * 1024  # service-wide attachment cap
MAX_UPLOAD_PER_HOUR = 200 * 1024 * 1024      # bytes per IP per hour
BURN_GRACE = 600                     # seconds to grab files from a burn paste
MAX_TTL = 30 * 24 * 3600         # 30 days
DEFAULT_TTL = 24 * 3600
ALLOWED_TTL = {
    "5m": 300, "15m": 900, "1h": 3600, "6h": 21600,
    "12h": 43200, "1d": 86400, "3d": 259200, "7d": 604800, "30d": 2592000,
}

os.makedirs(DATA_DIR, exist_ok=True)


def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pastes (
                id           TEXT PRIMARY KEY,
                body         TEXT NOT NULL,
                created_at   INTEGER NOT NULL,
                expires_at   INTEGER NOT NULL,
                burn         INTEGER NOT NULL DEFAULT 0,
                delete_token TEXT NOT NULL,
                views        INTEGER NOT NULL DEFAULT 0,
                is_markdown  INTEGER NOT NULL DEFAULT 0
            )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_expires ON pastes(expires_at)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                paste_id TEXT NOT NULL REFERENCES pastes(id) ON DELETE CASCADE,
                name     TEXT NOT NULL,
                ctype    TEXT NOT NULL,
                size     INTEGER NOT NULL,
                data     BLOB NOT NULL
            )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_files_paste ON files(paste_id)")


def purge_expired(conn=None):
    own = conn is None
    if own:
        conn = db()
    try:
        cur = conn.execute("DELETE FROM pastes WHERE expires_at <= ?", (int(time.time()),))
        if own:
            conn.commit()
        return cur.rowcount
    finally:
        if own:
            conn.close()


def all_ids():
    total = len(ALPHABET) ** ID_LEN
    for n in range(total):
        s = ""
        v = n
        for _ in range(ID_LEN):
            s = ALPHABET[v % len(ALPHABET)] + s
            v //= len(ALPHABET)
        yield s


def new_id(conn):
    """Pick a free short id; reuse freed slots after purging expired rows."""
    for _ in range(200):
        cand = "".join(random.choice(ALPHABET) for _ in range(ID_LEN))
        row = conn.execute("SELECT 1 FROM pastes WHERE id = ?", (cand,)).fetchone()
        if not row:
            return cand
    # Dense disk: purge expired, then take the first free slot deterministically.
    purge_expired(conn)
    used = {r[0] for r in conn.execute("SELECT id FROM pastes")}
    for cand in all_ids():
        if cand not in used:
            return cand
    return None  # namespace exhausted


# ---------------------------------------------------------------- rate limit
class RateLimiter:
    def __init__(self, per_minute):
        self.per_minute = per_minute
        self.hits = {}
        self.lock = threading.Lock()

    def allow(self, key):
        now = time.time()
        with self.lock:
            bucket = self.hits.setdefault(key, [])
            bucket[:] = [t for t in bucket if now - t < 60]
            if len(bucket) >= self.per_minute:
                return False
            bucket.append(now)
            if len(self.hits) > 5000:
                for k in [k for k, v in self.hits.items() if not v]:
                    del self.hits[k]
            return True


class ByteQuota:
    """Rolling per-key byte budget (in-memory, resets every hour)."""

    def __init__(self, limit, window=3600):
        self.limit = limit
        self.window = window
        self.entries = {}
        self.lock = threading.Lock()

    def allow(self, key, n):
        now = time.time()
        with self.lock:
            start, used = self.entries.get(key, (now, 0))
            if now - start >= self.window:
                start, used = now, 0
            if used + n > self.limit:
                return False
            self.entries[key] = (start, used + n)
            if len(self.entries) > 5000:
                for k in [k for k, (s, _) in self.entries.items() if now - s >= self.window]:
                    del self.entries[k]
            return True


rl_write = RateLimiter(30)
rl_read = RateLimiter(240)
rl_upload_bytes = ByteQuota(MAX_UPLOAD_PER_HOUR)

CTA = "s.tw14.ru"


def render(tpl, **values):
    """Fill {placeholders} in one pass so literal braces in CSS/JS survive."""
    return re.sub(r"\{([a-z_]+)\}", lambda m: str(values.get(m.group(1), m.group(0))), tpl)


def page(tpl, **values):
    values.setdefault("max_file_mb", MAX_FILE_BYTES // (1024 * 1024))
    values.setdefault("max_total_mb", MAX_FILES_TOTAL // (1024 * 1024))
    values.setdefault("max_files", MAX_FILES)
    return render(tpl, css=PAGE_CSS, cta=CTA, **values)


PAGE_CSS = """
:root{--bg:#0f1115;--card:#171a21;--fg:#e6e6e6;--dim:#8b93a7;--acc:#6ea8fe;--line:#242938}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
 font:15px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Ubuntu,sans-serif}
.wrap{max-width:880px;margin:0 auto;padding:32px 18px 64px}
h1{font-size:19px;font-weight:600;margin:0 0 4px}
h1 a{color:var(--fg);text-decoration:none}
.sub{color:var(--dim);font-size:13px;margin-bottom:22px}
textarea{width:100%;min-height:320px;background:var(--card);color:var(--fg);
 border:1px solid var(--line);border-radius:10px;padding:14px;resize:vertical;
 font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;outline:none}
textarea:focus{border-color:var(--acc)}
.row{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-top:12px}
select,button{background:var(--card);color:var(--fg);border:1px solid var(--line);
 border-radius:8px;padding:9px 12px;font-size:14px;cursor:pointer}
button.primary{background:var(--acc);border-color:var(--acc);color:#0b1020;font-weight:600}
button.primary:disabled{opacity:.5;cursor:default}
label.chk{display:flex;gap:7px;align-items:center;color:var(--dim);font-size:14px;cursor:pointer}
pre{margin:0;background:var(--card);border:1px solid var(--line);border-radius:10px;
 padding:16px;overflow:auto;font:13px/1.55 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
 white-space:pre-wrap;word-break:break-word}
.meta{display:flex;gap:14px;flex-wrap:wrap;color:var(--dim);font-size:13px;
 margin:0 0 14px;align-items:center}
.meta a{color:var(--acc);text-decoration:none}
.err{background:#3a1720;border:1px solid #6b2434;color:#ffb4c0;
 padding:11px 14px;border-radius:8px;margin-bottom:14px;font-size:14px}
.ok{background:#132a1e;border:1px solid #24523a;color:#b6f0cd;
 padding:14px;border-radius:8px;font-size:14px;margin-top:14px}
.ok a{color:#8ef0b4;font-size:17px;font-weight:600;text-decoration:none;word-break:break-all}
.kbd{font:12px ui-monospace,monospace;background:#20242f;border:1px solid var(--line);
 border-radius:5px;padding:1px 5px;color:var(--dim)}
footer{margin-top:34px;color:var(--dim);font-size:12px}
.files{margin:0 0 14px;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.files-h{font-weight:600;font-size:13px;margin-bottom:8px}
.files ul{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:6px}
.files li{display:flex;justify-content:space-between;gap:12px;align-items:center;min-width:0}
.files a{color:var(--acc);text-decoration:none;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.dim{color:var(--dim)}
.flist{list-style:none;margin:10px 0 0;padding:0;display:flex;flex-direction:column;gap:6px}
.flist li{display:flex;gap:10px;align-items:center;background:var(--card);border:1px solid var(--line);border-radius:8px;padding:7px 10px;font-size:13px}
.flist li span:first-child{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.flist button{background:none;border:none;color:var(--dim);font-size:16px;line-height:1;padding:0 2px;cursor:pointer}
.flist button:hover{color:#ffb4c0}
body.dragging .wrap{outline:2px dashed var(--acc);outline-offset:10px;border-radius:14px}
"""

FORM_HTML = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{cta} — поделиться текстом</title><style>{css}</style></head>
<body><div class="wrap">
<h1><a href="/">{cta}</a></h1>
<div class="sub">Вставь текст или прикрепи файлы, выбери время жизни — получишь короткую ссылку.</div>
{error}
<textarea id="t" placeholder="текст, лог, кусок кода… (можно оставить пустым, если есть файлы)" autofocus></textarea>
<div class="row">
  <input type="file" id="f" multiple hidden>
  <button type="button" id="pick">📎 Прикрепить файлы</button>
  <span class="dim" id="flim">до {max_files} файлов, {max_file_mb} МБ каждый</span>
</div>
<ul class="flist" id="flist"></ul>
<div class="row">
  <select id="ttl">
    <option value="5m">5 минут</option>
    <option value="15m">15 минут</option>
    <option value="1h">1 час</option>
    <option value="6h">6 часов</option>
    <option value="12h">12 часов</option>
    <option value="1d" selected>1 день</option>
    <option value="3d">3 дня</option>
    <option value="7d">7 дней</option>
    <option value="30d">30 дней</option>
  </select>
  <label class="chk"><input type="checkbox" id="burn"> удалить после прочтения</label>
  <button class="primary" id="go">Создать ссылку</button>
  <span class="kbd">Ctrl+Enter</span>
</div>
<div id="out"></div>
<footer>Без аккаунтов. Ссылки случайные и живут до указанного срока. Файлы — до {max_file_mb} МБ каждый, всего до {max_total_mb} МБ.</footer>
</div>
<script>
const t=document.getElementById('t'),go=document.getElementById('go'),out=document.getElementById('out'),
      f=document.getElementById('f'),pick=document.getElementById('pick'),flist=document.getElementById('flist');
const MAXF={max_files},MAXFB={max_file_mb}*1048576,MAXTOT={max_total_mb}*1048576;
let picked=[];
function esc(s){return String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}
function human(n){return n<1024?n+' Б':n<1048576?(n/1024).toFixed(1)+' КБ':(n/1048576).toFixed(1)+' МБ';}
function draw(){
  flist.innerHTML=picked.map((x,i)=>'<li><span>'+esc(x.name)+'</span><span class="dim">'+human(x.size)+
    '</span><button type="button" data-i="'+i+'" title="убрать">×</button></li>').join('');
}
function addFiles(list){
  picked=picked.concat([...list]);
  if(picked.length>MAXF){picked=picked.slice(0,MAXF);fail('не больше '+MAXF+' файлов');}
  draw();
}
pick.onclick=()=>f.click();
f.onchange=()=>{addFiles(f.files);f.value='';};
flist.onclick=e=>{const b=e.target.closest('button[data-i]');if(!b)return;picked.splice(+b.dataset.i,1);draw();};
['dragenter','dragover'].forEach(ev=>document.addEventListener(ev,e=>{
  e.preventDefault();document.body.classList.add('dragging');}));
['dragleave','drop'].forEach(ev=>document.addEventListener(ev,e=>{
  if(ev==='drop'||e.target===document.documentElement)document.body.classList.remove('dragging');}));
document.addEventListener('drop',e=>{e.preventDefault();
  if(e.dataTransfer&&e.dataTransfer.files.length)addFiles(e.dataTransfer.files);});
function fail(msg){out.innerHTML='<div class="err">'+esc(msg)+'</div>';}
function send(){
  if(go.disabled)return;
  if(!t.value.trim()&&!picked.length){t.focus();return;}
  let total=0;
  for(const x of picked){total+=x.size;
    if(x.size>MAXFB){fail('файл «'+x.name+'» больше '+(MAXFB/1048576)+' МБ');return;}}
  if(total>MAXTOT){fail('файлы весят больше '+(MAXTOT/1048576)+' МБ');return;}
  const fd=new FormData();
  fd.append('text',t.value);
  fd.append('ttl',document.getElementById('ttl').value);
  fd.append('burn',document.getElementById('burn').checked?'1':'0');
  picked.forEach(x=>fd.append('files',x,x.name));
  go.disabled=true;go.textContent=picked.length?'Загружаю… 0%':'Создаю…';
  const xhr=new XMLHttpRequest();
  xhr.open('POST','/api/share');
  xhr.upload.onprogress=e=>{if(e.lengthComputable)
    go.textContent='Загружаю… '+Math.round(e.loaded/e.total*100)+'%';};
  function reset(){go.disabled=false;go.textContent='Создать ссылку';}
  xhr.onload=()=>{
    let j={};try{j=JSON.parse(xhr.responseText);}catch(_){}
    if(xhr.status<200||xhr.status>=300){fail(j.error||('ошибка '+xhr.status));reset();return;}
    out.innerHTML='<div class="ok">Ссылка готова:<br><a href="'+j.url+'" target="_blank">'+esc(j.url)+
      '</a><br><span style="color:#8b93a7;font-size:13px">живёт до '+esc(j.expires_human)+
      (j.files?' · файлов: '+j.files:'')+
      ' · <a href="'+j.delete_url+'" style="color:#ffb4c0">удалить</a></span></div>';
    reset();
  };
  xhr.onerror=()=>{fail('сеть недоступна');reset();};
  xhr.send(fd);
}
go.onclick=send;
t.addEventListener('keydown',e=>{if((e.ctrlKey||e.metaKey)&&e.key==='Enter')send();});
</script></body></html>"""

VIEW_HTML = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{id} — {cta}</title><style>{css}</style></head>
<body><div class="wrap">
<h1><a href="/">{cta}</a></h1>
<div class="meta">
  <span><b style="color:#e6e6e6">{id}</b></span>
  <span>до {expires_human}</span>
  <span>{size}</span>
  <a href="/{id}/raw">raw</a>
  <a href="/{id}/raw?download=1">скачать</a>
  {copy_link}
</div>
{files}
{body_block}
<footer>Исчезнет {expires_human}.</footer>
</div>
<script>
const cp=document.getElementById('cp');
if(cp)cp.onclick=async e=>{e.preventDefault();
  try{await navigator.clipboard.writeText(document.getElementById('body').textContent);
    e.target.textContent='скопировано';setTimeout(()=>e.target.textContent='копировать',1400);}
  catch(_){}};
</script></body></html>"""

NOT_FOUND_HTML = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>нет такой ссылки — {cta}</title><style>{css}</style></head>
<body><div class="wrap"><h1><a href="/">{cta}</a></h1>
<div class="err">Такой ссылки нет — либо она истекла, либо её уже прочитали и удалили.</div>
<p><a href="/" style="color:#6ea8fe">создать новую</a></p></div></body></html>"""


def human_delta(seconds):
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} сек"
    if seconds < 3600:
        return f"{seconds // 60} мин"
    if seconds < 86400:
        h = seconds // 3600
        return f"{h} ч"
    return f"{seconds // 86400} дн"


def human_at(ts):
    return time.strftime("%d.%m %H:%M", time.localtime(ts))


def human_size(n):
    n = max(0, int(n))
    if n < 1024:
        return f"{n} Б"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} КБ"
    return f"{n / (1024 * 1024):.1f} МБ"


def clean_filename(name):
    """Keep just a safe basename: no path, no control chars, bounded length."""
    name = (name or "").replace("\\", "/").split("/")[-1]
    name = re.sub(r"[\x00-\x1f\x7f]", "", name).strip().strip(".")
    return name[:180] or "file"


def clean_ctype(ctype):
    ctype = (ctype or "application/octet-stream").split(";")[0].strip().lower()
    if not re.fullmatch(r"[a-z0-9.+-]+/[a-z0-9.+-]+", ctype):
        return "application/octet-stream"
    return ctype


def attachment_list(pid, rows):
    if not rows:
        return ""
    items = "".join(
        f'<li><a href="/{pid}/f/{r["id"]}">{html.escape(r["name"])}</a>'
        f' <span class="dim">{human_size(r["size"])}</span></li>'
        for r in rows
    )
    total = sum(r["size"] for r in rows)
    return (f'<div class="files"><div class="files-h">Файлы '
            f'<span class="dim">{len(rows)} · {human_size(total)}</span></div>'
            f'<ul>{items}</ul></div>')


class Handler(BaseHTTPRequestHandler):
    server_version = "paste"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        # content-free access log
        print("%s %s" % (self.address_string(), fmt % args), flush=True)

    # ------------------------------------------------------------- helpers
    def client_ip(self):
        fwd = self.headers.get("X-Forwarded-For")
        if fwd:
            return fwd.split(",")[0].strip()
        return self.client_address[0]

    def send_body(self, code, body, ctype="text/html; charset=utf-8", extra=None):
        raw = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(raw)

    def json_body(self, code, obj):
        self.send_body(code, json.dumps(obj, ensure_ascii=False),
                       "application/json; charset=utf-8")

    def read_json(self, limit=MAX_BYTES + 65536):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return None, "пустой запрос"
        if length > limit:
            return None, "слишком большой запрос"
        try:
            return json.loads(self.rfile.read(length).decode("utf-8")), None
        except Exception:
            return None, "некорректный JSON"

    def parse_multipart(self, length):
        """Parse a multipart/form-data body into (fields, files), stdlib only."""
        ctype = self.headers.get("Content-Type", "")
        raw = self.rfile.read(length)
        head = ("Content-Type: %s\r\nMIME-Version: 1.0\r\n\r\n" % ctype).encode("latin-1", "replace")
        try:
            msg = BytesParser(policy=email.policy.default).parsebytes(head + raw)
        except Exception:
            return None, "некорректная форма"
        if not msg.is_multipart():
            return None, "некорректная форма"
        fields, files = {}, []
        for part in msg.iter_parts():
            name = part.get_param("name", header="content-disposition")
            if name is None:
                continue
            payload = part.get_payload(decode=True) or b""
            filename = part.get_filename()
            if filename is not None:
                files.append((filename, part.get_content_type(), payload))
            else:
                fields[name] = payload.decode("utf-8", "replace")
        return (fields, files), None

    # -------------------------------------------------------------- routing
    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        parts = urlsplit(self.path)
        path = parts.path
        query = parse_qs(parts.query)

        if path == "/healthz":
            return self.send_body(200, "ok", "text/plain; charset=utf-8")

        if path == "/favicon.ico":
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if path == "/":
            return self.send_body(200, page(FORM_HTML, error=""))

        if not rl_read.allow(self.client_ip()):
            return self.send_body(429, "too many requests", "text/plain; charset=utf-8")

        m = re.fullmatch(r"/([A-Za-z0-9]{%d})/f/(\d+)" % ID_LEN, path)
        if m:
            return self.handle_file(m.group(1), int(m.group(2)))

        m = re.fullmatch(r"/d/([A-Za-z0-9_-]{8,64})", path)
        if m:
            return self.handle_delete_token(m.group(1))

        m = re.fullmatch(r"/([A-Za-z0-9]{%d})(/raw)?" % ID_LEN, path)
        if m:
            pid, raw = m.group(1), bool(m.group(2))
            return self.handle_view(pid, raw, query)

        return self.send_body(404, page(NOT_FOUND_HTML))

    def do_POST(self):
        if urlsplit(self.path).path != "/api/share":
            return self.json_body(404, {"error": "not found"})
        if not rl_write.allow(self.client_ip()):
            return self.json_body(429, {"error": "слишком много запросов, подожди минуту"})

        ctype = (self.headers.get("Content-Type") or "").lower()
        files_in = []
        if ctype.startswith("multipart/form-data"):
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return self.json_body(400, {"error": "пустой запрос"})
            if length > MAX_UPLOAD_BYTES:
                return self.json_body(413, {
                    "error": f"запрос больше {MAX_UPLOAD_BYTES // (1024 * 1024)} МБ"})
            if not rl_upload_bytes.allow(self.client_ip(), length):
                return self.json_body(429, {
                    "error": f"за час с этого адреса загружено больше "
                             f"{MAX_UPLOAD_PER_HOUR // (1024 * 1024)} МБ"})
            parsed, err = self.parse_multipart(length)
            if err:
                return self.json_body(400, {"error": err})
            fields, files_in = parsed
        else:
            fields, err = self.read_json()
            if err:
                return self.json_body(400, {"error": err})

        text = fields.get("text")
        if not isinstance(text, str):
            text = ""
        if len(text.encode("utf-8")) > MAX_BYTES:
            return self.json_body(413, {"error": "текст больше 1 МБ"})

        if len(files_in) > MAX_FILES:
            return self.json_body(400, {"error": f"не больше {MAX_FILES} файлов"})
        clean_files = []
        total = 0
        for fname, ftype, payload in files_in:
            size = len(payload)
            if size > MAX_FILE_BYTES:
                return self.json_body(413, {
                    "error": f"файл «{clean_filename(fname)}» больше "
                             f"{MAX_FILE_BYTES // (1024 * 1024)} МБ"})
            total += size
            if total > MAX_FILES_TOTAL:
                return self.json_body(413, {
                    "error": f"все файлы вместе больше {MAX_FILES_TOTAL // (1024 * 1024)} МБ"})
            clean_files.append((clean_filename(fname), clean_ctype(ftype), size, payload))
        if not text.strip() and not clean_files:
            return self.json_body(400, {"error": "текст и файлы пусты"})

        ttl_key = str(fields.get("ttl") or "1d")
        ttl = ALLOWED_TTL.get(ttl_key)
        if ttl is None:
            try:
                ttl = int(ttl_key)
            except (TypeError, ValueError):
                ttl = DEFAULT_TTL
        ttl = max(60, min(ttl, MAX_TTL))

        burn = 1 if str(fields.get("burn")).lower() in ("1", "true", "on", "yes") else 0
        now = int(time.time())
        expires = now + ttl
        token = secrets.token_urlsafe(12)

        conn = db()
        try:
            conn.execute("BEGIN IMMEDIATE")
            pid = new_id(conn)
            if pid is None:
                conn.rollback()
                return self.json_body(507, {"error": "все короткие id заняты, попробуй позже"})
            conn.execute(
                "INSERT INTO pastes(id, body, created_at, expires_at, burn, delete_token)"
                " VALUES(?,?,?,?,?,?)",
                (pid, text, now, expires, burn, token),
            )
            if clean_files:
                stored = conn.execute(
                    "SELECT COALESCE(SUM(size), 0) FROM files").fetchone()[0]
                if stored + total > MAX_STORAGE_BYTES:
                    conn.rollback()
                    return self.json_body(507, {"error": "хранилище файлов заполнено"})
                conn.executemany(
                    "INSERT INTO files(paste_id, name, ctype, size, data)"
                    " VALUES(?,?,?,?,?)",
                    [(pid, n, c, s, sqlite3.Binary(d)) for n, c, s, d in clean_files],
                )
            conn.commit()
        except sqlite3.OperationalError:
            conn.rollback()
            return self.json_body(503, {"error": "база занята, попробуй ещё раз"})
        finally:
            conn.close()

        base = self.base_url()
        return self.json_body(200, {
            "id": pid,
            "url": f"{base}/{pid}",
            "expires_at": expires,
            "expires_human": human_at(expires),
            "delete_url": f"{base}/d/{token}",
            "files": len(clean_files),
        })

    def base_url(self):
        proto = self.headers.get("X-Forwarded-Proto") or "https"
        host = self.headers.get("X-Forwarded-Host") or self.headers.get("Host") or CTA
        return f"{proto}://{host}".rstrip("/")

    # -------------------------------------------------------------- handles
    def handle_view(self, pid, raw, query):
        # Read-and-consume for burn pastes, atomically.
        conn = db()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = int(time.time())
            row = conn.execute("SELECT * FROM pastes WHERE id = ?", (pid,)).fetchone()
            if row is None or row["expires_at"] <= now:
                if row is not None:
                    conn.execute("DELETE FROM pastes WHERE id = ?", (pid,))
                    conn.commit()
                conn.rollback()
                return self.send_body(404, page(NOT_FOUND_HTML))
            file_rows = conn.execute(
                "SELECT id, name, ctype, size FROM files WHERE paste_id = ? ORDER BY id",
                (pid,),
            ).fetchall()
            if row["burn"]:
                if raw or not file_rows:
                    conn.execute("DELETE FROM pastes WHERE id = ?", (pid,))
                    file_rows = []
                else:
                    # Text is consumed now; attachments stay downloadable for a
                    # short grace window, then the janitor removes the paste.
                    expires = min(row["expires_at"], now + BURN_GRACE)
                    conn.execute(
                        "UPDATE pastes SET body = '', burn = 0, expires_at = ? WHERE id = ?",
                        (expires, pid),
                    )
                    row = dict(row)
                    row["expires_at"] = expires
            else:
                conn.execute("UPDATE pastes SET views = views + 1 WHERE id = ?", (pid,))
            conn.commit()
        except sqlite3.OperationalError:
            conn.rollback()
            return self.send_body(503, "busy", "text/plain; charset=utf-8")
        finally:
            conn.close()

        body = row["body"]
        expires_human = human_at(row["expires_at"])
        parts = []
        if body:
            parts.append(human_size(len(body.encode("utf-8"))))
        if file_rows:
            parts.append(f"файлов {len(file_rows)}")
        size = " · ".join(parts)

        if raw:
            extra = {}
            if query.get("download"):
                extra["Content-Disposition"] = f'attachment; filename="{pid}.txt"'
            return self.send_body(200, body, "text/plain; charset=utf-8", extra)

        body_block = (f'<pre id="body">{html.escape(body)}</pre>' if body
                      else '<div class="sub">Текста нет — только файлы.</div>')
        return self.send_body(200, page(
            VIEW_HTML, id=html.escape(pid), body_block=body_block,
            files=attachment_list(pid, file_rows),
            copy_link='<a href="#" id="cp">копировать</a>' if body else "",
            expires_human=expires_human, size=size,
        ))

    def handle_file(self, pid, fid):
        conn = db()
        try:
            row = conn.execute(
                "SELECT f.id, f.name, f.ctype, f.data, p.expires_at"
                " FROM files f JOIN pastes p ON p.id = f.paste_id"
                " WHERE f.id = ? AND f.paste_id = ?",
                (fid, pid),
            ).fetchone()
            if row is None or row["expires_at"] <= int(time.time()):
                if row is not None:
                    conn.execute("DELETE FROM files WHERE id = ?", (fid,))
                    conn.commit()
                return self.send_body(404, page(NOT_FOUND_HTML))
        finally:
            conn.close()

        name = clean_filename(row["name"])
        ascii_name = re.sub(r"[^A-Za-z0-9._ -]", "_", name).strip() or "file"
        disposition = (f"attachment; filename=\"{ascii_name}\"; "
                       f"filename*=UTF-8''{quote(name, safe='')}")
        return self.send_body(200, row["data"], clean_ctype(row["ctype"]), {
            "Content-Disposition": disposition,
        })

    def handle_delete_token(self, token):
        conn = db()
        try:
            row = conn.execute("SELECT id FROM pastes WHERE delete_token = ?", (token,)).fetchone()
            if row is None:
                return self.send_body(404, page(NOT_FOUND_HTML))
            conn.execute("DELETE FROM pastes WHERE delete_token = ?", (token,))
            conn.commit()
        finally:
            conn.close()
        return self.send_body(200, "<!doctype html><meta charset=utf-8>"
                                   "<body style='background:#0f1115;color:#e6e6e6;"
                                   "font-family:sans-serif;padding:40px'>"
                                   "<h3>Удалено</h3><p><a style='color:#6ea8fe' href='/'>"
                                   "создать новую</a></p>")


def janitor():
    while True:
        time.sleep(300)
        try:
            purge_expired()
        except Exception as exc:  # keep the thread alive
            print("purge failed:", exc, flush=True)


def main():
    init_db()
    purge_expired()
    threading.Thread(target=janitor, daemon=True).start()
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    srv.daemon_threads = True
    print(f"listening on {HOST}:{PORT}, ids={len(ALPHABET) ** ID_LEN}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
