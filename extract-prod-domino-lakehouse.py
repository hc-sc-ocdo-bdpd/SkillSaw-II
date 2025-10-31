#!/usr/bin/env python3
# extract_prod_domino_lakehouse_sp.py
# ======================================================================
# Lotus/HCL Notes -> Microsoft Fabric Warehouse/Lakehouse (normalized EAV) + CAS attachments
# Auth: AAD Service Principal only (client credentials). No browser, no device code, no user SSO.
# Works with 32-bit Python (pywin32 + pyodbc x86 + ODBC SQL Server driver x86).
# ======================================================================

import os, re, sys, traceback, hashlib, tempfile, shutil, unicodedata, string, time, struct
from pathlib import Path
from contextlib import contextmanager
from typing import Any, List, Tuple, Optional, Dict, Callable
from datetime import datetime, timezone

import win32com.client
try:
    import pyodbc
except Exception as e:
    print("[FATAL] pyodbc is required:", e); raise
try:
    import msal
except Exception as e:
    print("[FATAL] msal is required:", e); raise
try:
    import pywintypes
except Exception:
    pywintypes = None

# ----------------------------- CONFIG ---------------------------------

# Domino / Notes
PREF_SERVER       = r"APP02/HC-SC/GC/CA"
PREF_SERVER_PATH  = r"csb\imsd\hcdir3.nsf"
LOCAL_REPLICA     = r"sap\sapaccess.nsf"
LOTUS_PASSWORD    = os.environ.get("LOTUS_PASSWORD", "Oban8ter18!")

# Fabric endpoint (Warehouse/Lakehouse SQL endpoint)
FABRIC_SERVER   = "tcp:cwip2qsn3yrufi3ixlvmvneje4-3zogig64wjtefgmbmsuh7lldwm.datawarehouse.fabric.microsoft.com,1433"
FABRIC_DATABASE = os.environ.get("FABRIC_DATABASE", "notes_extract_prod2")

# AAD Service Principal (client credentials)
TENANT_ID     = os.environ.get("AZ_TENANT_ID", "42fd9015-de4d-4223-a368-baeacab48927")
CLIENT_ID     = os.environ.get("AZ_CLIENT_ID", "2bc1c9b9-d0ad-4ff1-ac90-f5f54f942efb")
CLIENT_SECRET = os.environ.get("AZ_CLIENT_SECRET", "o5B8Q~XnkYM_BFpZ3anY~5lzrSiVqqGW3P_60br1")
AUTHORITY     = f"https://login.microsoftonline.com/{TENANT_ID}"
SQL_SCOPES    = ["https://database.windows.net//.default"]  # NOTE: double slash is correct

# CAS root (content-addressed storage for extracted attachments)
CAS_ROOT = Path(os.environ.get("NOTES_CAS_ROOT") or os.environ.get("LOCALAPPDATA") or Path.home()) / "notes_cas"

# Canonical Notes views + synonyms
CANONICAL_TARGETS = [
    "Person By Surname",
    "Person By Organization",
    "Organizational Structure",
    "All Employees HC Export",
    "All Employees PHAC Export",
    "GEDS Update M365",
]

VIEW_SYNONYMS: Dict[str, List[str]] = {
    "Person By Surname": [
        r"\b(persons?|people|employees?)\b.*\b(surname|last\s*name|alphabetic(?:ally)?)\b",
        r"\b(employ[ée]s?)\b.*\b(alphab[ée]tiqu(?:e|ement))\b",
        r"^\s*\d+\.\s*employees?,?\s*alphabetically\s*$",
        r"^\s*employ[ée]s?,?\s*tri[ée]s?\s*alphab[ée]tiquement\s*$",
        r"\bemployees?,?\s*alphabetically\b",
        r"\bemploy[ée]s?\s*tri[ée]s?\s*alphab[éè]tiquement\b",
    ],
    "Person By Organization": [
        r"\b(persons?|people|employees?)\b.*\b(by|par)\b.*\b(org(?:ani[sz]ation)?|branch|directorate|direction|r[ée]gion)\b",
        r"\bby\s+org\s+structure\b",
        r"^\s*\d+\.\s*employees?\s+by\s+region,\s*by\s*branch\s*$",
        r"^\s*employ[ée]s?\s+par\s+r[ée]gion,\s*par\s*direction\s*g[éè]n[ée]rale\s*$",
        r"\benglish.*by\s+org\s+structure\b",
        r"\bfrench.*par\s+structure\s+org\b",
        r"\b(hpcb|isc).*(by|par).*(org\s*structure|structure\s+org)",
    ],
    "Organizational Structure": [
        r"\borganizational\s+structure\b",
        r"\borganization\s+al\s+structure\b",
        r"\borganization\s+structure\s+unsorted\b",
    ],
    "All Employees HC Export": [
        r"\ball\s+employees?\b.*\bHC\b.*\bexport\b",
        r"^\s*all\s+hc\s+employees?\s+export\s*$",
    ],
    "All Employees PHAC Export": [
        r"\ball\s+employees?\b.*\bPHAC\b.*\bexport\b",
        r"^\s*all\s+phac\s+employees?\s+export\s*$",
    ],
    "GEDS Update M365": [
        r"\bgeds\b.*update.*m365\b",
        r"^\s*geds\\?update\s+m365\s*$",
        r"\bm365\s+geds\s+update\b",
    ],
}

# Behavior
EXCLUDE_PREFIXES = ("..admin", "*help", "*aide", "(lookup")
DEBUG = True
CATEGORY_COLUMN_INDEX = 0
CREATE_SCHEMA_IF_MISS = True

# Notes Embedded Object types
EO_TYPE_IMAGE       = 1452
EO_TYPE_OLE         = 1453
EO_TYPE_ATTACHMENT  = 1454

FORM_MAX    = 256
SUBJECT_MAX = 1024
AUTHOR_MAX  = 512

# --------------------------- UTILITIES --------------------------------

def log(*args):
    if DEBUG: print(*args)

def sanitize_folder_name(name: str, max_len=100) -> str:
    if not name or not name.strip(): return "Unnamed"
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'[\s_]+', '_', name)
    return name[:max_len].strip('_')

def safe_str(val: Any, max_len: int, field: str) -> Optional[str]:
    if val is None: return None
    s = str(val)
    if len(s) > max_len:
        if DEBUG: log(f"[WARN] Truncated {field} from {len(s)} to {max_len} chars")
        return s[:max_len]
    return s

def as_dt(val: Any) -> Optional[datetime]:
    dt: Optional[datetime]
    if isinstance(val, datetime):
        dt = val
    else:
        try:
            dt = datetime.fromisoformat(str(val))
        except Exception:
            return None
    if dt is None: return None
    if dt.tzinfo is not None:
        try:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            dt = dt.replace(tzinfo=None)
    return dt

def sha256_bytes(b: bytes) -> bytes:
    h = hashlib.sha256(); h.update(b); return h.digest()

def sha256_file(p: Path) -> bytes:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.digest()

def cas_store(src: Path) -> Tuple[bytes, str, int]:
    digest = sha256_file(src)
    hexs = digest.hex()
    rel  = Path(hexs[0:2]) / hexs[2:4] / (hexs + ".bin")
    dest = CAS_ROOT / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        tmp = dest.with_suffix(".tmp")
        shutil.copy2(src, tmp)
        tmp.replace(dest)
    return digest, str(rel).replace("\\", "/"), src.stat().st_size

_PUNCT_TABLE = str.maketrans({c: " " for c in string.punctuation})
_WS = re.compile(r"\s+")
_ALT_SPLIT = re.compile(r"\(([^()]*\|[^()]*)\)")
_CHARCLASS = re.compile(r"\[([^\]]+)\]")
_QUANT     = re.compile(r"\{[^}]*\}")

def _normalize(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", s).lower()
    s = s.translate(_PUNCT_TABLE)
    s = " ".join(s.split())
    return s

def _leaf(name: str) -> str:
    parts = re.split(r"[\\/]+", (name or "").strip())
    return parts[-1].strip() if parts else ""

def _choose_alt(s: str) -> str:
    def _rep(m: re.Match) -> str:
        return m.group(1).split("|", 1)[0]
    return _ALT_SPLIT.sub(_rep, s)

def _simplify_charclass(s: str) -> str:
    def _rep(m: re.Match) -> str:
        inside = m.group(1)
        for ch in inside:
            if ch.isalpha(): return ch
        return inside[0] if inside else ""
    return _CHARCLASS.sub(_rep, s)

def regex_to_needles(pat: str) -> List[str]:
    s = pat
    for repl in (r"\b", r"\s*", r"\s+", r"\s", r"\t", r"\n", r"\r"):
        s = s.replace(repl, " ")
    s = s.replace("^", " ").replace("$", " ")
    s = s.replace(r"\.", ".").replace(r"\/", "/").replace(r"\\", "\\")
    s = s.replace(r"\(", "(").replace(r"\)", ")")
    s = s.replace(r"\?", "?").replace(r"\+", "+").replace(r"\*", "*")
    s = s.replace(".*", " ").replace(".+", " ")
    s = _QUANT.sub(" ", s)
    s = s.replace("(?:", "(")
    s = _simplify_charclass(s)
    s = _choose_alt(s)
    s = re.sub(r"[()^$?+*|]", " ", s)
    s = re.sub(r"\\", " ", s)
    s = _WS.sub(" ", s).strip().lower()

    needles = []
    if s and any(c.isalnum() for c in s):
        needles.append(s)
    s2 = re.sub(r"[^0-9a-zà-ÿ/\\\- ]+", "", s)
    s2 = _WS.sub(" ", s2).strip()
    if s2 and s2 != s:
        needles.append(s2)

    seen = set(); out = []
    for n in needles:
        if n not in seen:
            seen.add(n); out.append(n)
    return out

def build_contains_map(view_synonyms: Dict[str, List[str]]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for canon, patterns in view_synonyms.items():
        needles: List[str] = [canon.lower()]
        for pat in patterns:
            needles.extend(regex_to_needles(pat))
        cleaned: List[str] = []
        seen = set()
        for n in needles:
            n = _WS.sub(" ", n).strip()
            if not n or n in seen: continue
            if len(n) < 3: continue
            cleaned.append(n); seen.add(n)
        out[canon] = cleaned
        if DEBUG:
            log(f"[DEBUG] Needles for '{canon}': {cleaned}")
    return out

CONTAINS_MAP = build_contains_map(VIEW_SYNONYMS)

def _escape_regex_literal_for_mysql(s: str) -> str:
    esc_sql = s.replace("\\", "\\\\").replace("'", "''")
    esc_regex = re.sub(r'([.^$*+?{}\[\]\\|()])', r'\\\1', esc_sql)
    return f"(?i)^{esc_regex}$"

def _fmt_sql_update_regex(plan_id: int, canon_name: str, notes_view_name: str) -> str:
    patt = _escape_regex_literal_for_mysql(notes_view_name)
    canon_sql = canon_name.replace("'", "''")
    return (
        "UPDATE ingestion_plan_views "
        f"SET regex_override='{patt}' "
        f"WHERE plan_id={plan_id} AND canon_name='{canon_sql}';"
    )

# ------------------------- RESILIENCE HELPERS --------------------------

RETRY_COM_TRIES   = 6
RETRY_COM_BACKOFF = 1.5
_TRANSIENT_COM_SUBSTRINGS = [
    "Network", "The server is not responding", "Timed out",
    "Argument has been deleted", "Object variable not set",
    "NotesViewEntryCollection", "unable to find path to server",
    "no network connection", "port error",
]

def _is_transient_com_error(exc: Exception) -> bool:
    msg = f"{exc}"
    if pywintypes and isinstance(exc, pywintypes.com_error):
        try:
            details = exc.args[2]
            if isinstance(details, tuple) and len(details) >= 3 and details[2]:
                msg = f"{msg} :: {details[2]}"
        except Exception:
            pass
    mlow = (msg or "").lower()
    return any(s.lower() in mlow for s in _TRANSIENT_COM_SUBSTRINGS)

def retry_call(fn: Callable, *args, tries: int, backoff_sec: float,
               is_retryable: Callable[[Exception], bool], **kwargs):
    attempt = 0
    last_exc = None
    delay = backoff_sec
    while attempt < tries:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if not is_retryable(e):
                raise
            attempt += 1
            if attempt >= tries:
                break
            time.sleep(delay)
            delay *= 2
    assert last_exc is not None
    raise last_exc

def resilient_com(fn: Callable, *args, **kwargs):
    return retry_call(
        fn, *args, **kwargs,
        tries=RETRY_COM_TRIES,
        backoff_sec=RETRY_COM_BACKOFF,
        is_retryable=_is_transient_com_error,
    )

class NotesReopenContext:
    def __init__(self, open_db_fn: Callable[[], Any], get_view_fn: Callable[[Any, str], Any], view_name: Optional[str] = None):
        self.open_db_fn = open_db_fn
        self.get_view_fn = get_view_fn
        self.notes_db = None
        self.view_name = view_name
    def reopen_db(self):
        self.notes_db = self.open_db_fn(); return self.notes_db
    def reopen_view(self, view_name: Optional[str] = None):
        if self.notes_db is None: self.reopen_db()
        vname = view_name or self.view_name
        if not vname: raise RuntimeError("No view_name provided for reopen_view")
        return self.get_view_fn(self.notes_db, vname)

def resilient_com_with_reopen(fn, reopen_ctx: NotesReopenContext, *args, **kwargs):
    attempt = 0
    delay = RETRY_COM_BACKOFF
    last_exc = None
    while attempt < RETRY_COM_TRIES:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if not _is_transient_com_error(e): raise
            attempt += 1; time.sleep(delay); delay *= 2
            try: reopen_ctx.reopen_db()
            except Exception: pass
    raise last_exc

# ----------------------------- DB LAYER (Fabric via SP) -------------------------------

_SQL_COPT_SS_ACCESS_TOKEN = 1256

def _aad_sql_access_token_bytes(token: str) -> bytes:
    b = token.encode("utf-16-le")
    return struct.pack("<i", len(b)) + b

def _acquire_client_token() -> str:
    if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET):
        raise RuntimeError("AZ_TENANT_ID / AZ_CLIENT_ID / AZ_CLIENT_SECRET must be set.")
    app = msal.ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
    res = app.acquire_token_for_client(scopes=SQL_SCOPES)
    if "access_token" not in res:
        raise RuntimeError(f"Failed to acquire SQL token: {res}")
    return res["access_token"]

def _choose_sql_driver() -> str:
    drivers = [d.strip() for d in pyodbc.drivers()]
    for name in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
        if name in drivers:
            return name
    raise RuntimeError("[ODBC] No suitable x86 driver found. Installed: " + ", ".join(drivers or ["<none>"]))

def _open_fabric_connection():
    driver = _choose_sql_driver()

    # Try Access Token injection (preferred)
    try:
        token = _acquire_client_token()
        attrs = { _SQL_COPT_SS_ACCESS_TOKEN: _aad_sql_access_token_bytes(token) }
        return pyodbc.connect(
            f"Driver={{{driver}}};"
            f"Server={FABRIC_SERVER};"
            f"Database={FABRIC_DATABASE};"
            f"Encrypt=yes;TrustServerCertificate=no;",
            attrs_before=attrs,
            autocommit=False,
            timeout=30
        )
    except pyodbc.Error as e:
        print("[SQL] AccessToken connect failed; falling back to ActiveDirectoryServicePrincipal:", e)

    # Fallback: built-in SP auth (no token injection)
    return pyodbc.connect(
        f"Driver={{{driver}}};"
        f"Server={FABRIC_SERVER};"
        f"Database={FABRIC_DATABASE};"
        f"Authentication=ActiveDirectoryServicePrincipal;"
        f"UID={CLIENT_ID};PWD={CLIENT_SECRET};"
        f"Encrypt=yes;TrustServerCertificate=no;",
        autocommit=False,
        timeout=30
    )

@contextmanager
def sql_db():
    con = _open_fabric_connection()
    try:
        yield con
        con.commit()
    except:
        con.rollback(); raise
    finally:
        con.close()

# ------------------------------ SCHEMA (T-SQL) --------------------------------

SCHEMA_SQL = [
"""
IF OBJECT_ID('dbo.sources','U') IS NULL
CREATE TABLE dbo.sources(
  id           BIGINT IDENTITY(1,1) PRIMARY KEY,
  server_name  NVARCHAR(255) NOT NULL,
  filepath     NVARCHAR(512) NOT NULL,
  replica_id   NVARCHAR(32)  NULL,
  title        NVARCHAR(255) NULL,
  last_seen_at DATETIME2     NULL,
  CONSTRAINT uk_source UNIQUE(server_name, filepath)
);
""",
f"""
IF OBJECT_ID('dbo.documents','U') IS NULL
CREATE TABLE dbo.documents(
  unid            CHAR(32)   NOT NULL PRIMARY KEY,
  source_id       BIGINT     NOT NULL,
  note_id         NVARCHAR(16) NULL,
  form            NVARCHAR({FORM_MAX}) NULL,
  subject         NVARCHAR({SUBJECT_MAX}) NULL,
  author          NVARCHAR({AUTHOR_MAX}) NULL,
  created_at      DATETIME2  NULL,
  modified_at     DATETIME2  NULL,
  has_attachments BIT        NOT NULL DEFAULT 0,
  text_hash       VARBINARY(32) NULL,
  text_body       NVARCHAR(MAX) NULL,
  doc_size_bytes  BIGINT NULL,
  INDEX IX_documents_source (source_id),
  INDEX IX_documents_modified (modified_at),
  INDEX IX_documents_form (form)
);
""",
"""
IF OBJECT_ID('dbo.items','U') IS NULL
CREATE TABLE dbo.items(
  id          BIGINT IDENTITY(1,1) PRIMARY KEY,
  name        NVARCHAR(128) NOT NULL,
  name_lc     AS LOWER(name) PERSISTED,
  notes_filter INT NULL,
  CONSTRAINT uk_item_name UNIQUE(name_lc)
);
""",
"""
IF OBJECT_ID('dbo.item_values','U') IS NULL
CREATE TABLE dbo.item_values(
  id            BIGINT IDENTITY(1,1) PRIMARY KEY,
  item_id       BIGINT NOT NULL,
  val_kind      NVARCHAR(16) NOT NULL DEFAULT 'unknown',
  val_hash      VARBINARY(32) NULL,
  v_string      NVARCHAR(1024) NULL,
  v_text        NVARCHAR(MAX) NULL,
  v_number      FLOAT NULL,
  v_datetime    DATETIME2 NULL,
  v_bool        BIT NULL,
  v_bytes       VARBINARY(MAX) NULL,
  attachment_id BIGINT NULL,
  INDEX IX_item_values_item_kind (item_id, val_kind),
  INDEX IX_item_values_num (v_number),
  INDEX IX_item_values_dt (v_datetime),
  INDEX IX_item_values_bool (v_bool),
  INDEX IX_item_values_string (v_string)
);
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE name='UK_itemvalue_dedup' AND object_id=OBJECT_ID('dbo.item_values'))
BEGIN
  CREATE UNIQUE INDEX UK_itemvalue_dedup ON dbo.item_values(item_id, val_kind, val_hash);
END
""",
"""
IF OBJECT_ID('dbo.doc_item_values','U') IS NULL
CREATE TABLE dbo.doc_item_values(
  unid           CHAR(32) NOT NULL,
  item_id        BIGINT   NOT NULL,
  val_order      INT      NOT NULL DEFAULT 0,
  item_value_id  BIGINT   NOT NULL,
  is_summary     BIT      NOT NULL DEFAULT 0,
  CONSTRAINT PK_doc_item_values PRIMARY KEY (unid, item_id, val_order),
  INDEX IX_div_item_value (item_value_id)
);
""",
"""
IF OBJECT_ID('dbo.attachments','U') IS NULL
CREATE TABLE dbo.attachments(
  id           BIGINT IDENTITY(1,1) PRIMARY KEY,
  unid         CHAR(32) NOT NULL,
  item_name    NVARCHAR(128) NULL,
  kind         NVARCHAR(16) NOT NULL,
  filename     NVARCHAR(512) NULL,
  mime_type    NVARCHAR(255) NULL,
  size_bytes   BIGINT NULL,
  sha256       VARBINARY(32) NOT NULL,
  storage_path NVARCHAR(1024) NOT NULL,
  created_at   DATETIME2 NULL,
  CONSTRAINT uk_file UNIQUE (sha256, unid, filename),
  INDEX IX_att_unid (unid),
  INDEX IX_att_kind (kind)
);
""",
"""
IF OBJECT_ID('dbo.document_views','U') IS NULL
CREATE TABLE dbo.document_views(
  id            BIGINT IDENTITY(1,1) PRIMARY KEY,
  unid          CHAR(32) NOT NULL,
  view_name     NVARCHAR(255) NOT NULL,
  category_path NVARCHAR(1024) NULL,
  leaf_category NVARCHAR(255)  NULL,
  INDEX IX_docview_view (view_name),
  INDEX IX_docview_unid (unid),
  CONSTRAINT uk_doc_view_nodup UNIQUE (unid, view_name, category_path)
);
""",
"""
IF OBJECT_ID('dbo.etl_runs','U') IS NULL
CREATE TABLE dbo.etl_runs(
  id            BIGINT IDENTITY(1,1) PRIMARY KEY,
  source_id     BIGINT NOT NULL,
  started_at    DATETIME2 NOT NULL,
  ended_at      DATETIME2 NULL,
  docs_scanned  INT DEFAULT 0,
  docs_upserted INT DEFAULT 0,
  atts_saved    INT DEFAULT 0,
  errors        INT DEFAULT 0,
  notes         NVARCHAR(1024) NULL,
  INDEX IX_etl_runs (source_id, started_at)
);
""",
"""
IF OBJECT_ID('dbo.ingestion_plans','U') IS NULL
CREATE TABLE dbo.ingestion_plans(
  id           BIGINT IDENTITY(1,1) PRIMARY KEY,
  server_name  NVARCHAR(255) NOT NULL,
  filepath     NVARCHAR(512) NOT NULL,
  enabled      BIT NOT NULL DEFAULT 1,
  notes        NVARCHAR(512) NULL,
  CONSTRAINT uk_plan UNIQUE(server_name, filepath)
);
""",
"""
IF OBJECT_ID('dbo.ingestion_plan_views','U') IS NULL
CREATE TABLE dbo.ingestion_plan_views(
  id              BIGINT IDENTITY(1,1) PRIMARY KEY,
  plan_id         BIGINT NOT NULL,
  canon_name      NVARCHAR(255) NOT NULL,
  enabled         BIT NOT NULL DEFAULT 1,
  regex_override  NVARCHAR(512) NULL,
  priority        INT NOT NULL DEFAULT 100,
  CONSTRAINT uk_plan_view UNIQUE (plan_id, canon_name)
);
""",
"""
IF OBJECT_ID('dbo.etl_checkpoints','U') IS NULL
CREATE TABLE dbo.etl_checkpoints(
  id           BIGINT IDENTITY(1,1) PRIMARY KEY,
  plan_id      BIGINT NOT NULL,
  source_id    BIGINT NOT NULL,
  view_name    NVARCHAR(255) NOT NULL,
  snapshot_sig CHAR(64) NOT NULL,
  next_index   INT NOT NULL DEFAULT 0,
  last_unid    CHAR(32) NULL,
  updated_at   DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT uk_checkpoint UNIQUE (plan_id, source_id, view_name)
);
"""
]

def ensure_schema():
    if not CREATE_SCHEMA_IF_MISS: return
    with sql_db() as con:
        cur = con.cursor()
        for sql in SCHEMA_SQL:
            cur.execute(sql)
        con.commit()

# ------------------------------ DML helpers ------------------------------

def get_or_create_source(cur, server_name: str, filepath: str,
                         title: Optional[str], replica_id: Optional[str]) -> int:
    cur.execute("""
    MERGE dbo.sources AS tgt
    USING (SELECT ? AS server_name, ? AS filepath) AS src
      ON tgt.server_name = src.server_name AND tgt.filepath = src.filepath
    WHEN MATCHED THEN UPDATE SET title = ?, replica_id = ?, last_seen_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN INSERT (server_name, filepath, title, replica_id, last_seen_at)
         VALUES (src.server_name, src.filepath, ?, ?, SYSUTCDATETIME())
    OUTPUT inserted.id;
    """, (server_name, filepath, title, replica_id, title, replica_id))
    return int(cur.fetchone()[0])

def start_etl_run(cur, source_id: int) -> int:
    cur.execute("INSERT INTO dbo.etl_runs (source_id, started_at) VALUES (?, SYSUTCDATETIME()); SELECT SCOPE_IDENTITY();",
                (source_id,))
    return int(cur.fetchone()[0])

def finish_etl_run(cur, run_id: int, stats: Dict[str,int]):
    cur.execute("""
      UPDATE dbo.etl_runs
         SET ended_at = SYSUTCDATETIME(),
             docs_scanned  = ?,
             docs_upserted = ?,
             atts_saved    = ?,
             errors        = ?
       WHERE id = ?;
    """, (stats.get("scanned",0), stats.get("upserted",0), stats.get("atts",0), stats.get("errors",0), run_id))

def get_item_id(cur, name: str) -> int:
    cur.execute("""
    MERGE dbo.items AS tgt
    USING (SELECT ? AS name) AS src
      ON tgt.name_lc = LOWER(src.name)
    WHEN NOT MATCHED THEN INSERT (name) VALUES (src.name)
    OUTPUT inserted.id;
    """, (name,))
    return int(cur.fetchone()[0])

def should_store_item(cur, name: str) -> bool:
    cur.execute("SELECT notes_filter FROM dbo.items WHERE name_lc = LOWER(?)", (name,))
    row = cur.fetchone()
    if row is None:
        return True
    try:
        return int(row[0]) == 1
    except Exception:
        return False

def upsert_document(cur, source_id: int, doc_row: Dict[str,Any]):
    cur.execute("""
    MERGE dbo.documents AS tgt
    USING (SELECT ? AS unid) AS src
      ON tgt.unid = src.unid
    WHEN MATCHED THEN UPDATE SET
         source_id=?, note_id=?, form=?, subject=?, author=?,
         created_at=?, modified_at=?, has_attachments=?,
         text_hash=?, text_body=?, doc_size_bytes=?
    WHEN NOT MATCHED THEN INSERT
      (unid, source_id, note_id, form, subject, author, created_at, modified_at,
       has_attachments, text_hash, text_body, doc_size_bytes)
      VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        doc_row["unid"],
        source_id, doc_row.get("note_id"), doc_row.get("form"), doc_row.get("subject"), doc_row.get("author"),
        doc_row.get("created_at"), doc_row.get("modified_at"), doc_row.get("has_attachments"),
        doc_row.get("text_hash"), doc_row.get("text_body"), doc_row.get("doc_size_bytes"),
        doc_row["unid"], source_id, doc_row.get("note_id"), doc_row.get("form"), doc_row.get("subject"),
        doc_row.get("author"), doc_row.get("created_at"), doc_row.get("modified_at"),
        doc_row.get("has_attachments"), doc_row.get("text_hash"), doc_row.get("text_body"),
        doc_row.get("doc_size_bytes")
    ))

def _null_eq(col: str) -> str:
    return f"(({col} = ?) OR ({col} IS NULL AND ? IS NULL))"

def _select_existing_item_value(cur, item_id: int, kind: str,
                                s: Optional[str], t: Optional[str],
                                n: Optional[float], dt: Optional[datetime],
                                b: Optional[int], att_id: Optional[int]) -> Optional[int]:
    sql = f"""
    SELECT TOP 1 id FROM dbo.item_values
     WHERE item_id = ? AND val_kind = ?
       AND {_null_eq('v_string')}
       AND {_null_eq('v_text')}
       AND {_null_eq('v_number')}
       AND {_null_eq('v_datetime')}
       AND {_null_eq('v_bool')}
       AND v_bytes IS NULL
       AND {_null_eq('attachment_id')}
    """
    params = (item_id, kind, s,s, t,t, n,n, dt,dt, b,b, att_id,att_id)
    cur.execute(sql, params)
    row = cur.fetchone()
    return int(row[0]) if row else None

def _compute_val_hash(item_id: int, kind: str,
                      s: Optional[str], t: Optional[str],
                      n: Optional[float], dt: Optional[datetime],
                      b: Optional[int], att_id: Optional[int]) -> bytes:
    def _none(x): return "" if x is None else str(x)
    payload = "\x1f".join([
        str(item_id), kind, _none(s), _none(t), _none(n),
        dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "",
        _none(b), _none(att_id)
    ])
    return hashlib.sha256(payload.encode("utf-8")).digest()

def get_or_create_item_value(cur, item_id: int, kind: str,
                             s: Optional[str]=None, t: Optional[str]=None,
                             n: Optional[float]=None, dt: Optional[datetime]=None,
                             b: Optional[int]=None, att_id: Optional[int]=None) -> int:
    existing = _select_existing_item_value(cur, item_id, kind, s, t, n, dt, b, att_id)
    if existing:
        return existing
    val_hash = _compute_val_hash(item_id, kind, s, t, n, dt, b, att_id)
    cur.execute("""
      INSERT INTO dbo.item_values
        (item_id, val_kind, val_hash, v_string, v_text, v_number, v_datetime, v_bool, v_bytes, attachment_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?);
      SELECT SCOPE_IDENTITY();
    """, (item_id, kind, val_hash, s, t, n, dt, b, att_id))
    return int(cur.fetchone()[0])

def link_doc_item_value(cur, unid: str, item_id: int, order_idx: int,
                        item_value_id: int, is_summary: int = 0):
    cur.execute("""
    MERGE dbo.doc_item_values AS tgt
    USING (SELECT ? AS unid, ? AS item_id, ? AS val_order) AS src
      ON tgt.unid = src.unid AND tgt.item_id = src.item_id AND tgt.val_order = src.val_order
    WHEN MATCHED THEN UPDATE SET item_value_id = ?, is_summary = ?
    WHEN NOT MATCHED THEN INSERT (unid, item_id, val_order, item_value_id, is_summary)
         VALUES (src.unid, src.item_id, src.val_order, ?, ?);
    """, (unid, item_id, order_idx, item_value_id, int(bool(is_summary)), item_value_id, int(bool(is_summary))))

def insert_item_value(cur, unid: str, item_id: int, order_idx: int, kind: str,
                      s: Optional[str]=None, t: Optional[str]=None,
                      n: Optional[float]=None, dt: Optional[datetime]=None,
                      b: Optional[int]=None, att_id: Optional[int]=None,
                      is_summary: int = 0):
    iv_id = get_or_create_item_value(cur, item_id, kind, s, t, n, dt, b, att_id)
    link_doc_item_value(cur, unid, item_id, order_idx, iv_id, is_summary=is_summary)

def insert_attachment(cur, row: Dict[str,Any]) -> Optional[int]:
    cur.execute("""
    MERGE dbo.attachments AS tgt
    USING (SELECT ? AS sha256, ? AS unid, ? AS filename) AS src
      ON tgt.sha256 = src.sha256 AND tgt.unid = src.unid AND (tgt.filename = src.filename OR (tgt.filename IS NULL AND src.filename IS NULL))
    WHEN MATCHED THEN UPDATE SET item_name=?, kind=?, mime_type=?, size_bytes=?, storage_path=?, created_at = ISNULL(tgt.created_at, SYSUTCDATETIME())
    WHEN NOT MATCHED THEN INSERT (unid, item_name, kind, filename, mime_type, size_bytes, sha256, storage_path, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
    OUTPUT inserted.id;
    """, (
        row["sha256"], row["unid"], row.get("filename"),
        row.get("item_name"), row.get("kind"), row.get("mime_type"), row.get("size_bytes"), row.get("storage_path"),
        row["unid"], row.get("item_name"), row.get("kind"), row.get("filename"), row.get("mime_type"),
        row.get("size_bytes"), row["sha256"], row.get("storage_path")
    ))
    got = cur.fetchone()
    return int(got[0]) if got else None

def _canon_category_path(category_path: Optional[str]) -> Optional[str]:
    if not category_path: return None
    parts = [p.strip() for p in category_path.split("\\") if p and p.strip()]
    if not parts: return None
    return "\\".join(parts)

def insert_document_view(cur, unid: str, view_name: str, category_path: Optional[str]):
    cat  = _canon_category_path(category_path)
    leaf = cat.split("\\")[-1] if cat else None
    cur.execute("""
    MERGE dbo.document_views AS tgt
    USING (SELECT ? AS unid, ? AS view_name, ? AS category_path) AS src
      ON tgt.unid = src.unid AND tgt.view_name = src.view_name AND (tgt.category_path = src.category_path OR (tgt.category_path IS NULL AND src.category_path IS NULL))
    WHEN MATCHED THEN UPDATE SET leaf_category = ?
    WHEN NOT MATCHED THEN INSERT (unid, view_name, category_path, leaf_category)
         VALUES (src.unid, src.view_name, src.category_path, ?);
    """, (unid, view_name, cat, leaf, leaf))

# -------------------- CHECKPOINT HELPERS ------------------------

def _sig_for_snapshot(rows: List[Tuple[str, Optional[str]]]) -> str:
    h = hashlib.sha256()
    for u, _ in rows:
        h.update((u or '').encode('utf-8')); h.update(b'\x00')
    return h.hexdigest()

def load_checkpoint(cur, plan_id: int, source_id: int, view_name: str) -> Optional[Dict[str,Any]]:
    cur.execute("""
      SELECT id, plan_id, source_id, view_name, snapshot_sig, next_index, last_unid
      FROM dbo.etl_checkpoints WHERE plan_id=? AND source_id=? AND view_name=?;
    """, (plan_id, source_id, view_name))
    r = cur.fetchone()
    if not r: return None
    return dict(id=r[0], plan_id=r[1], source_id=r[2], view_name=r[3],
                snapshot_sig=r[4], next_index=r[5], last_unid=r[6])

def upsert_checkpoint(cur, plan_id: int, source_id: int, view_name: str,
                      snapshot_sig: str, next_index: int, last_unid: Optional[str]):
    cur.execute("""
    MERGE dbo.etl_checkpoints AS tgt
    USING (SELECT ? AS plan_id, ? AS source_id, ? AS view_name) AS src
      ON tgt.plan_id = src.plan_id AND tgt.source_id = src.source_id AND tgt.view_name = src.view_name
    WHEN MATCHED THEN UPDATE SET snapshot_sig = ?, next_index = ?, last_unid = ?, updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN INSERT (plan_id, source_id, view_name, snapshot_sig, next_index, last_unid, updated_at)
         VALUES (src.plan_id, src.source_id, src.view_name, ?, ?, ?, SYSUTCDATETIME());
    """, (plan_id, source_id, view_name, snapshot_sig, next_index, last_unid, snapshot_sig, next_index, last_unid))

# ------------------------- PLAN-DRIVEN LAYER ---------------------------

def open_database(server_name: str, filepath: str):
    filepath = filepath.replace('/', '\\')
    session = win32com.client.Dispatch("Lotus.NotesSession")
    session.Initialize(LOTUS_PASSWORD)

    def _get_db():
        db = session.GetDatabase(server_name, filepath)
        if not db.IsOpen:
            try: db.Open(server_name, filepath)
            except Exception: pass
        return db

    notes_db = resilient_com(_get_db)
    if notes_db.IsOpen:
        log(f"[INFO] Opened server DB: {server_name}:{filepath}")
        return session, server_name, filepath, notes_db

    try:
        uiw = win32com.client.Dispatch("Notes.NotesUIWorkspace")
        uidb = uiw.CurrentDatabase
        ui_server = getattr(uidb, "Server", "")
        ui_file   = getattr(uidb, "FilePath", "")
        if ui_file:
            def _get_db_ui():
                db2 = session.GetDatabase(ui_server, ui_file)
                if not db2.IsOpen:
                    try: db2.Open(ui_server, ui_file)
                    except Exception: pass
                return db2
            db2 = resilient_com(_get_db_ui)
            if db2.IsOpen:
                log(f"[INFO] Auto-detected UI DB: {ui_server}:{ui_file}")
                return session, ui_server, ui_file, db2
    except Exception:
        pass

    local_server = ""
    def _get_db_local():
        db3 = session.GetDatabase(local_server, filepath)
        if not db3.IsOpen:
            try: db3.Open(local_server, filepath)
            except Exception: pass
        return db3

    db3 = resilient_com(_get_db_local)
    if not db3.IsOpen:
        raise RuntimeError(f"Failed to open DB {server_name}:{filepath}. Check paths and credentials.")
    log(f"[INFO] Opened LOCAL DB: {local_server}:{filepath}")
    return session, local_server, filepath, db3

def select_views_for_plan(notes_db, canon_targets: List[str], overrides_by_canon: Dict[str, Optional[str]],
                          plan_id: Optional[int] = None, max_suggestions: int = 20) -> List[Any]:
    all_views = list(notes_db.Views)
    print("[INFO] All available views in the database:")
    for v in all_views:
        print(f"  - {v.Name}")

    decorated: List[Tuple[Any, str, str, str, str]] = []
    for v in all_views:
        full_raw = v.Name or ""
        leaf_raw = _leaf(full_raw)
        full_norm = _normalize(full_raw)
        leaf_norm = _normalize(leaf_raw)
        decorated.append((v, full_norm, leaf_norm, full_raw, leaf_raw))

    def is_excluded(vname: str) -> bool:
        low = (vname or "").lower().strip()
        return low.startswith(EXCLUDE_PREFIXES)

    def prefer(curr: Optional[str], cand: str) -> bool:
        if curr is None: return True
        return ("english / anglais" not in (curr or "").lower()) and ("english / anglais" in cand.lower())

    chosen: Dict[str, Any] = {}

    for canon in canon_targets:
        override_raw = overrides_by_canon.get(canon)
        if override_raw:
            needles = [_normalize(override_raw), (override_raw or "").lower()]
        else:
            needles = CONTAINS_MAP.get(canon, [canon.lower()])

        nn: List[str] = []
        for n in needles:
            if not isinstance(n, str): continue
            n = " ".join(n.split()).strip()
            if n: nn.append(n)
        needles = nn

        if DEBUG: log(f"[DEBUG] Matching canon='{canon}' with needles={needles}")

        for v, full_norm, leaf_norm, full_raw, leaf_raw in decorated:
            if is_excluded(full_raw): continue
            match = any((n in full_norm) or (n in leaf_norm) or (n in full_raw.lower()) or (n in leaf_raw.lower()) for n in needles)
            if match:
                already = chosen.get(canon)
                if (already is None) or prefer(getattr(already, "Name", None), v.Name):
                    chosen[canon] = v

    targets = [v for v in (chosen.get(c) for c in canon_targets) if v is not None]

    if not targets:
        print("[WARN] None of the plan’s requested views were found by synonyms/overrides.")
        try:
            all_names = [getattr(v, "Name", "") or "" for v in all_views]
            show = all_names[:max_suggestions]
            if show:
                print("[INFO] Here are some visible view names (first {}):".format(len(show)))
                for nm in show: print(f"  - {nm}")
                if plan_id is not None and canon_targets:
                    print("[INFO] Suggested SQL to pin regex_override (copy one per canon):")
                    nm0 = show[0]
                    for canon in canon_targets: print(_fmt_sql_update_regex(plan_id, canon, nm0))
                    print("-- Replace with exact name and re-run.")
        except Exception:
            pass
        return []

    print(f"[INFO] Selected {len(targets)} view(s) for this plan:")
    for canon in canon_targets:
        v = chosen.get(canon)
        if v: print(f"  - {canon}  =>  {v.Name}")
    return targets

# ---------------------------- PIPELINE --------------------------------

def _iter_embedded_objects_collection(eos):
    if eos is None: return
    if hasattr(eos, "Count") and hasattr(eos, "Item"):
        try:
            count = int(eos.Count)
            for i in range(1, count + 1):
                try: yield eos.Item(i)
                except Exception: continue
            return
        except Exception: pass
    if hasattr(eos, "__iter__"):
        for it in eos: yield it

def flatten_rich_text_item(item) -> str:
    try:
        txt = getattr(item, "Text", "")
        return str(txt) if txt is not None else ""
    except Exception:
        return ""

def coerce_insert_item_values(cur, unid: str, item_name: str, values_any, is_rich: bool=False):
    item_id = get_item_id(cur, item_name)
    vals = list(values_any) if isinstance(values_any, (list, tuple)) else [values_any]
    for idx, v in enumerate(vals):
        if isinstance(v, bool):
            insert_item_value(cur, unid, item_id, idx, 'bool', b=int(v))
        elif isinstance(v, (int, float)):
            insert_item_value(cur, unid, item_id, idx, 'number', n=float(v))
        elif isinstance(v, datetime):
            insert_item_value(cur, unid, item_id, idx, 'datetime', dt=as_dt(v))
        else:
            dt = as_dt(v)
            if dt is not None:
                insert_item_value(cur, unid, item_id, idx, 'datetime', dt=dt); continue
            if v is None:
                insert_item_value(cur, unid, item_id, idx, 'unknown')
            else:
                s = str(v)
                if len(s) <= 1024:
                    insert_item_value(cur, unid, item_id, idx, 'richtext' if is_rich else 'string', s=s)
                else:
                    insert_item_value(cur, unid, item_id, idx, 'richtext' if is_rich else 'text', s=s[:1024], t=s)

def extract_embedded_attachments_from_doc(doc, unid: str, tmp_dir: Path) -> List[Dict[str,Any]]:
    out: List[Dict[str,Any]] = []
    for item in doc.Items:
        eos = getattr(item, "EmbeddedObjects", None)
        if not eos: continue
        for idx, eo in enumerate(_iter_embedded_objects_collection(eos), 1):
            name     = getattr(eo, "Name", None) or f"Unnamed_{idx}"
            obj_type = getattr(eo, "Type", None)
            if obj_type not in (EO_TYPE_IMAGE, EO_TYPE_OLE, EO_TYPE_ATTACHMENT):
                log(f"  - skip embedded type {obj_type} ({name})"); continue
            safe = sanitize_folder_name(name, 255)
            dest = tmp_dir / safe
            try:
                eo.ExtractFile(str(dest))
            except Exception as e:
                log(f"  ! extract fail: {name} -> {e}"); continue
            try:
                sha, rel, size = cas_store(dest)
                kind = 'attachment' if obj_type == EO_TYPE_ATTACHMENT else ('image' if obj_type == EO_TYPE_IMAGE else 'ole')
                out.append(dict(
                    unid=unid,
                    item_name=getattr(item, "Name", None),
                    kind=kind,
                    filename=name,
                    mime_type=None,
                    size_bytes=size,
                    sha256=sha,
                    storage_path=rel,
                ))
            finally:
                try: dest.unlink(missing_ok=True)
                except Exception: pass
    return out

def get_doc_times(doc) -> Tuple[Optional[datetime], Optional[datetime]]:
    created  = as_dt(getattr(doc, "Created", None))
    modified = as_dt(getattr(doc, "LastModified", None))
    return created, modified

def get_doc_text_body(doc) -> str:
    parts = []
    for item in doc.Items:
        is_rich = bool(getattr(item, "EmbeddedObjects", None)) or hasattr(item, "AppendText") or getattr(item, "Type", None) == 1
        if is_rich:
            txt = flatten_rich_text_item(item)
            if txt: parts.append(f"{item.Name}:\n{txt}\n")
        else:
            vals = getattr(item, "Values", None)
            if vals:
                sv = "; ".join(str(x) for x in vals) if isinstance(vals, (list, tuple)) else str(vals)
                if sv and len(sv) <= 4096: parts.append(f"{item.Name}: {sv}")
    return "\n".join(parts)

def upsert_document_from_notes(doc, source_id: int, con, tmp_dir: Path, stats: Dict[str,int]) -> str:
    cur = con.cursor()
    unid = getattr(doc, "UniversalID", None)
    if not unid: return ""
    form = subject = author = None
    for item in doc.Items:
        nm = (getattr(item, "Name", "") or "").lower()
        vals = getattr(item, "Values", None)
        if not vals: continue
        v0 = vals[0] if isinstance(vals, (list, tuple)) else vals
        if subject is None and nm == "subject": subject = v0
        if form    is None and nm == "form":    form    = v0
        if author  is None and nm in ("author","from","postedby"): author = v0

    subject    = safe_str(subject, SUBJECT_MAX, "subject"); form = safe_str(form, FORM_MAX, "form"); author = safe_str(author, AUTHOR_MAX, "author")
    created_at, modified_at = get_doc_times(doc)
    text_body = get_doc_text_body(doc)
    text_hash = sha256_bytes(text_body.encode("utf-8")) if text_body else None
    attachments_meta = extract_embedded_attachments_from_doc(doc, unid, tmp_dir)
    has_atts = 1 if attachments_meta else 0
    note_id_hex = (str(getattr(doc, "NoteID", "") or "").strip() or None)

    doc_row = dict(
        unid=unid, note_id=note_id_hex, form=form, subject=subject, author=author,
        created_at=created_at, modified_at=modified_at,
        has_attachments=has_atts, text_hash=text_hash, text_body=text_body,
        doc_size_bytes=len(text_body.encode("utf-8")) if text_body else None
    )
    upsert_document(cur, source_id, doc_row); stats["upserted"] += 1

    for item in doc.Items:
        name = getattr(item, "Name", "UnknownItem")
        if not should_store_item(cur, name): continue
        is_rich = bool(getattr(item, "EmbeddedObjects", None)) or hasattr(item, "AppendText") or getattr(item, "Type", None) == 1
        if is_rich:
            txt = flatten_rich_text_item(item)
            coerce_insert_item_values(cur, unid, name, txt, is_rich=True)
        else:
            vals = getattr(item, "Values", None)
            if vals is not None: coerce_insert_item_values(cur, unid, name, vals, is_rich=False)

    att_ids_by_filename: Dict[str,int] = {}
    for meta in attachments_meta:
        att_id = insert_attachment(cur, meta)
        if att_id: att_ids_by_filename[meta.get("filename") or ""] = att_id
        stats["atts"] += 1

    for item in doc.Items:
        if getattr(item, "Name", "") != "$FILE": continue
        if not should_store_item(cur, "$FILE"): continue
        item_id = get_item_id(cur, "$FILE")
        vals = getattr(item, "Values", []) or []
        if not isinstance(vals, (list, tuple)): vals = [vals]
        for i, fn in enumerate(vals):
            fn_s = str(fn); att_id = att_ids_by_filename.get(fn_s)
            insert_item_value(cur, unid, item_id, i, 'string', s=fn_s, att_id=att_id)

    con.commit()
    return unid

def snapshot_view_entries(view, category_col_idx: int = CATEGORY_COLUMN_INDEX, max_restarts: int = 5):
    out: List[Tuple[str, Optional[str]]] = []
    seen: set = set()

    def _get_entries():
        return view.AllEntries

    restarts = 0
    entries = resilient_com(_get_entries)
    entry = resilient_com(entries.GetFirstEntry)

    while entry:
        try:
            if resilient_com(lambda e=entry: e.IsDocument):
                doc = resilient_com(lambda e=entry: e.Document)
                if doc:
                    unid = getattr(doc, "UniversalID", None)
                    if unid and unid not in seen:
                        try:
                            cols = resilient_com(lambda e=entry: e.ColumnValues) or []
                        except Exception:
                            cols = []
                        raw = str(cols[category_col_idx]).strip() if len(cols) > category_col_idx else ""
                        category_path = None
                        if raw:
                            parts = [sanitize_folder_name(p.strip()) for p in raw.split("\\") if p.strip()]
                            if parts:
                                category_path = "\\".join(parts)
                        out.append((unid, category_path))
                        seen.add(unid)

            entry = resilient_com(entries.GetNextEntry, entry)

        except Exception as e:
            if _is_transient_com_error(e) and restarts < max_restarts:
                restarts += 1
                print(f"[WARN] View iteration transient error; restarting ({restarts}/{max_restarts})")
                try:
                    entries = resilient_com(_get_entries)
                    entry = resilient_com(entries.GetFirstEntry)
                except Exception:
                    print(f"[WARN] Failed to restart; proceeding with snapshot of {len(out)} entries.")
                    break
                continue
            print(f"[WARN] Snapshot aborted after {len(out)} entries due to error: {e}")
            break

    return out

def process_view_into_db(notes_db, view, source_id: int, con, stats: Dict[str,int],
                         plan_id: Optional[int]=None, batch_size: int=50,
                         reopen_ctx: Optional[NotesReopenContext]=None):
    tmp_dir = Path(tempfile.mkdtemp(prefix="notes_tmp_"))
    view_name = getattr(view, "Name", "UnknownView")
    try:
        print(f"[INFO] → View '{view_name}'")

        snapshot = snapshot_view_entries(view)
        print(f"[INFO]   Snapshot captured: {len(snapshot)} entries")
        snapshot_sig = _sig_for_snapshot(snapshot)

        cur = con.cursor()
        ckpt = None
        if plan_id is not None:
            ckpt = load_checkpoint(cur, plan_id, source_id, view_name)
            if ckpt and ckpt["snapshot_sig"] != snapshot_sig:
                print("[INFO] View membership changed; restarting index at 0")
                ckpt = None

        next_idx = (ckpt["next_index"] if ckpt else 0)
        total = len(snapshot)

        def _get_doc(unid: str):
            return notes_db.GetDocumentByUNID(unid)

        if reopen_ctx is None:
            def _open_db_again():
                return notes_db
            def _get_view_again(db, vname):
                all_views = list(db.Views)
                for v in all_views:
                    if getattr(v, "Name", "") == vname:
                        return v
                raise RuntimeError(f"View '{vname}' not found after reopen")
            reopen_ctx = NotesReopenContext(_open_db_again, _get_view_again, view_name=view_name)

        while next_idx < total:
            end = min(next_idx + batch_size, total)
            batch = snapshot[next_idx:end]

            resilient_com_with_reopen(lambda: getattr(view, "Name"), reopen_ctx)

            for (unid, category_path) in batch:
                try:
                    doc = resilient_com_with_reopen(lambda u=unid: _get_doc(u), reopen_ctx)
                    if not doc:
                        stats["errors"] += 1
                        print(f"[WARN] Skipping UNID {unid}: not found")
                        continue

                    upserted_unid = upsert_document_from_notes(doc, source_id, con, tmp_dir, stats)
                    if upserted_unid:
                        insert_document_view(con.cursor(), upserted_unid, view_name, category_path)
                    stats["scanned"] += 1

                except Exception as e:
                    stats["errors"] += 1
                    print(f"[WARN] Skipping UNID {unid} due to error: {e}")

            con.commit()
            next_idx = end
            if plan_id is not None:
                upsert_checkpoint(cur, plan_id, source_id, view_name, snapshot_sig, next_idx, batch[-1][0] if batch else None)
                con.commit()
                print(f"[INFO]   Checkpoint updated: {next_idx}/{total}")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

# ------------------------------- MAIN ---------------------------------

def load_ingestion_plans(con) -> List[Dict[str, Any]]:
    cur = con.cursor()
    cur.execute("""
      SELECT p.id, p.server_name, p.filepath
      FROM dbo.ingestion_plans p
      WHERE p.enabled = 1
      ORDER BY p.server_name, p.filepath
    """)
    plans = []
    for row in cur.fetchall() or []:
        plan = {"id": row[0], "server_name": row[1], "filepath": row[2]}
        cur.execute("""
          SELECT canon_name, COALESCE(NULLIF(regex_override,''), NULL) AS regex_override
          FROM dbo.ingestion_plan_views
          WHERE plan_id=? AND enabled=1
          ORDER BY priority, canon_name
        """, (plan["id"],))
        rows = cur.fetchall() or []
        plan["canon_targets"] = [r[0] for r in rows]
        plan["regex_overrides"] = {r[0]: r[1] for r in rows if r[1]}
        plans.append(plan)
    if not plans:
        print("[WARN] No enabled ingestion plans found.")
    return plans

def main():
    import struct as _struct, sys as _sys
    print(f"[BOOT] Python {_sys.version.split()[0]} ({'64' if _struct.calcsize('P')==8 else '32'}-bit)")

    try:
        CAS_ROOT.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        import tempfile
        temp_root = Path(tempfile.gettempdir()) / "notes_cas"
        temp_root.mkdir(parents=True, exist_ok=True)
        print(f"[WARN] CAS_ROOT not writable; fell back to: {temp_root}")
        globals()['CAS_ROOT'] = temp_root

    ensure_schema()

    with sql_db() as con:
        cur = con.cursor()
        # Seed a default plan when empty
        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM dbo.ingestion_plans)
            BEGIN
                INSERT INTO dbo.ingestion_plans(server_name, filepath, enabled, notes)
                VALUES (?, ?, 1, 'Seeded plan');
                DECLARE @pid BIGINT = SCOPE_IDENTITY();
                INSERT INTO dbo.ingestion_plan_views(plan_id, canon_name, enabled, priority)
                VALUES (@pid, 'Person By Surname', 1, 10),
                       (@pid, 'Person By Organization', 1, 20),
                       (@pid, 'Organizational Structure', 1, 30);
            END
        """, (PREF_SERVER, PREF_SERVER_PATH))
        con.commit()

        plans = load_ingestion_plans(con)
        if not plans:
            print("[INFO] Nothing to do. Populate ingestion_plans and ingestion_plan_views.")
            return

        for plan in plans:
            server = plan["server_name"]; path = plan["filepath"]
            canon_targets = plan.get("canon_targets", []) or []
            overrides     = plan.get("regex_overrides", {}) or {}

            def _open_db_again_closure(s=server, p=path):
                session = win32com.client.Dispatch("Lotus.NotesSession")
                session.Initialize(LOTUS_PASSWORD)
                def _get_db():
                    db = session.GetDatabase(s, p)
                    if not db.IsOpen:
                        try: db.Open(s, p)
                        except Exception: pass
                    return db
                return resilient_com(_get_db)

            def _get_view_again_closure(db, vname):
                all_views = list(db.Views)
                for v in all_views:
                    if getattr(v, "Name", "") == vname:
                        return v
                raise RuntimeError(f"View '{vname}' not found after reopen")

            try:
                session, server_eff, filepath_eff, notes_db = open_database(server, path)
            except Exception as e:
                print(f"[ERROR] Failed to open {server}:{path} -> {e}")
                continue

            db_title   = getattr(notes_db, "Title", None)
            replica_id = getattr(notes_db, "ReplicaID", None)

            source_id = get_or_create_source(cur, server_eff, filepath_eff, db_title, replica_id)
            run_id    = start_etl_run(cur, source_id)
            stats     = dict(scanned=0, upserted=0, atts=0, errors=0)
            con.commit()

            try:
                targets = select_views_for_plan(notes_db, canon_targets, overrides, plan_id=plan["id"])
                if not targets:
                    print(f"[INFO] No views selected for plan {server}:{path}.")
                else:
                    for v in targets:
                        vname = getattr(v, "Name", "UnknownView")
                        reopen_ctx = NotesReopenContext(
                            open_db_fn=_open_db_again_closure,
                            get_view_fn=_get_view_again_closure,
                            view_name=vname
                        )
                        process_view_into_db(
                            notes_db, v, source_id, con, stats,
                            plan_id=plan["id"], batch_size=50, reopen_ctx=reopen_ctx
                        )
            finally:
                finish_etl_run(cur, run_id, stats)
                con.commit()

    print("[DONE] Ingest complete for all enabled plans.")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
