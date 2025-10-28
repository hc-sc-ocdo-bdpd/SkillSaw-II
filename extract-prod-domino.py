import os
import re
import time
import win32com.client
import concurrent.futures

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Preferred Domino server for remote open
PREF_SERVER = r"APP02/HC-SC/GC/CA"   # change if needed

NSF_PATH = "FND-CHHAD-Reference-Libraryl.nsf"
# NSF_PATH = "sapaccess.nsf"

# Prefer env var; falls back to literal if set here
LOTUS_PASSWORD = os.environ.get("LOTUS_PASSWORD", "")

OUTPUT_DIR = "output"
CATEGORY_COLUMN_INDEX = 0
MAX_FOLDER_NAME_LENGTH = 100
DEBUG = True

# Notes EmbeddedObject.Type (for COM extraction)
EO_TYPE_IMAGE       = 1452
EO_TYPE_OLE         = 1453
EO_TYPE_ATTACHMENT  = 1454

SESSION = None  # set in __main__

# ---------------------------------------------------------------------------
# Resilience helpers (COM retries + open with fallbacks)
# ---------------------------------------------------------------------------
try:
    import pywintypes
except Exception:
    pywintypes = None

# Common transient Notes/COM/network messages to treat as retryable
_TRANSIENT_COM_SUBSTRINGS = [
    "Network",
    "The server is not responding",
    "Timed out",
    "unable to find path to server",
    "no network connection",
    "port error",
    "NotesViewEntryCollection",
    "Object variable not set",
    "Argument has been deleted",
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

def resilient_com(callable_fn, *args, tries=6, backoff=1.5, **kwargs):
    attempt = 0
    delay = backoff
    last = None
    while attempt < tries:
        try:
            return callable_fn(*args, **kwargs)
        except Exception as e:
            last = e
            if not _is_transient_com_error(e):
                raise
            attempt += 1
            if attempt >= tries:
                break
            time.sleep(delay)
            delay *= 2
    raise last

def open_database(server_name: str, filepath: str):
    """
    Try opening the NSF in this order:
      1) Server: (server_name, filepath)
      2) UI fallback: whatever DB is open in Notes UI
      3) Local replica/file: ("", filepath)
    Returns: (session, effective_server, effective_filepath, notes_db)
    """
    filepath = filepath.replace("/", "\\")
    session = win32com.client.Dispatch("Lotus.NotesSession")
    session.Initialize(LOTUS_PASSWORD)

    # 1) Server first
    def _get_db_server():
        db = session.GetDatabase(server_name, filepath)
        if not db.IsOpen:
            try:
                db.Open(server_name, filepath)
            except Exception:
                pass
        return db

    db = resilient_com(_get_db_server)
    if db and db.IsOpen:
        print(f"[INFO] Opened server DB: {server_name}:{filepath}")
        return session, server_name, filepath, db

    # 2) UI fallback
    try:
        uiw = win32com.client.Dispatch("Notes.NotesUIWorkspace")
        uidb = uiw.CurrentDatabase
        ui_server = getattr(uidb, "Server", "")
        ui_file   = getattr(uidb, "FilePath", "")
        if ui_file:
            def _get_db_ui():
                db2 = session.GetDatabase(ui_server, ui_file)
                if not db2.IsOpen:
                    try:
                        db2.Open(ui_server, ui_file)
                    except Exception:
                        pass
                return db2
            db2 = resilient_com(_get_db_ui)
            if db2 and db2.IsOpen:
                print(f"[INFO] Auto-detected UI DB: {ui_server}:{ui_file}")
                return session, ui_server, ui_file, db2
    except Exception:
        pass

    # 3) Local fallback (server="")
    def _get_db_local():
        db3 = session.GetDatabase("", filepath)
        if not db3.IsOpen:
            try:
                db3.Open("", filepath)
            except Exception:
                pass
        return db3

    db3 = resilient_com(_get_db_local)
    if db3 and db3.IsOpen:
        print(f"[INFO] Opened LOCAL DB: :{filepath}")
        return session, "", filepath, db3

    raise RuntimeError(f"Failed to open DB {server_name}:{filepath}. Check server, path, and credentials.")

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------
def log(msg):
    if DEBUG:
        print(msg)

def sanitize_folder_name(name, max_length=MAX_FOLDER_NAME_LENGTH):
    if not name or not name.strip():
        return "Unnamed"
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'[\s_]+', '_', name)
    return name[:max_length].strip('_')

def get_document_subject(doc):
    for item in doc.Items:
        if getattr(item, "Name", "").lower() == "subject" and getattr(item, "Values", None):
            return item.Values[0]
    for item in doc.Items:
        if getattr(item, "Name", "").lower() == "form" and getattr(item, "Values", None):
            return f"Form_{item.Values[0]}"
    return "UnnamedDocument"

def _iter_embedded_objects_collection(eos):
    if eos is None:
        return
    if hasattr(eos, "Count") and hasattr(eos, "Item"):
        try:
            count = int(eos.Count)
            for i in range(1, count + 1):
                try:
                    yield eos.Item(i)
                except Exception:
                    continue
            return
        except Exception:
            pass
    if hasattr(eos, "__iter__"):
        for it in eos:
            yield it

# ---------------------------------------------------------------------------
# Attachment / Embed discovery (COM, $FILE, MIME)
# ---------------------------------------------------------------------------

def discover_embeds_via_embeddedobjects(doc):
    """Return list of dicts: {'kind','name','item'} via RichTextItem.EmbeddedObjects."""
    found = []
    for item in doc.Items:
        eos = getattr(item, "EmbeddedObjects", None)
        if not eos:
            continue
        for eo in _iter_embedded_objects_collection(eos):
            name = getattr(eo, "Name", "unknown")
            t = getattr(eo, "Type", None)
            if t == EO_TYPE_ATTACHMENT:
                kind = "attachment"
            elif t == EO_TYPE_IMAGE:
                kind = "image"
            elif t == EO_TYPE_OLE:
                kind = "ole"
            else:
                kind = "object"
            found.append({"kind": kind, "name": name, "item": getattr(item, "Name", "")})
    return found

def discover_embeds_via_file_items(doc):
    """Return list of dicts for $FILE items (common Domino storage for attachments)."""
    found = []
    for item in doc.Items:
        name = getattr(item, "Name", "")
        if name != "$FILE":
            continue
        vals = getattr(item, "Values", []) or []
        for v in vals:
            s = str(v).strip()
            if s:
                found.append({"kind": "attachment", "name": s, "item": "$FILE"})
    return found

def _mime_entity_filename(entity):
    """Best-effort filename extraction from MIME entity headers."""
    try:
        if hasattr(entity, "GetHeader"):
            cd = entity.GetHeader("Content-Disposition")
            ct = entity.GetHeader("Content-Type")
        else:
            cd = getattr(entity, "ContentDisposition", None)
            ct = getattr(entity, "ContentType", None)
    except Exception:
        cd = None
        ct = None

    for header in (cd, ct):
        if not header:
            continue
        hs = str(header)
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";\r\n]+)"?', hs, flags=re.I)
        if m:
            return m.group(1)
        m = re.search(r'name="?([^";\r\n]+)"?', hs, flags=re.I)
        if m:
            return m.group(1)
    return None

def _mime_is_attachment(entity):
    try:
        if hasattr(entity, "GetHeader"):
            cd = entity.GetHeader("Content-Disposition")
            if cd and ("attachment" in str(cd).lower() or "inline" in str(cd).lower()):
                return True
    except Exception:
        pass
    return _mime_entity_filename(entity) is not None

def discover_embeds_via_mime(doc):
    """Walk MIME tree and return list of dicts {'kind','name','item'} for parts with filenames."""
    found = []
    try:
        root = doc.GetMIMEEntity("")  # first MIME entity
    except Exception:
        root = None
    if not root:
        return found

    stack = [root]
    while stack:
        ent = stack.pop()
        try:
            if str(getattr(ent, "ContentType", "")).lower().startswith("multipart"):
                child = getattr(ent, "GetFirstChild", lambda: None)()
                while child:
                    stack.append(child)
                    child = getattr(child, "GetNextSibling", lambda: None)()
        except Exception:
            pass

        try:
            if _mime_is_attachment(ent):
                fname = _mime_entity_filename(ent) or "attachment"
                found.append({"kind": "attachment", "name": fname, "item": "Body"})
        except Exception:
            continue

    return found

def discover_all_embeds(doc):
    """Union of all discovery methods (dedup by kind+name)."""
    candidates = []
    candidates += discover_embeds_via_embeddedobjects(doc)
    candidates += discover_embeds_via_file_items(doc)
    candidates += discover_embeds_via_mime(doc)

    seen = set()
    out = []
    for d in candidates:
        key = (d["kind"].lower(), d["name"])
        if key in seen:
            continue
        seen.add(key)
        out.append(d)
    return out

# ---------------------------------------------------------------------------
# Text injection (best-effort inline marking by filename mention)
# ---------------------------------------------------------------------------

def _escape_for_regex(s):
    return re.escape(s)

def inject_markers_by_filename(text, embeds):
    """
    If the body text mentions an embedded filename, wrap the first occurrence
    with a visible marker. Best effort—case-insensitive, whole-token-ish matching.
    """
    if not text:
        return text
    modified = text
    used = set()

    # Sort longer names first to avoid partial overlaps
    names = sorted({e["name"] for e in embeds if e["kind"] == "attachment" and e["name"]}, key=len, reverse=True)

    for name in names:
        if name in used:
            continue
        # Build a regex that matches name as a token (allowing punctuation boundaries)
        pattern = r'(?i)(?<!\w)(' + _escape_for_regex(name) + r')(?!\w)'
        def repl(m):
            used.add(name)
            return f"[[EMBEDDED_ATTACHMENT: {m.group(1)}]]"
        # replace only first occurrence
        new_text, n = re.subn(pattern, repl, modified, count=1)
        if n > 0:
            modified = new_text

    return modified

# ---------------------------------------------------------------------------
# Document extraction
# ---------------------------------------------------------------------------

def extract_document(doc, folder_path):
    subject = get_document_subject(doc)
    uid = getattr(doc, "UniversalID", "unknown")[:8]
    doc_dir = os.path.join(folder_path, sanitize_folder_name(f"{subject}_{uid}"))
    os.makedirs(doc_dir, exist_ok=True)

    # Discover the doc's embeds once
    all_embeds = discover_all_embeds(doc)

    with open(os.path.join(doc_dir, "document.txt"), "w", encoding="utf-8") as f:
        f.write(f"----- Document: {subject} ({uid}) -----\n")

        for item in doc.Items:
            try:
                name = getattr(item, "Name", "UnknownItem")
                itype = getattr(item, "Type", None)
                if itype == 1:  # RichText-ish
                    f.write(f"{name} (RichText):\n")

                    # Body text
                    text = getattr(item, "Text", "") or ""
                    # Best-effort inline injection by filename mention
                    text_with_marks = inject_markers_by_filename(text, all_embeds)
                    f.write(text_with_marks.strip() + "\n")

                    # Single per-item summary AFTER the body
                    if all_embeds:
                        parts = "; ".join(f"{e['kind']}:{e['name']}" for e in all_embeds)
                        f.write(f"[EMBEDDED_SUMMARY_ITEM name='{name}': {parts}]\n")

                else:
                    vals = getattr(item, "Values", None)
                    f.write(f"{name}: {vals}\n")

            except Exception as e:
                f.write(f"{getattr(item, 'Name', 'UnknownItem')}: <Error reading value: {e}>\n")

        f.write("--------------------\n")

    # Second pass: actually extract files to disk (COM EmbeddedObjects works)
    for item in doc.Items:
        eos = getattr(item, "EmbeddedObjects", None)
        if not eos:
            continue
        for idx, eo in enumerate(_iter_embedded_objects_collection(eos), 1):
            name = getattr(eo, "Name", None) or f"Unnamed_{idx}"
            obj_type = getattr(eo, "Type", None)
            safe = sanitize_folder_name(name, 255)
            dest = os.path.join(doc_dir, safe)

            if obj_type not in (EO_TYPE_IMAGE, EO_TYPE_OLE, EO_TYPE_ATTACHMENT):
                print(f"⚠ Skipping object {idx}: unsupported type {obj_type} ({name})")
                continue

            try:
                eo.ExtractFile(dest)
                print(f"✅ Extracted [{obj_type}] '{name}' → {dest}")
            except Exception as e:
                print(f"‼ Failed to extract [{obj_type}] '{name}': {e}")

# ---------------------------------------------------------------------------
# View processing (threaded)
# ---------------------------------------------------------------------------

def process_view(view, output_dir):
    view_name = view.Name
    safe_view = sanitize_folder_name(view_name)
    view_dir = os.path.join(output_dir, safe_view)
    os.makedirs(view_dir, exist_ok=True)
    print(f"[INFO] → View '{view_name}'")

    entries = view.AllEntries
    entry = entries.GetFirstEntry()
    extracted = 0

    while entry:
        next_entry = entries.GetNextEntry(entry)
        if entry.IsDocument:
            doc = entry.Document
            if doc:
                cols = getattr(entry, "ColumnValues", []) or []
                cat_string = (str(cols[CATEGORY_COLUMN_INDEX]).strip()
                              if len(cols) > CATEGORY_COLUMN_INDEX else "")
                cat_string = cat_string or "Uncategorized"
                parts = [sanitize_folder_name(p.strip())
                         for p in cat_string.split("\\") if p.strip()]
                parts = parts or ["Uncategorized"]

                final_dir = os.path.join(view_dir, *parts)
                os.makedirs(final_dir, exist_ok=True)
                extract_document(doc, final_dir)
                extracted += 1
        entry = next_entry

    print(f".[INFO]   {extracted} documents extracted from '{view_name}'")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Try server → UI → local fallbacks
    SESSION, EFFECTIVE_SERVER, EFFECTIVE_PATH, db = open_database(PREF_SERVER, NSF_PATH)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("[INFO] All available views in the database:")
    for v in db.Views:
        print(f"  - {v.Name}")

    target_keywords = ["by category", "by author"]
    views_to_process = [
        v for v in db.Views
        if any(kw in v.Name.strip().lower() for kw in target_keywords)
    ]

    print(f"[INFO] Found {len(views_to_process)} target views.\n")

    # If you see COM marshaling issues, process views sequentially instead.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_view, view, OUTPUT_DIR)
            for view in views_to_process
        ]
        concurrent.futures.wait(futures)

    print("[DONE] All views processed.")
