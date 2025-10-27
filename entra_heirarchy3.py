#!/usr/bin/env python3
"""
entra_heirarchy.py — Single-run Graph org export (managers are ALWAYS resolved)

Outputs (every run):
  - users_flat.json        (flat users with managerId populated)
  - org_for_viewer.json    (flat, minimal for HTML viewer)
  - org_tree.json          (nested tree for inspection)

Env (required):
  AZ_TENANT_ID, AZ_CLIENT_ID, AZ_CLIENT_SECRET
Optional:
  USER_FILTER (e.g., accountEnabled eq true)
  PAGE_SIZE (default 100)

Managers resolution (always in this run):
  1) If a local managers file is present, use it:
     - $MANAGERS_FILE or any of:
       ./managers.json, ./manager_map.json, ./managers_map.json, ./child_to_manager.json
     Supported shapes:
       - { childId: managerId, ... }
       - { managerId: [childId,...], ... }
       - [ {"managerId":"...", "reports":[...]}, ... ]
       - [ {"id":"childId","managerId":"..."}, ... ]
  2) Otherwise, call Graph in $batch: /users/{id}/manager?$select=id

Note: No checkpoint/resume. Each run fully resolves manager relationships.
"""

import os, json, time, base64, random, logging
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import requests
from msal import ConfidentialClientApplication

# ----------------------------
# Config (env only; no inline secrets)
# ----------------------------
TENANT_ID      = "42fd9015-de4d-4223-a368-baeacab48927"
CLIENT_ID      = "2bc1c9b9-d0ad-4ff1-ac90-f5f54f942efb"
CLIENT_SECRET  = "o5B8Q~XnkYM_BFpZ3anY~5lzrSiVqqGW3P_60br1"
if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET):
    raise RuntimeError("Set AZ_TENANT_ID, AZ_CLIENT_ID, AZ_CLIENT_SECRET in environment.")

AUTHORITY  = f"https://login.microsoftonline.com/{TENANT_ID}"
GRAPH_ROOT = "https://graph.microsoft.com/v1.0"

# Adaptive throttling
PAGE_SIZE            = int(os.getenv("PAGE_SIZE", "100"))
MIN_PAGE_SIZE        = 25
BATCH_LIMIT          = 10
REQUEST_TIMEOUT_SEC  = 30
MAX_ATTEMPTS         = 8

INTER_PAGE_SLEEP     = 0.35
INTER_BATCH_SLEEP    = 0.40
MAX_CONSEC_SERVICE_ERRORS = 6

OUT_USERS_FILE   = "users_flat.json"
OUT_TREE_FILE    = "org_tree.json"
OUT_VIEWER_FILE  = "org_for_viewer.json"
USER_FILTER      = os.getenv("USER_FILTER", "").strip()

REQUIRED_ROLES = {"Directory.Read.All", "User.Read.All"}

# Slim fields for viewer + name fallbacks
USER_SELECT_FIELDS = [
    "id",
    "displayName",
    "userPrincipalName",
    "mailNickname",
    "mail",
    "jobTitle",
    "department",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("graph-org")

# ----------------------------
# Auth + role check
# ----------------------------
def acquire_token() -> str:
    app = ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
    res = app.acquire_token_for_client(["https://graph.microsoft.com/.default"])
    if "access_token" not in res:
        raise RuntimeError(f"Auth failed: {res}")
    return res["access_token"]

def _decode_jwt_noverify(tok: str) -> dict:
    parts = tok.split(".")
    if len(parts) != 3: return {}
    def pad(s): return s + "=" * (-len(s) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(pad(parts[1])).decode("utf-8"))
    except Exception:
        return {}

def assert_roles(token: str):
    roles = set(_decode_jwt_noverify(token).get("roles", []))
    missing = REQUIRED_ROLES - roles
    if missing:
        log.warning(f"Access token missing app roles: {missing}. Present: {sorted(roles)}")

# ----------------------------
# HTTP with adaptive backoff
# ----------------------------
class AdaptiveLimiter:
    def __init__(self):
        self.consec_service_errors = 0
        self.page_sleep = INTER_PAGE_SLEEP
        self.page_size = PAGE_SIZE

    def note_success(self):
        self.consec_service_errors = max(0, self.consec_service_errors - 1)

    def note_service_error(self):
        self.consec_service_errors += 1
        if self.consec_service_errors in (3, 4):
            self.page_sleep = min(self.page_sleep + 0.25, 2.0)
        if self.consec_service_errors >= MAX_CONSEC_SERVICE_ERRORS:
            nap = 30 + 10 * (self.consec_service_errors - MAX_CONSEC_SERVICE_ERRORS)
            log.warning(f"Heavy throttling; sleeping {nap}s and reducing $top.")
            time.sleep(nap)
            self.page_size = max(MIN_PAGE_SIZE, self.page_size // 2)
            self.page_sleep = min(self.page_sleep + 0.5, 3.0)
            self.consec_service_errors = 0

limiter = AdaptiveLimiter()

def _do_request(session_fn, url, *, headers=None, json_body=None):
    return session_fn(url, headers=headers or {}, json=json_body, timeout=REQUEST_TIMEOUT_SEC)

def with_backoff(session_fn, url, *, headers=None, json_body=None):
    delay = 1.0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        resp = _do_request(session_fn, url, headers=headers, json_body=json_body)
        if resp.status_code in (200, 201, 204):
            limiter.note_success()
            return resp

        if resp.status_code in (429, 503, 504):
            limiter.note_service_error()
            ra = resp.headers.get("Retry-After")
            try:
                wait = float(ra) if ra else delay + random.uniform(0, 0.5 * delay)
            except Exception:
                wait = delay + random.uniform(0, 0.5 * delay)
            delay = min(delay * 2, 30)
            short_url = url.split("?")[0]
            log.warning(f"Throttled {resp.status_code} on {short_url} "
                        f"(attempt {attempt}/{MAX_ATTEMPTS}); retrying in {wait:.2f}s …")
            time.sleep(wait)
            continue

        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise RuntimeError(f"HTTP {resp.status_code}: {detail}")

    raise RuntimeError(f"Exceeded retry attempts for {url}")

def graph_get(s: requests.Session, url: str, token: str) -> requests.Response:
    return with_backoff(s.get, url, headers={"Authorization": f"Bearer {token}"})

def graph_post_json(s: requests.Session, url: str, token: str, body: Any) -> requests.Response:
    return with_backoff(s.post, url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json_body=body)

# ----------------------------
# Managers file (auto-detect + parse)
# ----------------------------
def autodetect_managers_path() -> Optional[str]:
    candidates = [
        os.getenv("MANAGERS_FILE", "").strip(),
        "managers.json",
        "manager_map.json",
        "managers_map.json",
        "child_to_manager.json",
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None

def load_managers_file(path: str) -> Dict[str, Optional[str]]:
    """
    Returns a child->manager map from:
      - dict child->manager: {childId: managerId, ...}
      - dict manager->reports: {managerId: [childId,...], ...}
      - list of objects: [{"managerId":"...", "reports":[...]}, ...]
      - list of pairs:   [{"id":"childId","managerId":"..."}, ...]
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    child_to_manager: Dict[str, Optional[str]] = {}

    if isinstance(data, dict):
        any_list = any(isinstance(v, list) for v in data.values())
        if any_list:
            for m, kids in data.items():
                for k in (kids or []):
                    child_to_manager[str(k)] = str(m)
        else:
            for child, mgr in data.items():
                child_to_manager[str(child)] = str(mgr) if mgr is not None else None

    elif isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            if "managerId" in row and isinstance(row.get("reports"), list):
                m = row.get("managerId")
                for k in row.get("reports") or []:
                    child_to_manager[str(k)] = str(m) if m is not None else None
            elif "id" in row and "managerId" in row:
                child_to_manager[str(row["id"])] = str(row["managerId"]) if row["managerId"] is not None else None
    else:
        raise ValueError("Unsupported managers file format.")

    return child_to_manager

# ----------------------------
# Cheap probe
# ----------------------------
def probe_org(token: str):
    with requests.Session() as s:
        url = f"{GRAPH_ROOT}/organization?$select=id,displayName&$top=1"
        try:
            _ = graph_get(s, url, token).json()
            log.info("Probe /organization ok.")
        except Exception as e:
            log.warning(f"/organization probe issue: {e}")

# ----------------------------
# Fetch users
# ----------------------------
def fetch_all_users(token: str) -> List[Dict[str, Any]]:
    fields = ",".join(USER_SELECT_FIELDS)
    base = f"{GRAPH_ROOT}/users?$select={fields}&$top={limiter.page_size}"
    if USER_FILTER:
        base += f"&$filter={USER_FILTER}"

    url = base
    users: List[Dict[str, Any]] = []
    with requests.Session() as s:
        while url:
            r = graph_get(s, url, token)
            data = r.json()
            batch = data.get("value", [])
            users.extend(batch)
            url = data.get("@odata.nextLink")
            log.info(f"Fetched users: {len(users)} so far (page_sleep={limiter.page_sleep:.2f}s, top={limiter.page_size})")
            if url:
                time.sleep(limiter.page_sleep)
    return users

# ----------------------------
# Resolve managers (Graph) — for ALL user_ids in this run
# ----------------------------
def batch_get_managers(token: str, user_ids: List[str]) -> Dict[str, Optional[str]]:
    manager_map: Dict[str, Optional[str]] = {}
    queue: deque[Tuple[str, int]] = deque([(uid, MAX_ATTEMPTS) for uid in user_ids])

    with requests.Session() as s:
        while queue:
            chunk: List[Tuple[str, int]] = []
            while queue and len(chunk) < BATCH_LIMIT:
                chunk.append(queue.popleft())

            body = {"requests": []}
            for idx, (uid, _) in enumerate(chunk, start=1):
                rel_url = f"/users/{uid}/manager?$select=id,displayName"
                body["requests"].append({"id": str(idx), "method": "GET", "url": rel_url})

            resp = graph_post_json(s, f"{GRAPH_ROOT}/$batch", token, body)
            responses = (resp.json() or {}).get("responses", [])
            id2uid: Dict[str, Tuple[str, int]] = {str(i+1): chunk[i] for i in range(len(chunk))}

            for item in responses:
                rid = item.get("id")
                status = int(item.get("status", 0))
                body = item.get("body", {}) or {}
                headers = {k.lower(): v for k, v in (item.get("headers", {}) or {}).items()}
                uid, attempts_left = id2uid[rid]

                if status == 200 and "id" in body:
                    manager_map[uid] = body["id"]
                elif status in (404, 204):
                    manager_map[uid] = None
                elif status in (429, 503, 504):
                    limiter.note_service_error()
                    ra = headers.get("retry-after")
                    try:
                        wait = float(ra) if ra else random.uniform(1.0, 3.0)
                    except Exception:
                        wait = random.uniform(1.0, 3.0)
                    if attempts_left > 1:
                        time.sleep(wait)
                        queue.append((uid, attempts_left - 1))
                    else:
                        log.error(f"$batch manager for {uid} exhausted retries; -> None")
                        manager_map[uid] = None
                else:
                    short = str(body)[:300].replace("\n", " ")
                    log.error(f"$batch manager for {uid} failed status={status}, body={short}")
                    manager_map[uid] = None

            time.sleep(INTER_BATCH_SLEEP)

    log.info(f"Resolved managers: {len(manager_map)}/{len(user_ids)}")
    return manager_map

# ----------------------------
# Build tree + viewer data
# ----------------------------
def build_hierarchy(users: List[Dict[str, Any]], manager_map: Dict[str, Optional[str]]):
    # Assign managerId, prepare children arrays
    nodes: Dict[str, Dict[str, Any]] = {}
    for u in users:
        uid = u["id"]
        u["managerId"] = manager_map.get(uid)  # may be None
        u["reports"] = []  # will hold child OBJECTS for tree
        nodes[uid] = u

    roots: List[Dict[str, Any]] = []
    for u in users:
        m = u.get("managerId")
        if m and m in nodes:
            nodes[m]["reports"].append(u)
        else:
            roots.append(u)

    # Sort for stability
    def sort_recursive(n: Dict[str, Any]):
        n["reports"].sort(key=lambda x: (x.get("displayName") or "").lower())
        for c in n["reports"]:
            sort_recursive(c)

    for r in roots:
        sort_recursive(r)

    # Flat viewer array
    flat_view: List[Dict[str, Any]] = []
    for u in users:
        flat_view.append({
            "id": u["id"],
            "displayName": u.get("displayName") or u.get("mailNickname") or u.get("userPrincipalName") or u["id"],
            "userPrincipalName": u.get("userPrincipalName"),
            "mailNickname": u.get("mailNickname"),
            "jobTitle": u.get("jobTitle"),
            "department": u.get("department"),
            "managerId": u.get("managerId"),
            "reports": [c["id"] for c in u.get("reports", [])],
        })

    return roots, flat_view

# ----------------------------
# MAIN — single-run manager resolution
# ----------------------------
def main():
    token = acquire_token()
    log.info("Authenticated to Microsoft Graph.")
    assert_roles(token)

    # Probe (cheap)
    probe_org(token)

    # Users for this run
    users = fetch_all_users(token)
    if not users:
        log.warning("No users returned.")
        for p in (OUT_USERS_FILE, OUT_TREE_FILE, OUT_VIEWER_FILE):
            with open(p, "w", encoding="utf-8") as f: json.dump([], f, indent=2, ensure_ascii=False)
        return

    # Managers: always resolve now (no resume/checkpoints)
    managers_path = autodetect_managers_path()
    if managers_path:
        log.info(f"Using managers file: {managers_path}")
        manager_map = load_managers_file(managers_path)
    else:
        log.info(f"No local managers file found; resolving managers from Graph for {len(users)} users …")
        manager_map = batch_get_managers(token, [u["id"] for u in users])

    # Build outputs
    roots, flat_view = build_hierarchy(users, manager_map)

    with open(OUT_USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2, ensure_ascii=False)
    with open(OUT_VIEWER_FILE, "w", encoding="utf-8") as f:
        json.dump(flat_view, f, indent=2, ensure_ascii=False)
    with open(OUT_TREE_FILE, "w", encoding="utf-8") as f:
        json.dump(roots, f, indent=2, ensure_ascii=False)

    log.info(f"✅ Wrote {OUT_USERS_FILE}    [{len(users)} users]")
    log.info(f"✅ Wrote {OUT_VIEWER_FILE}  [{len(flat_view)} nodes]")
    log.info(f"✅ Wrote {OUT_TREE_FILE}    [{len(roots)} root(s)]")
    if managers_path:
        log.info("ℹ️ Managers sourced from local file in this run.")
    else:
        log.info("ℹ️ Managers sourced from Graph in this run.")

if __name__ == "__main__":
    main()
