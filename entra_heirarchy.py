#!/usr/bin/env python3
"""
entra_heirarchy.py  (adaptive throttle version)

Outputs:
  - users_flat.json
  - org_tree.json
  - manager_checkpoint.json

Env:
  AZ_TENANT_ID, AZ_CLIENT_ID, AZ_CLIENT_SECRET
  USER_FILTER (optional), e.g. accountEnabled eq true
"""

import os, json, time, base64, random, logging
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import requests
from msal import ConfidentialClientApplication

# ----------------------------
# Config (adaptive defaults)
# ----------------------------
TENANT_ID      = "42fd9015-de4d-4223-a368-baeacab48927"
CLIENT_ID      = "2bc1c9b9-d0ad-4ff1-ac90-f5f54f942efb"
CLIENT_SECRET  = "o5B8Q~XnkYM_BFpZ3anY~5lzrSiVqqGW3P_60br1"
AUTHORITY     = f"https://login.microsoftonline.com/{TENANT_ID}"
GRAPH_ROOT    = "https://graph.microsoft.com/v1.0"

# Start reasonably low; we’ll adapt down if throttled
PAGE_SIZE            = int(os.getenv("PAGE_SIZE", "100"))
MIN_PAGE_SIZE        = 25
BATCH_LIMIT          = 10
REQUEST_TIMEOUT_SEC  = 30
MAX_ATTEMPTS         = 8

# Base pacing; we’ll increase if we detect repeated 503/429s
INTER_PAGE_SLEEP     = 0.35
INTER_BATCH_SLEEP    = 0.40
MAX_CONSEC_SERVICE_ERRORS = 6   # after this, take a long nap and reduce page size

OUT_USERS_FILE   = "users_flat.json"
OUT_TREE_FILE    = "org_tree.json"
CHECKPOINT_FILE  = "manager_checkpoint.json"
USER_FILTER      = os.getenv("USER_FILTER", "").strip()

REQUIRED_ROLES = {"Directory.Read.All", "User.Read.All"}

USER_SELECT_FIELDS_OLD = [
    "id","userPrincipalName","displayName","givenName","surname","mail","mailNickname",
    "jobTitle","department","companyName","employeeId","employeeType","employeeHireDate",
    "employeeOrgData","businessPhones","mobilePhone","officeLocation","usageLocation",
    "city","state","country","countryLetterCode","accountEnabled","createdDateTime",
    "onPremisesImmutableId","onPremisesDistinguishedName","onPremisesSamAccountName",
    "onPremisesDomainName","onPremisesSecurityIdentifier","onPremisesSyncEnabled",
    "onPremisesExtensionAttributes","externalUserState","preferredLanguage",
    "streetAddress","postalCode"
]

USER_SELECT_FIELDS = [
    "id",
    "displayName",
    "userPrincipalName",   # fallback if displayName missing
    "mailNickname",        # fallback if displayName missing
    "jobTitle",
    "department",
    "managerId",           # if available from Graph
    "reports"              # if available from Graph
]


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("graph-org")

# ----------------------------
# Auth + role check
# ----------------------------
def acquire_token() -> str:
    if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET):
        raise RuntimeError("AZ_TENANT_ID / AZ_CLIENT_ID / AZ_CLIENT_SECRET must be set.")
    app = ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
    res = app.acquire_token_for_client(["https://graph.microsoft.com/.default"])
    if "access_token" not in res:
        raise RuntimeError(f"Auth failed: {res}")
    return res["access_token"]

def _decode_jwt_noverify(tok: str) -> dict:
    parts = tok.split(".")
    if len(parts) != 3:
        return {}
    def pad(s): return s + "=" * (-len(s) % 4)
    return json.loads(base64.urlsafe_b64decode(pad(parts[1])).decode("utf-8"))

def assert_roles(token: str):
    roles = set(_decode_jwt_noverify(token).get("roles", []))
    missing = REQUIRED_ROLES - roles
    if missing:
        log.warning(f"Access token missing required app roles: {missing}. "
                    f"Present: {sorted(roles)}. Admin consent may be missing.")

# ----------------------------
# HTTP helpers with adaptive backoff
# ----------------------------
class AdaptiveLimiter:
    def __init__(self):
        self.consec_service_errors = 0  # 429/503/504 streak
        self.page_sleep = INTER_PAGE_SLEEP
        self.page_size = PAGE_SIZE

    def note_success(self):
        self.consec_service_errors = max(0, self.consec_service_errors - 1)

    def note_service_error(self):
        self.consec_service_errors += 1
        # If we’re getting hammered repeatedly, slow down and shrink page size
        if self.consec_service_errors in (3, 4):
            self.page_sleep = min(self.page_sleep + 0.25, 2.0)
        if self.consec_service_errors >= MAX_CONSEC_SERVICE_ERRORS:
            # Long nap + reduce page size
            nap = 30 + 10 * (self.consec_service_errors - MAX_CONSEC_SERVICE_ERRORS)
            log.warning(f"Heavy throttling detected; sleeping {nap}s and reducing $top.")
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
            log.warning(f"Throttled {resp.status_code} on {url.split('?')[0]} "
                        f"(attempt {attempt}/{MAX_ATTEMPTS}); retrying in {wait:.2f}s …")
            time.sleep(wait)
            continue

        # Non-retryable
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
# Cheap probe: is Graph healthy?
# ----------------------------
def probe_org(token: str):
    with requests.Session() as s:
        url = f"{GRAPH_ROOT}/organization?$select=id,displayName&$top=1"
        try:
            r = graph_get(s, url, token)
            _ = r.json()
            log.info("Probe /organization ok.")
        except Exception as e:
            log.warning(f"/organization probe had issues: {e}")

# ----------------------------
# Data fetchers
# ----------------------------
def fetch_all_users(token: str) -> List[Dict[str, Any]]:
    fields = ",".join(USER_SELECT_FIELDS)
    base = f"{GRAPH_ROOT}/users?$select={fields}"
    # Use *current* adaptive page size
    base += f"&$top={limiter.page_size}"
    if USER_FILTER:
        base += f"&$filter={USER_FILTER}"

    url = base
    users: List[Dict[str, Any]] = []
    with requests.Session() as s:
        while url:
            r = graph_get(s, url, token)
            data = r.json()
            users.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
            log.info(f"Fetched users: {len(users)} so far (page_sleep={limiter.page_sleep:.2f}s, top={limiter.page_size})")
            if url:
                time.sleep(limiter.page_sleep)
    return users

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
            data = resp.json()
            responses = data.get("responses", [])

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
                        log.warning(f"$batch item for {uid} got {status}; retrying in {wait:.2f}s "
                                    f"(left {attempts_left-1})")
                        time.sleep(wait)
                        queue.append((uid, attempts_left - 1))
                    else:
                        log.error(f"$batch item for {uid} exhausted retries ({status}); -> None")
                        manager_map[uid] = None
                else:
                    short = str(body)[:300].replace("\n", " ")
                    log.error(f"$batch item for {uid} failed status={status}, body={short}")
                    manager_map[uid] = None

            time.sleep(INTER_BATCH_SLEEP)

    log.info(f"Resolved managers: {len(manager_map)}/{len(user_ids)}")
    return manager_map

# ----------------------------
# Checkpoint
# ----------------------------
def load_checkpoint() -> Dict[str, Optional[str]]:
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_checkpoint(mm: Dict[str, Optional[str]]):
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(mm, f)
    os.replace(tmp, CHECKPOINT_FILE)

# ----------------------------
# Build tree
# ----------------------------
def build_hierarchy(users: List[Dict[str, Any]], manager_map: Dict[str, Optional[str]]) -> List[Dict[str, Any]]:
    nodes: Dict[str, Dict[str, Any]] = {}
    for u in users:
        uid = u["id"]
        u["managerId"] = manager_map.get(uid)
        u["reports"] = []
        nodes[uid] = u

    roots: List[Dict[str, Any]] = []
    for u in users:
        m = u.get("managerId")
        if m and m in nodes:
            nodes[m]["reports"].append(u)
        else:
            roots.append(u)

    def sort_recursive(n: Dict[str, Any]):
        n["reports"].sort(key=lambda x: (x.get("displayName") or "").lower())
        for c in n["reports"]:
            sort_recursive(c)

    for r in roots:
        sort_recursive(r)
    return roots

# ----------------------------
# Main
# ----------------------------
def main():
    token = acquire_token()
    log.info("Authenticated to Microsoft Graph.")
    assert_roles(token)

    # Probe first (cheap)
    probe_org(token)

    # Users
    users = fetch_all_users(token)
    if not users:
        log.warning("No users returned.")
        with open(OUT_USERS_FILE, "w", encoding="utf-8") as f: json.dump([], f, indent=2, ensure_ascii=False)
        with open(OUT_TREE_FILE, "w", encoding="utf-8") as f: json.dump([], f, indent=2, ensure_ascii=False)
        return

    # Managers with resume
    existing = load_checkpoint()
    pending = [u["id"] for u in users if u["id"] not in existing]
    if pending:
        log.info(f"Resolving managers for {len(pending)} users (resume has {len(existing)}) …")
        partial = batch_get_managers(token, pending)
        existing.update(partial)
        save_checkpoint(existing)
    else:
        log.info("All managers already resolved (from checkpoint).")

    tree = build_hierarchy(users, existing)

    with open(OUT_USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2, ensure_ascii=False)
    with open(OUT_TREE_FILE, "w", encoding="utf-8") as f:
        json.dump(tree, f, indent=2, ensure_ascii=False)

    log.info(f"✅ Wrote {OUT_USERS_FILE} (flat) [{len(users)} users]")
    log.info(f"✅ Wrote {OUT_TREE_FILE} (tree)  [{len(tree)} root(s)]")
    log.info(f"ℹ️  Checkpoint at {CHECKPOINT_FILE} (safe to delete after success)")

if __name__ == "__main__":
    main()
