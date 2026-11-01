"""End-to-end checks of the upload flow against a mocked Immich API.

Run from the repo root: python tests/smoke_test.py (needs requirements.txt + httpx).
"""
import io, json, os, sqlite3, sys, tempfile, uuid

repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
tmp = tempfile.mkdtemp()
db = os.path.join(tmp, "data", "state.db")

# Pre-create a DB with the misspelled upload_events columns from upstream 7ae6fda
os.makedirs(os.path.dirname(db))
c = sqlite3.connect(db)
c.execute("CREATE TABLE upload_events (id INTEGER PRIMARY KEY AUTOINCREMENT, token TEXT, uploadedat TEXT DEFAULT CURRENT_TIMESTAMP, ip TEXT, useragent TEXT, fingerprint TEXT, filename TEXT, size INTEGER, checksum TEXT, immichassetid TEXT)")
c.commit(); c.close()

os.environ.update({
    "STATE_DB": db, "IMMICH_BASE_URL": "http://immich.test/api", "IMMICH_API_KEY": "k",
    "PUBLIC_UPLOAD_PAGE_ENABLED": "false", "IMMICH_ALBUM_NAME": "", "SESSION_SECRET": "s", "TZ": "UTC",
})
sys.path.insert(0, repo)
from PIL import Image
from fastapi.testclient import TestClient
import app.app as A

class Resp:
    def __init__(self, status, body):
        self.status_code, self._body = status, body
        self.text = json.dumps(body); self.content = self.text.encode()
    def json(self): return self._body

uploads = []
def fake_post(url, headers=None, data=None, json=None, timeout=None):
    if url.endswith("/auth/login"):
        return Resp(201, {"accessToken": "tok", "userId": "u1", "userEmail": "a@b.c", "name": "A", "isAdmin": True})
    if url.endswith("/assets/bulk-upload-check"):
        return Resp(200, {"results": [{"id": json["assets"][0]["id"], "action": "accept"}]})
    if url.endswith("/assets"):
        body = data.read() if hasattr(data, "read") else b""
        while True:
            more = data.read(65536)
            if not more: break
            body += more
        uploads.append(body)
        # Emulate v3 validation: offset required on datetimes
        import re
        for field in ("fileCreatedAt", "fileModifiedAt"):
            m = re.search(rb'name="' + field.encode() + rb'"\r\n\r\n([^\r]+)', body)
            if not m or not re.search(rb"(Z|[+-]\d\d:\d\d)$", m.group(1)):
                return Resp(400, {"message": "Validation failed", "errors": [{"path": [field], "message": "Invalid ISO datetime"}]})
        return Resp(201, {"id": str(uuid.uuid4()), "status": "created"})
    if url.endswith("/albums"):
        return Resp(201, {"id": "alb1", "albumName": json["albumName"]})
    raise AssertionError(url)
def fake_get(url, headers=None, timeout=None):
    if url.endswith("/albums"): return Resp(200, [])
    return Resp(200, {})
def fake_put(url, headers=None, json=None, timeout=None):
    return Resp(200, [{"id": json["ids"][0], "success": True}])
A.requests.post, A.requests.get, A.requests.put = fake_post, fake_get, fake_put

def jpeg(exif_dt=None, offset=None, color=(1, 2, 3)):
    im = Image.new("RGB", (8, 8), color)
    ex = Image.Exif()
    if exif_dt:
        sub = ex.get_ifd(0x8769)
        sub[0x9003] = exif_dt
        if offset: sub[0x9011] = offset
    buf = io.BytesIO(); im.save(buf, "JPEG", exif=ex.tobytes()); return buf.getvalue()

def field(body, name):
    import re
    return re.search(rb'name="' + name.encode() + rb'"\r\n\r\n([^\r]*)', body).group(1).decode()

ok = lambda cond, msg: print(("PASS " if cond else "FAIL ") + msg) or cond
results = []
client = TestClient(A.app)

# 1. Migration of misspelled columns
cols = {r[1] for r in sqlite3.connect(db).execute("PRAGMA table_info(upload_events)")}
results.append(ok({"uploaded_at", "user_agent", "immich_asset_id"} <= cols, f"upload_events columns repaired: {sorted(cols)}"))

# 2. Anonymous upload refused when public page disabled
r = client.post("/api/upload", data={"item_id": "i1", "session_id": "s1"}, files={"file": ("a.jpg", jpeg(), "image/jpeg")})
results.append(ok(r.status_code == 403 and not uploads, f"anonymous upload refused ({r.status_code})"))
r = client.post("/api/upload/chunk/init", json={"item_id": "i1", "session_id": "s1"})
results.append(ok(r.status_code == 403, f"anonymous chunk init refused ({r.status_code})"))

# 3. Login + invite creation
r = client.post("/api/login", json={"email": "a@b.c", "password": "x"})
results.append(ok(r.status_code == 200, "login"))
r = client.post("/api/invites", json={"albumName": "Party", "maxUses": 5, "expiresDays": 3})
inv = r.json(); token = inv["token"]
results.append(ok(r.status_code == 200 and inv["expiresAt"].endswith("Z"), f"invite created, expiresAt={inv['expiresAt']}"))
client.post("/api/logout")

# 4. Invite upload with EXIF + offset -> UTC with Z, no deviceAssetId
r = client.post("/api/upload", data={"item_id": "i2", "session_id": "s2", "invite_token": token, "last_modified": "1700000000000"},
                files={"file": ("../evil<b>.jpg", jpeg("2024:07:01 18:30:00", "+02:00"), "image/jpeg")})
body = uploads[-1] if uploads else b""
results.append(ok(r.status_code == 200, f"invite upload accepted ({r.status_code} {r.text})"))
results.append(ok(field(body, "fileCreatedAt") == "2024-07-01T16:30:00.000Z", f"EXIF+offset -> {field(body, 'fileCreatedAt')}"))
results.append(ok(b"deviceAssetId" not in body and b"deviceId" not in body, "no deviceAssetId/deviceId sent"))
results.append(ok("added to album 'Party'" in r.json().get("status", ""), f"album add: {r.json().get('status')}"))

# 5. No EXIF -> browser lastModified in UTC
r = client.post("/api/upload", data={"item_id": "i3", "session_id": "s2", "invite_token": token, "last_modified": "1700000000000"},
                files={"file": ("b.png", b"\x89PNG not really", "image/png")})
results.append(ok(field(uploads[-1], "fileCreatedAt") == "2023-11-14T22:13:20.000Z", f"lastModified -> {field(uploads[-1], 'fileCreatedAt')}"))

# 6. EXIF without offset -> interpreted in TZ (UTC here)
r = client.post("/api/upload", data={"item_id": "i4", "session_id": "s2", "invite_token": token},
                files={"file": ("c.jpg", jpeg("2020:01:02 03:04:05", color=(9, 9, 9)), "image/jpeg")})
results.append(ok(field(uploads[-1], "fileCreatedAt") == "2020-01-02T03:04:05.000Z", f"EXIF naive -> {field(uploads[-1], 'fileCreatedAt')}"))

# 7. Duplicate by checksum
r = client.post("/api/upload", data={"item_id": "i5", "session_id": "s2", "invite_token": token},
                files={"file": ("c.jpg", jpeg("2020:01:02 03:04:05", color=(9, 9, 9)), "image/jpeg")})
results.append(ok(r.json().get("status") == "duplicate", "local duplicate detected"))

# 8. Chunked upload with traversal-ish ids
big = jpeg("2021:05:05 05:05:05", color=(50, 60, 70))
sid, iid = "../../etc", "..\\x"
r = client.post("/api/upload/chunk/init", json={"item_id": iid, "session_id": sid, "invite_token": token, "name": "big.jpg"})
results.append(ok(r.status_code == 200, f"chunk init with invite ({r.status_code})"))
half = len(big) // 2
for idx, part in enumerate((big[:half], big[half:])):
    r = client.post("/api/upload/chunk", data={"item_id": iid, "session_id": sid, "chunk_index": str(idx), "total_chunks": "2", "invite_token": token},
                    files={"chunk": ("p", part, "application/octet-stream")})
    assert r.status_code == 200, r.text
chunk_root = os.path.join(tmp, "data", "chunks")
inside = all(os.path.abspath(os.path.join(dp)).startswith(os.path.abspath(chunk_root)) for dp, _, _ in os.walk(chunk_root))
results.append(ok(inside and os.listdir(chunk_root) == ["______etc"], f"chunk dirs stay in chunk root: {os.listdir(chunk_root)}"))
r = client.post("/api/upload/chunk", data={"item_id": iid, "session_id": sid, "chunk_index": "5", "total_chunks": "2", "invite_token": token},
                files={"chunk": ("p", b"x", "application/octet-stream")})
results.append(ok(r.status_code == 400, "out-of-range chunk index rejected"))
r = client.post("/api/upload/chunk/complete", json={"item_id": iid, "session_id": sid, "invite_token": token, "name": "big.jpg", "content_type": "image/jpeg"})
results.append(ok(r.status_code == 200 and field(uploads[-1], "fileCreatedAt") == "2021-05-05T05:05:05.000Z", f"chunk complete ({r.status_code} {r.text})"))

# 9. Upload log + invite list as owner
client.post("/api/login", json={"email": "a@b.c", "password": "x"})
r = client.get(f"/api/invite/{token}/uploads")
items = r.json().get("items", [])
results.append(ok(r.status_code == 200 and len(items) == 4 and items[0]["uploadedAt"].endswith("Z"), f"upload log: {r.status_code}, {len(items)} items"))
lst = client.get("/api/invites").json()["items"][0]
results.append(ok(lst["used"] == 4 and lst["active"], f"invite usage counted: used={lst['used']}"))

# 10. Expired invite (client-sent naive UTC) blocks uploads; resetUsage alone works
r = client.patch(f"/api/invite/{token}", json={"expiresAt": "2000-01-01T00:00:00"})
client.post("/api/logout")
r = client.post("/api/upload", data={"item_id": "i9", "session_id": "s2", "invite_token": token},
                files={"file": ("z.jpg", jpeg(color=(200, 1, 1)), "image/jpeg")})
results.append(ok(r.status_code == 403 and r.json()["error"] == "invite_expired", f"expired invite blocked ({r.json()})"))
client.post("/api/login", json={"email": "a@b.c", "password": "x"})
r = client.patch(f"/api/invite/{token}", json={"resetUsage": True})
results.append(ok(r.status_code == 200, f"resetUsage alone ({r.status_code})"))

# 11. Immich validation errors are surfaced readably
saved = A.to_immich_datetime
A.to_immich_datetime = lambda dt: dt.replace(tzinfo=None).isoformat()
client.patch(f"/api/invite/{token}", json={"expiresDays": 3})
r = client.post("/api/upload", data={"item_id": "i10", "session_id": "s2", "invite_token": token},
                files={"file": ("y.jpg", jpeg(color=(1, 200, 1)), "image/jpeg")})
A.to_immich_datetime = saved
results.append(ok(r.status_code == 400 and "fileCreatedAt" in r.json()["error"], f"v3 error message: {r.json()}"))

# 12. Delete only touches own invites' logs
r = client.post("/api/invites/delete", json={"tokens": [token]})
results.append(ok(r.json().get("deleted") == 1, f"delete: {r.json()}"))

# 13. Pages + ping
results.append(ok(client.get("/login").status_code == 200 and client.post("/api/ping").json()["ok"], "login page + ping"))

print(f"\n{sum(results)}/{len(results)} passed")
sys.exit(0 if all(results) else 1)
