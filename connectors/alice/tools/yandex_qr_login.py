#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests>=2.31", "qrcode>=7.4"]
# ///
"""One-time Yandex login by QR code → the x-token the alice connector's cloud
voice (`[connector.push]`) needs.

Scan the QR with the Yandex app (or the camera, then confirm in Yandex ID) on a
phone already signed in to the account that owns the speaker. The token is
written straight into an env file — locally or on a server over ssh — and is
never printed.

    uv run yandex_qr_login.py --env-file contrib/deploy/.env
    uv run yandex_qr_login.py --ssh root@host:/opt/app/contrib/deploy/.env

Flow as in the Home Assistant integration AlexxIT/YandexStation (MIT).
"""

from __future__ import annotations

import argparse
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

import qrcode
import requests

PASSPORT = "https://passport.yandex.ru"
# The Yandex app's public OAuth client, as used by open-source Yandex Station tools.
CLIENT_ID = "c0ebe342af7d48fbbbfcf2d2eedb8f9e"
CLIENT_SECRET = "ad0a908f0aa341a182a37ecd75bc319e"


def login_by_qr(timeout: int) -> tuple[str, str]:
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    page = s.get(f"{PASSPORT}/pwl-yandex", timeout=15)
    page.raise_for_status()
    csrf = re.search(r'__CSRF__ = "([^"]+)', page.text)
    if not csrf:
        sys.exit("Yandex login page changed: no CSRF token found")
    h = {"X-CSRF-Token": csrf[1]}

    r = s.post(f"{PASSPORT}/pwl-yandex/api/passport/auth/password/submit",
               json={"retpath": f"{PASSPORT}/"}, headers=h, timeout=15)
    r.raise_for_status()
    auth = r.json()
    r = s.post(f"{PASSPORT}/pwl-yandex/api/passport/auth/magic/code",
               data={"location_id": "0", "magic_track_id": auth["track_id"], "track_id": ""},
               headers=h, timeout=15)
    r.raise_for_status()
    link = r.json()["link"]

    qr = qrcode.QRCode(border=1)
    qr.add_data(link)
    qr.print_ascii(invert=True)
    print(f"\nScan with the Yandex app on your phone (or open: {link})\nWaiting", end="", flush=True)

    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(2)
        print(".", end="", flush=True)
        st = s.post(f"{PASSPORT}/pwl-yandex/api/passport/auth/magic/code/status",
                    json=auth, headers=h, timeout=15).json()
        if st.get("state") == "otp_auth_finished":
            break
    else:
        sys.exit("\nTimed out waiting for the QR confirmation")
    print(" confirmed")

    s.post(f"{PASSPORT}/pwl-yandex/api/passport/sessions/get_session",
           data={"track_id": st["trackId"]}, headers=h, timeout=15).raise_for_status()
    cookies = "; ".join(f"{c.name}={c.value}" for c in s.cookies if c.domain.endswith("yandex.ru"))
    r = requests.post("https://mobileproxy.passport.yandex.net/1/bundle/oauth/token_by_sessionid",
                      data={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
                      headers={"Ya-Client-Host": "passport.yandex.ru", "Ya-Client-Cookie": cookies},
                      timeout=15)
    x_token = r.json().get("access_token")
    if not x_token:
        sys.exit(f"No x-token in the reply (status {r.status_code})")
    info = requests.get("https://mobileproxy.passport.yandex.net/1/bundle/account/short_info/?avatar_size=islands-300",
                        headers={"Authorization": f"OAuth {x_token}"}, timeout=15).json()
    return x_token, info.get("display_login") or info.get("display_name") or "?"


def upsert(text: str, var: str, value: str) -> str:
    line = f"{var}={value}"
    if re.search(rf"^{re.escape(var)}=.*$", text, flags=re.M):
        return re.sub(rf"^{re.escape(var)}=.*$", lambda _: line, text, flags=re.M)
    return text + ("" if text.endswith("\n") or not text else "\n") + line + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    dest = ap.add_mutually_exclusive_group(required=True)
    dest.add_argument("--env-file", type=Path, help="local env file to write the token into")
    dest.add_argument("--ssh", help="user@host:/path/to/.env on a server")
    ap.add_argument("--var", default="YANDEX_X_TOKEN", help="env var name (default YANDEX_X_TOKEN)")
    ap.add_argument("--timeout", type=int, default=180, help="seconds to wait for the scan")
    a = ap.parse_args()

    x_token, login = login_by_qr(a.timeout)
    if a.env_file:
        old = a.env_file.read_text() if a.env_file.exists() else ""
        a.env_file.write_text(upsert(old, a.var, x_token))
        a.env_file.chmod(0o600)
        where = str(a.env_file)
    else:
        host, _, path = a.ssh.partition(":")
        if not path:
            sys.exit("--ssh needs user@host:/path/to/.env")
        old = subprocess.run(["ssh", host, f"cat {shlex.quote(path)} 2>/dev/null || true"],
                             capture_output=True, text=True, check=True).stdout
        subprocess.run(["ssh", host, f"umask 077 && cat > {shlex.quote(path)}"],
                       input=upsert(old, a.var, x_token), text=True, check=True)
        where = a.ssh
    print(f"Signed in as {login}; {a.var} written to {where} (token not shown).")


if __name__ == "__main__":
    main()
