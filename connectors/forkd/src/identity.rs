/// Whether this process can setuid/setgid children: root, or a non-root service
/// holding CAP_SETUID + CAP_SETGID in its effective set (systemd
/// `AmbientCapabilities=CAP_SETUID CAP_SETGID` under a hardened `User=` unit).
pub(super) fn can_drop_uid() -> bool {
    if unsafe { libc::geteuid() } == 0 {
        return true;
    }
    // CapEff is a hex bitmask in /proc/self/status; CAP_SETGID = bit 6, CAP_SETUID = bit 7.
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return false;
    };
    const NEEDED: u64 = (1 << 6) | (1 << 7);
    status
        .lines()
        .find_map(|l| l.strip_prefix("CapEff:"))
        .and_then(|hex| u64::from_str_radix(hex.trim(), 16).ok())
        .is_some_and(|caps| caps & NEEDED == NEEDED)
}

/// Resolve a group name to its gid via `getgrnam` (called once, at config time).
pub(super) fn resolve_group(name: &str) -> Option<u32> {
    let cname = std::ffi::CString::new(name).ok()?;
    // SAFETY: as with getpwnam — read the static buffer immediately, copy out.
    unsafe {
        let gr = libc::getgrnam(cname.as_ptr());
        if gr.is_null() {
            None
        } else {
            Some((*gr).gr_gid)
        }
    }
}

/// Resolve a username to `(uid, gid)` via `getpwnam` (called once, at config time).
pub(super) fn resolve_user(name: &str) -> Option<(u32, u32)> {
    let cname = std::ffi::CString::new(name).ok()?;
    // SAFETY: getpwnam returns a pointer into a static buffer; we read it immediately
    // and copy the two fields out before any other libc call can clobber it.
    unsafe {
        let pw = libc::getpwnam(cname.as_ptr());
        if pw.is_null() {
            None
        } else {
            Some(((*pw).pw_uid, (*pw).pw_gid))
        }
    }
}
