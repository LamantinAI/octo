# Browser connector

`browser.fetch { url, html?, timeout_secs? }` renders a page using Chrome/CDP.
`status` reports navigation/execution; `extraction_status` distinguishes empty
text from extracted text. Extracted text can still be a menu or a challenge,
so it is not proof that an article was read.

```toml
[connector]
type = "browser"
data_dir = "browser-data"
headless = true
# executable = "/usr/bin/chromium" # externally managed, no download or pruning
# chrome_version = "150.0.1.2"     # optional exact four-part version
chrome_keep_builds = 2
chrome_update_interval_secs = 86400
chrome_download_timeout_secs = 300
```

Without `executable` or an exact version, resolve the real Stable channel from
Chrome for Testing's channel manifest. The resolved executable is reused for the
process lifetime, including browser relaunches. On subsequent process starts,
reuse the selected build until the update interval expires. This interval is not
a background update schedule. Setting it to zero checks at each process start.
A failed online update falls back to the previously selected build; a failed new
browser launch tries the previous working binary once. Exact pins fail explicitly
instead of silently changing the requested version.

A selection is persisted only after Chrome launches successfully. Pruning keeps
the active build and, with the default limit, the previous working build. Existing
historical accumulation is pruned before another download; temporary download
files are cleaned under the same cache lock. Only four-part version directories
and recognized download staging names are managed; profiles and externally
configured executables are not touched. Each managed cache belongs exclusively
to one connector/process, enforced by an OS file lock for its lifetime.

Page timeouts close their tab. Navigation failures probe the existing browser;
a healthy process remains reusable instead of being restarted after every bad
URL. Browser provisioning has its own deadline, separate from page navigation.

The lighter browser/HTTP-first strategy is tracked separately in Octo #16.
