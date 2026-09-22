//! Cutting a long (or video) recording into speech chunks the dictation endpoint takes
//! whole. The endpoint accepts ~23 minutes but silently truncates a long transcript
//! mid-sentence, so a recording is split on its pauses into chunks near [`TARGET_SECS`],
//! each re-encoded as audio-only Opus. Needs `ffmpeg` + `ffprobe` on the host.

use std::{
    fs::{create_dir_all, remove_dir_all},
    path::{Path, PathBuf},
    process,
    time::Duration,
};

use chrono::Utc;
use futures::{stream, StreamExt};
use octo_openai_auth::Subscription;
use serde_json::{json, Value};
use tokio::process::Command;

use crate::{looks_truncated, transcribe, TranscribeError};

/// Default chunk length to aim for: well under the endpoint's silent-truncation point.
pub const TARGET_SECS: f64 = 300.0;
/// A proven-safe upper bound for one chunk (23 min worked; 24 min answers 500).
pub const HARD_LIMIT_SECS: f64 = 1380.0;
/// `silencedetect` settings: what counts as a pause worth cutting at.
const SILENCE_NOISE: &str = "-30dB";
const SILENCE_MIN_SECS: f64 = 0.6;
/// Default number of chunks uploaded at once.
pub const PARALLEL_UPLOADS: usize = 4;
/// Attempts per chunk for a transient failure (network, 5xx).
const CHUNK_ATTEMPTS: u64 = 3;
/// Extensions that are (or usually are) video: always split, so only the audio goes up.
const VIDEO_EXTS: [&str; 7] = ["mp4", "mov", "mkv", "avi", "m4v", "wmv", "flv"];

/// One planned chunk of the source, in seconds.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Span {
    pub start: f64,
    pub end: f64,
}

/// The recording's length in seconds, via `ffprobe`.
pub async fn duration(path: &Path) -> Result<f64, String> {
    let out = run(Command::new("ffprobe")
        .args(["-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0"])
        .arg(path))
    .await?;
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .map_err(|e| format!("ffprobe gave no duration: {e}"))
}

/// Midpoints of the recording's pauses, in seconds — the candidate cut points.
pub async fn pauses(path: &Path) -> Result<Vec<f64>, String> {
    let filter = format!("silencedetect=noise={SILENCE_NOISE}:d={SILENCE_MIN_SECS}");
    let out = run(Command::new("ffmpeg")
        .args(["-hide_banner", "-nostats", "-v", "info", "-i"])
        .arg(path)
        .args(["-af", &filter, "-f", "null", "-"]))
    .await?;
    Ok(silence_midpoints(&String::from_utf8_lossy(&out.stderr)))
}

/// Pair `silence_start` / `silence_end` lines from ffmpeg's log into midpoints.
fn silence_midpoints(log: &str) -> Vec<f64> {
    let values = |key: &str| -> Vec<f64> {
        log.lines()
            .filter_map(|l| l.split(key).nth(1))
            .filter_map(|rest| rest.split_whitespace().next()?.parse().ok())
            .collect()
    };
    let mut mids: Vec<f64> = values("silence_start: ")
        .into_iter()
        .zip(values("silence_end: "))
        .map(|(s, e)| (s + e) / 2.0)
        .collect();
    mids.sort_by(f64::total_cmp);
    mids
}

/// Greedy plan: from each position, cut at the last pause before `target` (or, failing
/// that, the first one within 1.4x of it); with no pause, cut blind at `target`. Every
/// chunk stays near `target`, the last one included, because an oversized chunk loses
/// speech without any error.
pub fn plan(total: f64, cuts: &[f64], target: f64) -> Vec<Span> {
    let mut bounds = vec![0.0];
    let mut pos = 0.0;
    while total - pos > target * 1.4 {
        let near = cuts.iter().copied().rfind(|&c| pos + target * 0.5 < c && c <= pos + target);
        let late = || {
            let limit = pos + (target * 1.4).min(HARD_LIMIT_SECS);
            cuts.iter().copied().rfind(|&c| pos + target < c && c <= limit)
        };
        let mut next = near.or_else(late).unwrap_or((pos + target).min(total));
        if next <= pos {
            next = (pos + target).min(total);
        }
        bounds.push(next);
        pos = next;
    }
    bounds.push(total);
    bounds
        .windows(2)
        .map(|w| Span { start: w[0], end: w[1] })
        .filter(|s| s.end - s.start > 0.5)
        .collect()
}

/// Cut one span as audio-only mono Opus in WebM. `-vn` matters: for a video source ffmpeg
/// would otherwise re-encode the picture too — minutes of CPU per chunk for pixels the
/// endpoint never looks at.
pub async fn cut(source: &Path, span: Span, dir: &Path, index: usize) -> Result<PathBuf, String> {
    let out = dir.join(format!("chunk_{index:03}.webm"));
    run(Command::new("ffmpeg")
        .args(["-y", "-v", "error", "-ss", &format!("{:.3}", span.start), "-t"])
        .arg(format!("{:.3}", span.end - span.start))
        .arg("-i")
        .arg(source)
        .args(["-vn", "-ac", "1", "-c:a", "libopus", "-b:a", "24k"])
        .arg(&out))
    .await?;
    Ok(out)
}

/// `hh:mm:ss` for a chunk's timecode.
pub fn hms(secs: f64) -> String {
    let s = secs as u64;
    format!("{:02}:{:02}:{:02}", s / 3600, s % 3600 / 60, s % 60)
}

/// Run a tool to completion; a missing binary reads as "install ffmpeg".
async fn run(cmd: &mut Command) -> Result<std::process::Output, String> {
    let program = cmd.as_std().get_program().to_string_lossy().into_owned();
    let out = cmd.kill_on_drop(true).output().await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            format!("{program} is not installed on this host (needed to split long recordings)")
        } else {
            format!("{program} failed to start: {e}")
        }
    })?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        return Err(format!("{program} failed ({}): {}", out.status, err.trim().chars().take(200).collect::<String>()));
    }
    Ok(out)
}

/// Upload the `which` chunks, `parallel` at a time.
pub(crate) async fn upload_chunks(
    chunks: &[PathBuf],
    which: &[usize],
    language: Option<&str>,
    sub: &Subscription,
    parallel: usize,
) -> Vec<(usize, Result<String, TranscribeError>)> {
    stream::iter(which.iter().copied())
        .map(|i| async move { (i, upload_chunk(&chunks[i], language, sub).await) })
        .buffer_unordered(parallel.max(1))
        .collect()
        .await
}

/// One chunk up, retrying a transient failure with a short backoff; a refused token is
/// returned at once for the caller to refresh.
async fn upload_chunk(path: &Path, language: Option<&str>, sub: &Subscription) -> Result<String, TranscribeError> {
    let audio = std::fs::read(path).map_err(|e| TranscribeError::Failed(format!("read chunk: {e}")))?;
    let mut last = String::new();
    for attempt in 1..=CHUNK_ATTEMPTS {
        match transcribe(&audio, "chunk.webm", "audio/webm", language, sub).await {
            Err(TranscribeError::Failed(e)) => last = e,
            other => return other,
        }
        if attempt < CHUNK_ATTEMPTS {
            tokio::time::sleep(Duration::from_secs(2 * attempt)).await;
        }
    }
    Err(TranscribeError::Failed(last))
}

/// Join the chunk texts in order under `[hh:mm:ss]` timecodes. A failed chunk is marked in
/// place; only if every chunk failed is the whole call an error.
pub(crate) fn merge(
    spans: &[Span],
    texts: Vec<Option<Result<String, TranscribeError>>>,
    total: f64,
) -> Result<Value, String> {
    let (mut lines, mut failed, mut truncated) = (Vec::new(), 0, Vec::new());
    let mut first_error = None;
    for (i, (span, result)) in spans.iter().zip(texts).enumerate() {
        let at = hms(span.start);
        match result.unwrap_or_else(|| Err(TranscribeError::Failed("not uploaded".into()))) {
            Ok(text) if text.trim().is_empty() => {}
            Ok(text) => {
                if looks_truncated(&text) {
                    truncated.push(i);
                }
                lines.push(format!("[{at}] {}", text.trim()));
            }
            Err(e) => {
                failed += 1;
                lines.push(format!("[{at}] [[chunk {i} failed: {e}]]"));
                first_error.get_or_insert(e.to_string());
            }
        }
    }
    if failed == spans.len() {
        return Err(format!("every chunk failed: {}", first_error.unwrap_or_default()));
    }
    Ok(json!({
        "text": lines.join("\n\n"),
        "chunks": spans.len(),
        "duration_secs": total.round(),
        "failed": failed,
        "truncated_chunks": truncated,
    }))
}

pub(crate) fn is_video(filename: &str) -> bool {
    let ext = Path::new(filename).extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase();
    VIDEO_EXTS.contains(&ext.as_str())
}

/// A private scratch directory for the chunks, removed when dropped (success or not).
pub(crate) struct Scratch(PathBuf);

impl Scratch {
    pub(crate) fn new() -> Result<Self, String> {
        let dir = std::env::temp_dir().join(format!(
            "octo-transcribe-{}-{}",
            process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        create_dir_all(&dir).map_err(|e| format!("scratch dir: {e}"))?;
        Ok(Self(dir))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::{hms, plan, silence_midpoints, Span};

    #[test]
    fn a_short_recording_is_one_chunk() {
        assert_eq!(plan(200.0, &[50.0, 120.0], 300.0), vec![Span { start: 0.0, end: 200.0 }]);
    }

    #[test]
    fn cuts_land_on_the_last_pause_before_the_target() {
        let spans = plan(1000.0, &[100.0, 280.0, 290.0, 560.0, 800.0], 300.0);
        assert_eq!(spans[0], Span { start: 0.0, end: 290.0 });
        assert_eq!(spans[1].start, 290.0);
        assert_eq!(spans.last().unwrap().end, 1000.0);
        assert!(spans.iter().all(|s| s.end - s.start <= 300.0 * 1.4));
    }

    #[test]
    fn with_no_pauses_it_cuts_blind_at_the_target() {
        let spans = plan(900.0, &[], 300.0);
        assert_eq!(spans.iter().map(|s| s.end).collect::<Vec<_>>(), vec![300.0, 600.0, 900.0]);
    }

    #[test]
    fn ffmpeg_silence_lines_pair_into_midpoints() {
        let log = "[silencedetect @ 0x1] silence_start: 10.5\n\
                   [silencedetect @ 0x1] silence_end: 11.5 | silence_duration: 1\n\
                   [silencedetect @ 0x1] silence_start: 3\n\
                   [silencedetect @ 0x1] silence_end: 4 | silence_duration: 1\n";
        assert_eq!(silence_midpoints(log), vec![3.5, 11.0]);
    }

    #[test]
    fn timecodes_are_hh_mm_ss() {
        assert_eq!(hms(3725.9), "01:02:05");
    }
}
