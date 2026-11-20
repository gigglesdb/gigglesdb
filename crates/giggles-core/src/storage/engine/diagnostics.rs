//! Best-effort diagnostics must not wait for a caller-held stderr lock.
use std::fmt::Display;

pub(super) fn error_text(error: &impl Display) -> String {
    const LIMIT: usize = 256;
    const MARKER: &str = "\u{2026}(truncated)";
    let mut text = error.to_string();
    if text.len() > LIMIT {
        let mut end = LIMIT - MARKER.len();
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str(MARKER);
    }
    text
}

pub(super) fn error(cause: String) {
    // The writer may wait on stderr; the sequencer and catalog caller never do.
    let _ = std::thread::Builder::new()
        .name("engine-diagnostic".into())
        .spawn(move || {
            use std::io::Write;
            let _ = writeln!(std::io::stderr(), "level=ERROR {cause}");
        });
}

#[cfg(test)]
mod tests {
    #[test]
    fn diagnostic_caller_does_not_wait_for_stderr_lock() {
        let stderr = std::io::stderr();
        let guard = stderr.lock();
        let (sent, received) = std::sync::mpsc::channel();
        let caller = std::thread::spawn(move || {
            super::error("diagnostic lock regression".into());
            let _ = sent.send(());
        });
        let result = received.recv_timeout(std::time::Duration::from_secs(2));
        drop(guard);
        caller.join().unwrap();
        result.unwrap();
    }

    #[test]
    fn bounded_error_preserves_utf8_and_short_causes() {
        assert_eq!(super::error_text(&"short"), "short");
        let text = super::error_text(&"\u{e9}".repeat(200));
        assert!(text.len() <= 256);
        assert!(text.ends_with("\u{2026}(truncated)"));
    }
}
