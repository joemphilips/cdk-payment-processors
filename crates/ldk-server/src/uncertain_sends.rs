//! BOLT11 sends whose dispatch is unknown, kept in a durable file.
//!
//! LDK Node dispatches a BOLT11 payment before it stores the Pending record.
//! When that write fails, `Bolt11Send` fails, but the HTLC is in flight and the
//! store still holds the Failed record of an earlier attempt. A mint that
//! trusts that record releases the proofs of a payment that can still settle.
//! The processor marks such a send here and reports `Unknown` for it until the
//! node shows a current result. The marks survive a restart.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use ldk_server_client::ldk_server_grpc::types::{Payment, PaymentStatus};

/// An outgoing HTLC resolves within its CLTV limit of 1,008 blocks, about 7
/// days. Twice that time after a mark, the send of the mark has resolved.
pub(crate) const IN_FLIGHT_BOUND_SECS: u64 = 14 * 24 * 60 * 60;

const FILE_HEADER: &str = "cdk-payment-processor-ldk-server uncertain-sends v1";

/// A BOLT11 send whose call failed after the node possibly dispatched it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UncertainSend {
    /// `latest_update_timestamp` of the stored record before the send, by the
    /// LDK Server clock. `None` when no record existed.
    pub(crate) baseline: Option<u64>,
    /// Local Unix time of the mark.
    pub(crate) marked_at: u64,
}

/// Whether a stored BOLT11 send record can be reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SendVerdict {
    /// The record is current.
    Record,
    /// The record can be stale while a payment is in flight. Report `Unknown`.
    Unknown,
}

/// The uncertain sends, by lowercase payment hash.
#[derive(Debug)]
pub(crate) struct UncertainSends {
    /// The state file. `None` keeps the marks in memory only.
    path: Option<PathBuf>,
    marks: HashMap<String, UncertainSend>,
}

impl UncertainSends {
    /// Marks that a restart loses. For tests and library use only.
    pub(crate) fn in_memory() -> Self {
        Self {
            path: None,
            marks: HashMap::new(),
        }
    }

    /// Opens the state file and writes it back, so that a start fails when the
    /// file cannot be written.
    ///
    /// A missing file holds no marks. A file that cannot be read or parsed is
    /// an error: without its marks, the processor could report a stale Failed
    /// record.
    pub(crate) fn open(path: PathBuf) -> Result<Self, String> {
        let marks = match std::fs::read_to_string(&path) {
            Ok(text) => parse(&text).map_err(|error| {
                format!("cannot parse the state file {}: {error}", path.display())
            })?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => {
                return Err(format!(
                    "cannot read the state file {}: {error}",
                    path.display()
                ))
            }
        };
        let sends = Self {
            path: Some(path),
            marks,
        };
        sends.persist()?;
        Ok(sends)
    }

    /// Marks a send and writes the state file before the caller reports the
    /// failed send.
    pub(crate) fn mark(
        &mut self,
        hash: &str,
        baseline: Option<u64>,
        now: u64,
    ) -> Result<(), String> {
        self.marks
            .retain(|_, mark| now.saturating_sub(mark.marked_at) < IN_FLIGHT_BOUND_SECS);
        self.marks.insert(
            hash.to_ascii_lowercase(),
            UncertainSend {
                baseline,
                marked_at: now,
            },
        );
        self.persist()
    }

    /// Decides whether `record`, the stored send for `hash`, can be reported.
    ///
    /// A mark ends with a Succeeded record, with a Failed record newer than the
    /// record before the send, or `IN_FLIGHT_BOUND_SECS` after the mark. Until
    /// then a Failed or missing record is `Unknown`.
    pub(crate) fn verdict(
        &mut self,
        hash: &str,
        record: Option<&Payment>,
        now: u64,
    ) -> SendVerdict {
        let hash = hash.to_ascii_lowercase();
        let Some(mark) = self.marks.get(&hash).copied() else {
            return SendVerdict::Record;
        };
        let status = record.and_then(|payment| PaymentStatus::from_i32(payment.status));
        let current = now.saturating_sub(mark.marked_at) >= IN_FLIGHT_BOUND_SECS
            || match (record, status) {
                (Some(_), Some(PaymentStatus::Succeeded)) => true,
                (Some(payment), Some(PaymentStatus::Failed)) => mark
                    .baseline
                    .is_none_or(|baseline| payment.latest_update_timestamp > baseline),
                _ => false,
            };
        if current {
            self.marks.remove(&hash);
            if let Err(error) = self.persist() {
                // The file keeps the mark. After a restart the check reaches
                // the same result again, so this fails closed.
                tracing::error!("Could not remove an uncertain send mark: {error}");
            }
            return SendVerdict::Record;
        }
        match status {
            Some(PaymentStatus::Pending) => SendVerdict::Record,
            _ => SendVerdict::Unknown,
        }
    }

    fn persist(&self) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        write_atomically(path, &format_marks(&self.marks))
            .map_err(|error| format!("cannot write the state file {}: {error}", path.display()))
    }
}

fn format_marks(marks: &HashMap<String, UncertainSend>) -> String {
    let mut lines: Vec<String> = marks
        .iter()
        .map(|(hash, mark)| {
            let baseline = mark
                .baseline
                .map_or_else(|| "-".to_string(), |baseline| baseline.to_string());
            format!("{hash} {baseline} {}", mark.marked_at)
        })
        .collect();
    lines.sort();
    let mut text = String::from(FILE_HEADER);
    text.push('\n');
    for line in lines {
        text.push_str(&line);
        text.push('\n');
    }
    text
}

fn parse(text: &str) -> Result<HashMap<String, UncertainSend>, String> {
    let mut lines = text.lines();
    if lines.next() != Some(FILE_HEADER) {
        return Err(format!("the first line is not `{FILE_HEADER}`"));
    }
    let mut marks = HashMap::new();
    for (index, line) in lines.enumerate() {
        let number = index + 2;
        let fields: Vec<&str> = line.split(' ').collect();
        let [hash, baseline, marked_at] = fields[..] else {
            return Err(format!("line {number} does not have three fields"));
        };
        if hash.len() != 64
            || !hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(format!("line {number} has no lowercase payment hash"));
        }
        let baseline = match baseline {
            "-" => None,
            value => Some(
                value
                    .parse()
                    .map_err(|_| format!("line {number} has an invalid baseline"))?,
            ),
        };
        let marked_at = marked_at
            .parse()
            .map_err(|_| format!("line {number} has an invalid mark time"))?;
        if marks
            .insert(
                hash.to_string(),
                UncertainSend {
                    baseline,
                    marked_at,
                },
            )
            .is_some()
        {
            return Err(format!("line {number} repeats a payment hash"));
        }
    }
    Ok(marks)
}

/// Writes a temporary file, flushes it, renames it over `path`, and flushes
/// the directory, so that a crash leaves the old or the new file.
fn write_atomically(path: &Path, contents: &str) -> std::io::Result<()> {
    let directory = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);

    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(&temporary)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temporary, path)?;
    File::open(directory)?.sync_all()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use ldk_server_client::ldk_server_grpc::types::PaymentDirection;

    use super::*;

    static NEXT_DIRECTORY: AtomicU32 = AtomicU32::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "uncertain-sends-{}-{}",
                std::process::id(),
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn file(&self) -> PathBuf {
            self.0.join("state")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn hash(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    fn record(status: PaymentStatus, latest_update_timestamp: u64) -> Payment {
        Payment {
            payment_id: hash(1),
            kind: None,
            amount_msat: Some(1_000),
            fee_paid_msat: None,
            direction: PaymentDirection::Outbound as i32,
            status: status as i32,
            latest_update_timestamp,
        }
    }

    #[test]
    fn a_missing_file_starts_empty_and_is_written() {
        let directory = TestDirectory::new();
        let sends = UncertainSends::open(directory.file()).unwrap();
        assert!(sends.marks.is_empty());
        assert_eq!(
            std::fs::read_to_string(directory.file()).unwrap(),
            format!("{FILE_HEADER}\n")
        );
    }

    #[test]
    fn a_mark_is_in_the_file_before_mark_returns() {
        let directory = TestDirectory::new();
        let mut sends = UncertainSends::open(directory.file()).unwrap();
        sends
            .mark(&hash(1).to_uppercase(), Some(100), 5_000)
            .unwrap();
        sends.mark(&hash(2), None, 5_001).unwrap();

        assert_eq!(
            std::fs::read_to_string(directory.file()).unwrap(),
            format!("{FILE_HEADER}\n{} 100 5000\n{} - 5001\n", hash(1), hash(2))
        );
        let reopened = UncertainSends::open(directory.file()).unwrap();
        assert_eq!(reopened.marks, sends.marks);
        let mut temporary = directory.file().into_os_string();
        temporary.push(".tmp");
        assert!(!PathBuf::from(temporary).exists());
    }

    #[test]
    fn an_unreadable_file_refuses_to_open() {
        let directory = TestDirectory::new();
        for text in [
            "garbage\n".to_string(),
            format!("{FILE_HEADER}\n{} 100\n", hash(1)),
            format!("{FILE_HEADER}\nABCD 100 5000\n"),
            format!("{FILE_HEADER}\n{} x 5000\n", hash(1)),
            format!("{FILE_HEADER}\n{0} 1 2\n{0} 3 4\n", hash(1)),
        ] {
            std::fs::write(directory.file(), &text).unwrap();
            let error = UncertainSends::open(directory.file()).unwrap_err();
            assert!(error.contains("cannot parse"), "{error}");
            // The file stays for the operator.
            assert_eq!(std::fs::read_to_string(directory.file()).unwrap(), text);
        }
    }

    #[test]
    fn a_mark_ends_with_a_current_record_and_the_file_follows() {
        let directory = TestDirectory::new();
        let mut sends = UncertainSends::open(directory.file()).unwrap();
        sends.mark(&hash(1), Some(100), 5_000).unwrap();

        let stale = record(PaymentStatus::Failed, 100);
        assert_eq!(
            sends.verdict(&hash(1), Some(&stale), 5_010),
            SendVerdict::Unknown
        );
        assert_eq!(sends.verdict(&hash(1), None, 5_010), SendVerdict::Unknown);
        let pending = record(PaymentStatus::Pending, 100);
        assert_eq!(
            sends.verdict(&hash(1), Some(&pending), 5_010),
            SendVerdict::Record
        );

        let newer = record(PaymentStatus::Failed, 101);
        assert_eq!(
            sends.verdict(&hash(1), Some(&newer), 5_020),
            SendVerdict::Record
        );
        assert!(UncertainSends::open(directory.file())
            .unwrap()
            .marks
            .is_empty());
    }

    #[test]
    fn a_reopened_mark_ends_fourteen_days_after_the_mark() {
        let directory = TestDirectory::new();
        let mut sends = UncertainSends::open(directory.file()).unwrap();
        sends.mark(&hash(1), Some(100), 5_000).unwrap();
        let mut reopened = UncertainSends::open(directory.file()).unwrap();

        let stale = record(PaymentStatus::Failed, 100);
        assert_eq!(
            reopened.verdict(&hash(1), Some(&stale), 5_000 + IN_FLIGHT_BOUND_SECS - 1),
            SendVerdict::Unknown
        );
        assert_eq!(
            reopened.verdict(&hash(1), Some(&stale), 5_000 + IN_FLIGHT_BOUND_SECS),
            SendVerdict::Record
        );
    }

    #[test]
    fn a_send_without_a_mark_reports_its_record() {
        let mut sends = UncertainSends::in_memory();
        let failed = record(PaymentStatus::Failed, 100);
        assert_eq!(
            sends.verdict(&hash(1), Some(&failed), 0),
            SendVerdict::Record
        );
    }
}
