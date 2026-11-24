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

const FILE_HEADER: &str = "cdk-payment-processor-ldk-server uncertain-sends v2 created";

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
    /// Local Unix time when the state file was created.
    created_at: u64,
    marks: HashMap<String, UncertainSend>,
}

/// Creates the state file of a new processor. Refuses a file that exists.
pub fn init_state_file(path: &Path, now: u64) -> Result<(), String> {
    if path.exists() {
        return Err(format!(
            "the state file {} exists; a new file would drop its marks",
            path.display()
        ));
    }
    let sends = UncertainSends {
        path: Some(path.to_path_buf()),
        created_at: now,
        marks: HashMap::new(),
    };
    sends.persist()
}

/// Removes one mark from the state file, after an operator resolved its send.
/// Returns false when the hash has no mark.
pub fn clear_mark(path: &Path, hash: &str) -> Result<bool, String> {
    let mut sends = UncertainSends::open(path.to_path_buf())?;
    let had_mark = sends.marks.contains_key(&hash.to_ascii_lowercase());
    sends.clear(hash)?;
    Ok(had_mark)
}

impl UncertainSends {
    /// Marks that a restart loses. For tests and library use only. The marks
    /// count as new, so a missing record is never taken as a send that did not
    /// happen.
    pub(crate) fn in_memory() -> Self {
        Self::in_memory_since(cdk_common::util::unix_time())
    }

    pub(crate) fn in_memory_since(created_at: u64) -> Self {
        Self {
            path: None,
            created_at,
            marks: HashMap::new(),
        }
    }

    /// Opens the state file and writes it back, so that a start fails when the
    /// file cannot be written.
    ///
    /// A missing file is an error: a lost file would drop every mark. Only
    /// `init_state_file` creates it. A file that cannot be read or parsed is
    /// also an error.
    pub(crate) fn open(path: PathBuf) -> Result<Self, String> {
        let (created_at, marks) = match std::fs::read_to_string(&path) {
            Ok(text) => parse(&text).map_err(|error| {
                format!("cannot parse the state file {}: {error}", path.display())
            })?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(format!(
                    "the state file {} is missing; restore it, or create the file of a new \
                     processor with --init-state",
                    path.display()
                ))
            }
            Err(error) => {
                return Err(format!(
                    "cannot read the state file {}: {error}",
                    path.display()
                ))
            }
        };
        let sends = Self {
            path: Some(path),
            created_at,
            marks,
        };
        sends.persist()?;
        Ok(sends)
    }

    /// True while the state file is younger than `IN_FLIGHT_BOUND_SECS`. A
    /// send of a lost earlier file can then still be in flight, so a missing
    /// record does not prove that a hash was never sent.
    pub(crate) fn is_young(&self, now: u64) -> bool {
        now.saturating_sub(self.created_at) < IN_FLIGHT_BOUND_SECS
    }

    /// Marks a send before its dispatch and writes the state file.
    ///
    /// Returns false and keeps the mark when the hash already has one: an
    /// earlier send can still be in flight, and its baseline is older.
    pub(crate) fn begin_send(
        &mut self,
        hash: &str,
        baseline: Option<u64>,
        now: u64,
    ) -> Result<bool, String> {
        if self.marks.contains_key(&hash.to_ascii_lowercase()) {
            return Ok(false);
        }
        self.mark(hash, baseline, now)?;
        Ok(true)
    }

    /// Removes the mark of a send with a definitive result.
    pub(crate) fn clear(&mut self, hash: &str) -> Result<(), String> {
        if self.marks.remove(&hash.to_ascii_lowercase()).is_some() {
            self.persist()?;
        }
        Ok(())
    }

    /// Marks a send and writes the state file.
    pub(crate) fn mark(
        &mut self,
        hash: &str,
        baseline: Option<u64>,
        now: u64,
    ) -> Result<(), String> {
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
    /// record before the send, or `IN_FLIGHT_BOUND_SECS` after the mark when a
    /// record exists. Until then a Failed or missing record is `Unknown`.
    ///
    /// A mark without a record never ends by time: LDK Node updates only a
    /// record that exists, so a send whose Pending write failed can have paid
    /// without any record. An operator resolves such a mark (`clear_mark`).
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
        let expired =
            record.is_some() && now.saturating_sub(mark.marked_at) >= IN_FLIGHT_BOUND_SECS;
        let current = expired
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
        write_atomically(path, &format_marks(self.created_at, &self.marks))
            .map_err(|error| format!("cannot write the state file {}: {error}", path.display()))
    }
}

fn format_marks(created_at: u64, marks: &HashMap<String, UncertainSend>) -> String {
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
    let mut text = format!("{FILE_HEADER} {created_at}\n");
    for line in lines {
        text.push_str(&line);
        text.push('\n');
    }
    text
}

fn parse(text: &str) -> Result<(u64, HashMap<String, UncertainSend>), String> {
    let mut lines = text.lines();
    let created_at = lines
        .next()
        .and_then(|line| line.strip_prefix(FILE_HEADER))
        .and_then(|rest| rest.strip_prefix(' '))
        .and_then(|time| time.parse().ok())
        .ok_or_else(|| format!("the first line is not `{FILE_HEADER} <unix time>`"))?;
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
    Ok((created_at, marks))
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
    fn a_missing_file_refuses_to_open() {
        // A lost file would drop every mark, so a start must not create it.
        let directory = TestDirectory::new();
        let error = UncertainSends::open(directory.file()).unwrap_err();
        assert!(error.contains("missing"), "{error}");
        assert!(!directory.file().exists());
    }

    fn opened(directory: &TestDirectory) -> UncertainSends {
        init_state_file(&directory.file(), 7).unwrap();
        UncertainSends::open(directory.file()).unwrap()
    }

    #[test]
    fn init_writes_the_creation_time_and_refuses_an_existing_file() {
        let directory = TestDirectory::new();
        init_state_file(&directory.file(), 7).unwrap();
        assert_eq!(
            std::fs::read_to_string(directory.file()).unwrap(),
            format!("{FILE_HEADER} 7\n")
        );
        let error = init_state_file(&directory.file(), 8).unwrap_err();
        assert!(error.contains("exists"), "{error}");
        let sends = UncertainSends::open(directory.file()).unwrap();
        assert!(sends.is_young(7 + IN_FLIGHT_BOUND_SECS - 1));
        assert!(!sends.is_young(7 + IN_FLIGHT_BOUND_SECS));
    }

    #[test]
    fn an_operator_clears_one_mark() {
        let directory = TestDirectory::new();
        let mut sends = opened(&directory);
        sends.mark(&hash(1), None, 5_000).unwrap();
        sends.mark(&hash(2), None, 5_001).unwrap();

        assert!(clear_mark(&directory.file(), &hash(1)).unwrap());
        assert!(!clear_mark(&directory.file(), &hash(1)).unwrap());
        let reopened = UncertainSends::open(directory.file()).unwrap();
        assert_eq!(reopened.marks.len(), 1);
        assert!(reopened.marks.contains_key(&hash(2)));
    }

    #[test]
    fn a_mark_is_in_the_file_before_mark_returns() {
        let directory = TestDirectory::new();
        let mut sends = opened(&directory);
        sends
            .mark(&hash(1).to_uppercase(), Some(100), 5_000)
            .unwrap();
        sends.mark(&hash(2), None, 5_001).unwrap();

        assert_eq!(
            std::fs::read_to_string(directory.file()).unwrap(),
            format!(
                "{FILE_HEADER} 7\n{} 100 5000\n{} - 5001\n",
                hash(1),
                hash(2)
            )
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
            format!("{FILE_HEADER}\n"),
            format!("{FILE_HEADER} 7\n{} 100\n", hash(1)),
            format!("{FILE_HEADER} 7\nABCD 100 5000\n"),
            format!("{FILE_HEADER} 7\n{} x 5000\n", hash(1)),
            format!("{FILE_HEADER} 7\n{0} 1 2\n{0} 3 4\n", hash(1)),
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
        let mut sends = opened(&directory);
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
        let mut sends = opened(&directory);
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

    #[test]
    fn an_expired_mark_without_a_record_stays() {
        // The node can have paid without a record: on success LDK Node updates
        // only a record that exists.
        let mut sends = UncertainSends::in_memory();
        sends.mark(&hash(1), Some(100), 5_000).unwrap();
        assert_eq!(
            sends.verdict(&hash(1), None, 5_000 + IN_FLIGHT_BOUND_SECS * 10),
            SendVerdict::Unknown
        );
        let stale = record(PaymentStatus::Failed, 100);
        assert_eq!(
            sends.verdict(&hash(1), Some(&stale), 5_000 + IN_FLIGHT_BOUND_SECS),
            SendVerdict::Record
        );
    }
}
