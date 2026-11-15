#[derive(Debug)]
pub enum Error {
    Core(String),
    Io(String),
    Corrupt(String),
    Identity(String),
    Raft(raft::Error),
    NotLeader,
    Apply(String),
}
pub type Result<T> = std::result::Result<T, Error>;
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Core(s) => write!(f, "core: {s}"),
            Self::Io(s) => write!(f, "I/O: {s}"),
            Self::Corrupt(s) => write!(f, "corrupt log: {s}"),
            Self::Identity(s) => write!(f, "identity: {s}"),
            Self::Raft(e) => e.fmt(f),
            Self::NotLeader => f.write_str("not leader"),
            Self::Apply(s) => write!(f, "apply: {s}"),
        }
    }
}
impl std::error::Error for Error {}
macro_rules! io_from {
    ($($t:ty),*) => { $(impl From<$t> for Error {
        fn from(e: $t) -> Self { Self::Io(e.to_string()) }
    })* };
}
io_from!(
    std::io::Error,
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    redb::SetDurabilityError
);
impl From<protobuf::ProtobufError> for Error {
    fn from(e: protobuf::ProtobufError) -> Self {
        Self::Corrupt(e.to_string())
    }
}
