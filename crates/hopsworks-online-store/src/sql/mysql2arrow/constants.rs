pub(crate) const SECONDS_IN_DAY: i64 = 86_400;
const KILO: usize = 1 << 10;
pub const RECORD_BATCH_SIZE: usize = 64 * KILO;
pub const DB_BUFFER_SIZE: usize = 32;