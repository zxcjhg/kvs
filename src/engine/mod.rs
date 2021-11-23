use crate::common::Result;

pub trait KvsEngine: Clone + Send + 'static {
    /// Sets a `value` for a given `key`
    /// Overrides with new `value` if `key` already exists
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Retrieves value from storage for a given `key`
    /// Returs None if key not found
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Removes a entry for a given `key`
    fn remove(&self, key: String) -> Result<()>;
}

mod lskv;
mod olskv;
mod sled;
pub use self::sled::SledStore;
pub use lskv::LogStructKVStore;
pub use olskv::OptLogStructKvs;
