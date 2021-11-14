use crate::common::Result;

pub trait KvsEngine {
    /// Sets a `value` for a given `key`
    /// Overrides with new `value` if `key` already exists
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Retrieves value from storage for a given `key`
    /// Returs None if key not found
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Removes a entry for a given `key`
    fn remove(&mut self, key: String) -> Result<()>;
}

mod lskv;
mod sled;

pub use self::sled::SledStore;
pub use lskv::LogStructKVStore;
