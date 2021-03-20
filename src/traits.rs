use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Display;

/// Trait to be implemented by a key used to persist
/// values into the store.
pub trait ResourceKey: Clone + Display + Ord + Serialize + DeserializeOwned {}

/// Trait to be implemented by values to be persisted in the store.
pub trait ResourceValue: Clone + Display + Serialize + DeserializeOwned {}
