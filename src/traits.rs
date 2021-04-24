use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Display;

/// Trait that ensures a value has a nil representation.
/// The nil representation of a value is used as a delete marker to
/// enable delete operation on a value.
/// The nil marker should be a value of the value type that is small
/// and unique.
pub trait Nil {
    fn nil() -> Self;
}

/// Trait to be implemented by a key used to persist
/// values into the store.
pub trait ResourceKey: Clone + Display + Ord + Serialize + DeserializeOwned {}

/// Trait to be implemented by values to be persisted in the store.
pub trait ResourceValue: Clone + Display + Serialize + DeserializeOwned + Nil + PartialEq {}
