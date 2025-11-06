use std::any::Any;
use std::collections::HashMap;

/// Evaluation context that stores arbitrary values by string keys
/// Similar to golang's map[string]any
pub struct EvalContext {
    data: HashMap<String, Box<dyn Any + Send + Sync>>,
}

impl EvalContext {
    /// Create a new empty evaluation context
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Insert a value into the context with the given key
    /// 
    /// # Arguments
    /// 
    /// * `key` - The string key to store the value under
    /// * `value` - The value to store (must be Send + Sync + 'static)
    /// 
    /// # Examples
    /// 
    /// ```
    /// use flow::expr::context::EvalContext;
    /// 
    /// let mut ctx = EvalContext::new();
    /// ctx.set("user_id", 42i64);
    /// ctx.set("session_id", "abc123".to_string());
    /// ```
    pub fn set<T: Send + Sync + 'static>(&mut self, key: impl Into<String>, value: T) {
        self.data.insert(key.into(), Box::new(value));
    }

    /// Get a value from the context by key
    /// 
    /// # Arguments
    /// 
    /// * `key` - The string key to retrieve the value for
    /// 
    /// # Returns
    /// 
    /// Returns `Some(&T)` if the key exists and the value is of type `T`,
    /// `None` otherwise.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use flow::expr::context::EvalContext;
    /// 
    /// let mut ctx = EvalContext::new();
    /// ctx.set("user_id", 42i64);
    /// 
    /// if let Some(user_id) = ctx.get::<i64>("user_id") {
    ///     println!("User ID: {}", user_id);
    /// }
    /// ```
    pub fn get<T: 'static>(&self, key: &str) -> Option<&T> {
        self.data.get(key)?.downcast_ref::<T>()
    }

    /// Get a mutable reference to a value from the context by key
    /// 
    /// # Arguments
    /// 
    /// * `key` - The string key to retrieve the value for
    /// 
    /// # Returns
    /// 
    /// Returns `Some(&mut T)` if the key exists and the value is of type `T`,
    /// `None` otherwise.
    pub fn get_mut<T: 'static>(&mut self, key: &str) -> Option<&mut T> {
        self.data.get_mut(key)?.downcast_mut::<T>()
    }

    /// Remove a value from the context by key
    /// 
    /// # Arguments
    /// 
    /// * `key` - The string key to remove
    /// 
    /// # Returns
    /// 
    /// Returns `Some(T)` if the key existed and the value was of type `T`,
    /// `None` otherwise.
    pub fn remove<T: 'static>(&mut self, key: &str) -> Option<T> {
        self.data.remove(key)?.downcast().ok().map(|boxed| *boxed)
    }

    /// Check if the context contains a key
    /// 
    /// # Arguments
    /// 
    /// * `key` - The string key to check
    /// 
    /// # Returns
    /// 
    /// Returns `true` if the key exists, `false` otherwise.
    pub fn contains_key(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    /// Get the number of items in the context
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the context is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clear all values from the context
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

impl Default for EvalContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let mut ctx = EvalContext::new();
        ctx.set("int", 42i64);
        ctx.set("string", "hello".to_string());
        ctx.set("bool", true);

        assert_eq!(ctx.get::<i64>("int"), Some(&42));
        assert_eq!(ctx.get::<String>("string"), Some(&"hello".to_string()));
        assert_eq!(ctx.get::<bool>("bool"), Some(&true));
    }

    #[test]
    fn test_get_wrong_type() {
        let mut ctx = EvalContext::new();
        ctx.set("int", 42i64);

        // Getting a different type should return None
        assert_eq!(ctx.get::<String>("int"), None);
    }

    #[test]
    fn test_get_missing_key() {
        let ctx = EvalContext::new();
        assert_eq!(ctx.get::<i64>("missing"), None);
    }

    #[test]
    fn test_remove() {
        let mut ctx = EvalContext::new();
        ctx.set("int", 42i64);

        assert_eq!(ctx.remove::<i64>("int"), Some(42));
        assert_eq!(ctx.get::<i64>("int"), None);
    }

    #[test]
    fn test_contains_key() {
        let mut ctx = EvalContext::new();
        ctx.set("key", "value".to_string());

        assert!(ctx.contains_key("key"));
        assert!(!ctx.contains_key("missing"));
    }

    #[test]
    fn test_clear() {
        let mut ctx = EvalContext::new();
        ctx.set("key1", 1i64);
        ctx.set("key2", 2i64);

        assert_eq!(ctx.len(), 2);
        ctx.clear();
        assert_eq!(ctx.len(), 0);
        assert!(ctx.is_empty());
    }
}
