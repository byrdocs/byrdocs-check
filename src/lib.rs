pub mod metadata;

pub fn get_env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("Error: Environment variable {key} not set."))
}

pub fn get_env_or(key: &str,val:String) -> String {
    std::env::var(key).unwrap_or_else(|_| val)
}

pub fn get_optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
