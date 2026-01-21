pub mod metadata;
pub fn get_env(key: &str) -> String {
    std::env::var(key).expect(&format!("Error: Environment variable {} not set.",key))
}
