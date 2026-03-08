use clap::Parser;

const DEFAULT_TOKEN_LIST_PATH: &str = "configs/tokens_list.json";

#[derive(Parser, Debug, Clone)]
pub struct Args {
    #[arg(long, env = "HTTP_BIND", default_value = "0.0.0.0:8080")]
    pub bind: String,

    #[arg(long, env = "ALCHEMY_API_KEY", default_value = "")]
    pub alchemy_api_key: String,

    #[arg(long, env="TOKEN_LIST_PATH", default_value=DEFAULT_TOKEN_LIST_PATH)]
    pub token_list_path: String,

    #[arg(long, env = "SNAPSHOT_INTERVAL", default_value = "60")]
    pub snapshot_interval: String,

    #[arg(long, env = "MAX_WATCHED_TOKENS_LIMIT", default_value = "1000")]
    pub max_watched_tokens_limit: String,

    #[arg(long, env = "ALLOWED_ORIGINS", default_value = "")]
    pub allowed_origins: String,
}

impl Args {
    pub fn from_env() -> Self {
        Self::parse()
    }
}
