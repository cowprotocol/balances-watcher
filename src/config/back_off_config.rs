use backon::ExponentialBuilder;
use std::time::Duration;

const TOKEN_LIST_FETCHER_BACKOFFS: usize = 3;

// token list fetcher backoff configuration
// needed to handle errors when load token list
pub fn get_token_list_fetcher_backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_secs(1))
        .with_max_delay(Duration::from_secs(3))
        .with_max_times(TOKEN_LIST_FETCHER_BACKOFFS)
        .with_jitter()
}
