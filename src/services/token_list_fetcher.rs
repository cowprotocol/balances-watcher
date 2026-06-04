//! Loads ERC20 token lists over HTTP and exposes the set of token addresses
//! for the active EVM network.
//!
//! [`TokenListFetcher`] is single-network: each instance is bound to one
//! [`EvmNetwork`] and filters every fetched list down to that chain. Results
//! are cached per URL via [`moka`] with a TTL, and concurrent fetches of the
//! same URL are coalesced, so multiple subscriptions sharing one fetcher never
//! trigger duplicate HTTP calls. Retries on transient HTTP failures are handled
//! with exponential backoff ([`backon`]).

use crate::{
    domain::{EvmNetwork, Token},
    metrics::Metrics,
    services::errors::FetcherError,
};
use alloy::primitives::Address;
use backon::{ExponentialBuilder, Retryable};
use futures::future::try_join_all;
use moka::future::Cache;
use reqwest::{Client, Response};
use serde::Deserialize;
use std::sync::Arc;
use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

const CACHE_CAPACITY: u64 = 256;

type ListUrl = String;

type ChainTokens = HashSet<Address>;

pub struct TokenListFetcher {
    cache: Cache<ListUrl, ChainTokens>,
    // http client
    client: Client,
    // backoff configuration
    backoff_cfg: ExponentialBuilder,
    metrics: Arc<Metrics>,
    // active network; the fetcher is single-chain, so it filters lists by this one
    network: EvmNetwork,
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    tokens: Vec<Token>,
}

impl TokenListFetcher {
    pub fn new(
        cache_ttl: Duration,
        backoff_cfg: ExponentialBuilder,
        metrics: Arc<Metrics>,
        network: EvmNetwork,
    ) -> Self {
        Self {
            cache: Cache::builder()
                .time_to_live(cache_ttl)
                .max_capacity(CACHE_CAPACITY)
                .build(),
            client: Client::new(),
            backoff_cfg,
            metrics,
            network,
        }
    }

    /// Fetches the given token lists, filters them down to the fetcher's active
    /// network and returns the union of all token addresses across the lists.
    ///
    /// Caching and in-flight de-duplication are delegated to [`moka`]: per-URL
    /// results are cached with a TTL, and concurrent requests for the same URL
    /// are coalesced into a single fetch.
    ///
    /// Fails fast: if any list fails to load (after retries), the whole call
    /// returns an error. This is intentional — clients must receive every
    /// requested token, otherwise we cannot rely on the reported balances.
    /// A failed fetch is not cached, so a subsequent call retries it.
    pub async fn get_tokens(&self, urls: &[String]) -> Result<ChainTokens, FetcherError> {
        let token_lists_handlers = urls.iter().map(|url| {
            let url_clone = url.clone();

            async move {
                self.cache
                    .try_get_with(
                        url_clone.clone(),
                        self.fetch_list_and_filter_by_chain(&url_clone),
                    )
                    .await
            }
        });

        let tokens = try_join_all(token_lists_handlers)
            .await
            .map_err(|err| (*err).clone())?
            .into_iter()
            .flatten();
        Ok(tokens.collect())
    }

    async fn fetch_list_and_filter_by_chain(&self, url: &str) -> Result<ChainTokens, FetcherError> {
        let network = self.network;
        let token_set = self
            .fetch_list(url)
            .await?
            .tokens
            .into_iter()
            .filter_map(|token| {
                if token.chain_id == network.chain_id() {
                    Some(token.address)
                } else {
                    None
                }
            });

        Ok(token_set.collect())
    }

    async fn fetch_list(&self, url: &str) -> Result<ApiResponse, FetcherError> {
        let t0 = Instant::now();
        let metrics = Arc::clone(&self.metrics);

        self.fetch_with_backoff(url)
            .await
            .inspect(move |_| {
                metrics.token_list_loaded_total.increment(1);
                metrics
                    .token_list_loaded_time_in_ms
                    .record(t0.elapsed().as_millis() as f64);
                tracing::debug!(
                    time_ms = ?t0.elapsed().as_millis(),
                    url = ?url,
                    "token list loaded"
                );
            })?
            .json()
            .await
            .map_err(|err| FetcherError::UnableToLoadList(url.into(), err.to_string()))
    }

    async fn fetch_with_backoff(&self, url: &str) -> Result<Response, FetcherError> {
        let resp = (|| async { self.client.get(url).send().await?.error_for_status() })
            .retry(self.backoff_cfg)
            .await
            .map_err(|err| {
                self.metrics.token_list_load_failed_total.increment(1);
                FetcherError::UnableToLoadList(url.into(), err.to_string())
            })?;

        Ok(resp)
    }
}

#[cfg(test)]
mod token_list_fetcher_tests {
    use super::*;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const BACK_OFFS: u64 = 3;

    fn make_token_list_resp_template(tokens: Vec<(u64, Address)>) -> ResponseTemplate {
        let token_list = serde_json::json!({
            "tokens": tokens.iter().map(|(chain_id, address)| {
                serde_json::json!({ "chainId": chain_id, "address": address })
            }).collect::<Vec<_>>()
        });

        ResponseTemplate::new(200).set_body_json(token_list)
    }

    fn make_token_list(chain_ids: Vec<u64>, len: usize) -> Vec<(u64, Address)> {
        chain_ids
            .into_iter()
            .flat_map(|chain_id| (0..len).map(move |_| (chain_id, Address::random())))
            .collect()
    }

    fn make_error_resp_template() -> ResponseTemplate {
        let error = serde_json::json!({
            "message": "unavailable"
        });

        ResponseTemplate::new(500).set_body_json(error)
    }

    fn make_fetcher() -> Arc<TokenListFetcher> {
        let back_off = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(1))
            .with_max_delay(Duration::from_millis(20))
            .with_max_times(BACK_OFFS as usize)
            .with_jitter();

        Arc::new(TokenListFetcher::new(
            Duration::from_millis(300),
            back_off,
            Arc::new(Metrics::install()),
            EvmNetwork::Eth,
        ))
    }

    #[tokio::test]
    async fn test_fail_backoffs() {
        let server = MockServer::start().await;

        let resp_template = make_error_resp_template();
        let retries = BACK_OFFS + 1;
        Mock::given(method("GET"))
            .respond_with(resp_template)
            .expect(retries)
            .mount(&server)
            .await;

        let fetcher = make_fetcher();
        let result = Arc::clone(&fetcher).get_tokens(&[server.uri()]).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_fail_and_success_after() {
        let server = MockServer::start().await;

        // fail case
        let resp_template = make_error_resp_template();
        let retries = BACK_OFFS + 1;
        Mock::given(method("GET"))
            .respond_with(resp_template)
            .up_to_n_times(retries)
            .with_priority(1)
            .mount(&server)
            .await;

        let fetcher = make_fetcher();
        let result = Arc::clone(&fetcher).get_tokens(&[server.uri()]).await;

        assert!(result.is_err());

        // success after if there is a new client
        let resp_template = make_token_list_resp_template(vec![]);
        Mock::given(method("GET"))
            .respond_with(resp_template)
            .with_priority(2)
            .expect(1)
            .mount(&server)
            .await;

        let response = Arc::clone(&fetcher).get_tokens(&[server.uri()]).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_cache() {
        let server = MockServer::start().await;
        let token_list = make_token_list(vec![1, 2, 100], 3);
        let network = EvmNetwork::Eth;
        let expected_list_by_chain: HashSet<_> = token_list
            .clone()
            .into_iter()
            .filter_map(|(chain_id, address)| {
                if chain_id == network.chain_id() {
                    Some(address)
                } else {
                    None
                }
            })
            .collect();

        let resp_template = make_token_list_resp_template(token_list.clone());
        Mock::given(method("GET"))
            .respond_with(resp_template)
            .expect(2)
            .mount(&server)
            .await;

        let fetcher = make_fetcher();
        // warm up cache
        let _ = Arc::clone(&fetcher)
            .get_tokens(&[server.uri()])
            .await
            .unwrap();

        // cache is still valid
        tokio::time::sleep(Duration::from_millis(100)).await;

        let tokens = Arc::clone(&fetcher)
            .get_tokens(&[server.uri()])
            .await
            .unwrap();

        assert_eq!(expected_list_by_chain, tokens);

        // invalidate cache
        tokio::time::sleep(Duration::from_millis(200)).await;

        let tokens = Arc::clone(&fetcher)
            .get_tokens(&[server.uri()])
            .await
            .unwrap();

        assert_eq!(expected_list_by_chain, tokens);
    }

    #[tokio::test]
    async fn test_concurrent_request_deduplication() {
        let server = MockServer::start().await;
        let token_list = make_token_list(vec![1, 2, 100], 3);

        let resp_template = make_token_list_resp_template(token_list.clone());
        Mock::given(method("GET"))
            .respond_with(resp_template)
            .expect(1)
            .mount(&server)
            .await;

        let fetcher = make_fetcher();

        let handlers: Vec<_> = (0..10)
            .map(|_| {
                let urls = [server.uri()];
                let fetcher = Arc::clone(&fetcher);
                tokio::spawn(async move { fetcher.get_tokens(&urls).await })
            })
            .collect();

        for handler in handlers {
            let result = handler.await;
            assert!(result.is_ok());
        }
    }
}
