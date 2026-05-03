use alloy::primitives::Address;
use alloy::transports::BoxFuture;
use backon::{ExponentialBuilder, Retryable};
use futures::future::{try_join_all, FutureExt, Shared};
use metrics::{counter, histogram};
use reqwest::{Client, Response};
use serde::Deserialize;
use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};

use crate::{
    domain::{EvmNetwork, Token},
    services::errors::FetcherError,
};

const BACK_OFFS: usize = 3;

type SharedFetchTask = Shared<BoxFuture<'static, Result<(), FetcherError>>>;

type ListUrl = String;

// need to load token lists and save them to cache
struct CachedTokenList {
    fetched_at: Instant,
    // a list can have tokens from multiple chains, so we need to map them
    list: HashMap<u64, HashSet<Address>>,
}

pub struct TokenListFetcher {
    // store cached lists by url
    cache: RwLock<HashMap<ListUrl, CachedTokenList>>,
    // store already fetched futures, need to share them between few requests with the same token lists
    // to not load them, if they are already in flight
    fetch_tasks: Mutex<HashMap<ListUrl, SharedFetchTask>>,
    // http client
    client: Client,
    // cache duration
    ttl: Duration,
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    tokens: Vec<Token>,
}

impl TokenListFetcher {
    pub fn new(cache_ttl: Duration) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            client: Client::new(),
            ttl: cache_ttl,
            fetch_tasks: Mutex::new(HashMap::new()),
        }
    }

    // this function incapsulate fetching and cache logic
    // if the list was cached and ttl is valid - return cache result per url
    // otherwise - share or create a new future(if it doesnt exist) to fetch the list and share the result
    pub async fn get_tokens(
        self: Arc<Self>,
        urls: &[String],
        network: EvmNetwork,
    ) -> Result<HashSet<Address>, FetcherError> {
        Arc::clone(&self).fetch_uncached(urls).await?;
        let tokens = self.get_cached(urls, network).await;
        Ok(tokens)
    }

    // get cached token lists and filter them by network
    async fn get_cached(&self, urls: &[String], network: EvmNetwork) -> HashSet<Address> {
        let cache = self.cache.read().await;
        urls.iter()
            .filter_map(|url| cache.get(url))
            .filter_map(|cached_by_chain_id| cached_by_chain_id.list.get(&network.chain_id()))
            .flatten()
            .copied()
            .collect()
    }

    async fn fetch_uncached(self: Arc<Self>, urls: &[String]) -> Result<(), FetcherError> {
        let uncached = self.get_uncached(urls).await;

        let fetch_tasks_iter = uncached.into_iter().map(|url| {
            let this = Arc::clone(&self);
            this.fetch_and_update_cache(url)
        });

        try_join_all(fetch_tasks_iter).await?;
        Ok(())
    }

    // check if urls were already cached
    // returns a list of uncached urls (if the url is not in the cache or the cache is invalid)
    async fn get_uncached<'a>(&self, urls: &'a [String]) -> Vec<&'a String> {
        let cache = self.cache.read().await;

        urls.iter()
            .filter(|url| {
                cache
                    .get(*url)
                    .is_none_or(|cached| cached.fetched_at.elapsed() > self.ttl)
            })
            .collect()
    }

    // check if the future to fetch token list was already created
    // if it was - just clone it, otherwise - create a new one
    // new future fetch data and store it in cache directly to avoid deduplication
    async fn fetch_and_update_cache(self: Arc<Self>, url: &String) -> Result<(), FetcherError> {
        let fetch_future = {
            let mut fetch_guard = self.fetch_tasks.lock().await;
            if let Some(future) = fetch_guard.get(url) {
                future.clone()
            } else {
                let client = self.client.clone();
                let url_cloned = url.clone();
                let this = Arc::clone(&self);

                let new_future = async move {
                    // fetch data and update cache
                    let response = Self::fetch_list(&client, &url_cloned).await?;

                    this.store_response_in_cache(url_cloned, response).await;

                    Ok(())
                }
                .boxed()
                .shared();

                fetch_guard.insert(url.clone(), new_future.clone());
                new_future
            }
        };

        let result = fetch_future.await;
        self.remove_fetch_task_if_resolved(url).await;

        result
    }

    // check if the fetch task was already resolved
    // if it was - removed it, otherwise - does nothing
    // it's needed to protect removing task from new caller that wasn't resolved yet
    async fn remove_fetch_task_if_resolved(&self, url: &String) {
        let mut fetch_tasks_guard = self.fetch_tasks.lock().await;
        if let Some(task) = fetch_tasks_guard.get(url) {
            if task.peek().is_some() {
                fetch_tasks_guard.remove(url);
            }
        }
    }

    // map response into CachedTokenList and save
    async fn store_response_in_cache(&self, url: String, response: ApiResponse) {
        let mut mapped_by_chain_id: HashMap<u64, HashSet<Address>> = HashMap::new();
        for token in response.tokens {
            mapped_by_chain_id
                .entry(token.chain_id)
                .or_default()
                .insert(token.address);
        }

        let cached = CachedTokenList {
            list: mapped_by_chain_id,
            fetched_at: Instant::now(),
        };

        self.cache.write().await.insert(url, cached);
    }

    async fn fetch_list(client: &Client, url: &String) -> Result<ApiResponse, FetcherError> {
        let t0 = Instant::now();

        Self::fetch_with_backoff(client, url)
            .await
            .inspect(move |_| {
                counter!("token_list_loaded_total").increment(1);
                histogram!("token_list_loaded_time_in_ms").record(t0.elapsed().as_millis() as f64);
                tracing::info!(
                    time_ms = ?t0.elapsed().as_millis(),
                    url = ?url,
                    "token list loaded"
                );
            })?
            .json()
            .await
            .map_err(|err| FetcherError::UnableToLoadList(url.clone(), err.to_string()))
    }

    async fn fetch_with_backoff(client: &Client, url: &String) -> Result<Response, FetcherError> {
        let backoff = Self::get_backoff();
        let resp = (|| async { client.get(url).send().await?.error_for_status() })
            .retry(backoff)
            .await
            .map_err(|err| {
                counter!("token_list_load_failed_total").increment(1);
                FetcherError::UnableToLoadList(url.clone(), err.to_string())
            })?;

        Ok(resp)
    }

    fn get_backoff() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(3))
            .with_max_times(BACK_OFFS)
            .with_jitter()
    }
}

#[cfg(test)]
mod token_list_fetcher_tests {
    use std::thread::sleep;

    use super::*;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn make_token_list_json(tokens: Vec<(u64, Address)>) -> serde_json::Value {
        serde_json::json!({
            "tokens": tokens.iter().map(|(chain_id, address)| {
                serde_json::json!({ "chainId": chain_id, "address": address })
            }).collect::<Vec<_>>()
        })
    }

    fn make_token_list(chain_ids: Vec<u64>, len: usize) -> Vec<(u64, Address)> {
        chain_ids
            .into_iter()
            .map(|chain_id| (0..len).map(move |_| (chain_id, Address::random())))
            .flatten()
            .collect()
    }

    fn make_error() -> serde_json::Value {
        serde_json::json!({
            "message": "unavailable"
        })
    }

    #[tokio::test]
    async fn test_fail_backoffs() {
        let server = MockServer::start().await;

        let resp_template = ResponseTemplate::new(500).set_body_json(make_error());
        let retries = BACK_OFFS as u64 + 1;
        Mock::given(method("GET"))
            .respond_with(resp_template)
            .expect(retries)
            .mount(&server)
            .await;

        let fetcher = Arc::new(TokenListFetcher::new(Duration::from_millis(300)));
        let result = Arc::clone(&fetcher)
            .get_tokens(&vec![server.uri()], EvmNetwork::Eth)
            .await;

        assert!(result.is_err());
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

        let resp_template =
            ResponseTemplate::new(200).set_body_json(make_token_list_json(token_list.clone()));
        Mock::given(method("GET"))
            .respond_with(resp_template)
            .expect(2)
            .mount(&server)
            .await;

        let fetcher = Arc::new(TokenListFetcher::new(Duration::from_millis(300)));
        // warm up cache
        let _ = Arc::clone(&fetcher)
            .get_tokens(&vec![server.uri()], EvmNetwork::Gnosis)
            .await;

        // cache is still valid
        sleep(Duration::from_millis(100));

        let tokens = Arc::clone(&fetcher)
            .get_tokens(&vec![server.uri()], network)
            .await
            .unwrap();

        assert_eq!(expected_list_by_chain, tokens);

        // invalidate cache
        sleep(Duration::from_millis(200));

        let tokens = Arc::clone(&fetcher)
            .get_tokens(&vec![server.uri()], network)
            .await
            .unwrap();

        assert_eq!(expected_list_by_chain, tokens);
    }
}
