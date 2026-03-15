use alloy::{primitives::Address, transports::http::Client};
use futures::{stream, StreamExt};
use metrics::{counter, histogram};
use serde::Deserialize;
use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};

use crate::{
    config::constants::TOKEN_FETCH_CONCURRENCY,
    domain::{EvmNetwork, Token},
    services::errors::FetcherError,
};

const CACHE_TTL: Duration = Duration::from_secs(3600 * 5); // 5 hours

struct CachedTokenList {
    fetched_at: Instant,
    list: HashMap<u64, HashSet<Address>>,
}

pub struct TokenListFetcher {
    cache: RwLock<HashMap<String, CachedTokenList>>,
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    client: Client,
    ttl: Duration,
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    tokens: Vec<Token>,
}

impl TokenListFetcher {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            client: Client::new(),
            ttl: CACHE_TTL,
            locks: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_tokens(
        &self,
        urls: &[String],
        network: EvmNetwork,
    ) -> Result<HashSet<Address>, FetcherError> {
        let mut normalized_urls: Vec<String> = urls
            .iter()
            .map(|url| url.to_lowercase())
            .collect();
        // sort urls to have the same order during all requests to not get deadlock
        normalized_urls.sort();
        // remove possible duplicates for the same reason
        normalized_urls.dedup();

        self.fetch_with_locks(&normalized_urls).await?;

        let cached = self.collect_from_cache(&normalized_urls, network).await;
        Ok(cached)
    }

    async fn fetch_with_locks(&self, normalized_urls: &[String]) -> Result<(), FetcherError> {
        // create locks per each url if needed and gather them
        let arcs: Vec<_> = {
            let mut locks = self.locks.lock().await;
            normalized_urls.iter().map(|url| {
                locks.entry(url.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            }).collect()
        };

        let mut guards = Vec::with_capacity(arcs.len());
        for arc in arcs {
            // lock url
            guards.push(arc.lock_owned().await);
        }

        let uncached: Vec<String> = {
            let cache = self.cache.read().await;
            normalized_urls.iter().filter(|url| {
                cache.get(*url)
                    .map(|c| c.fetched_at.elapsed() > self.ttl).unwrap_or(true)
            })
                .cloned()
                .collect()
        };

        if !uncached.is_empty() {
            self.fetch_and_cache(&uncached).await?;
        }

        Ok(())
    }

    async fn fetch_and_cache(&self, urls: &[String]) -> Result<(), FetcherError> {
        let t0 = Instant::now();
        let result: Vec<(String, Result<ApiResponse, FetcherError>)> =
            stream::iter(urls.iter().cloned())
                .map(move |url| {
                    let client = self.client.clone();
                    async move {
                        let response = Self::fetch_list(&client, &url).await;
                        (url, response)
                    }
                })
                .buffer_unordered(TOKEN_FETCH_CONCURRENCY)
                .collect()
                .await;
        tracing::info!(
            time_ms = t0.elapsed().as_millis(),
            count = result.len(),
            "all tokens lists loaded"
        );

        for (_, response) in &result {
            if let Err(err) = response {
                return Err(err.clone());
            }
        }

        let mut mapped_by_url: HashMap<String, HashMap<u64, HashSet<Address>>> = HashMap::new();
        for (url, response) in result {
            if let Ok(api_resp) = response {
                let mut map_by_chain: HashMap<u64, HashSet<Address>> = HashMap::new();

                for token in api_resp.tokens {
                    map_by_chain
                        .entry(token.chain_id)
                        .or_default()
                        .insert(token.address);
                }

                if !map_by_chain.is_empty() {
                    mapped_by_url.insert(url, map_by_chain);
                }
            }
        }

        let loaded_urls: Vec<&String> = mapped_by_url.keys().collect();
        tracing::info!(lists = ?loaded_urls, "token lists loaded");

        let mut cache = self.cache.write().await;
        for (url, result) in mapped_by_url {
            cache.insert(
                url,
                CachedTokenList {
                    fetched_at: Instant::now(),
                    list: result,
                },
            );
        }

        Ok(())
    }

    async fn fetch_list(client: &Client, url: &String) -> Result<ApiResponse, FetcherError> {
        let t0 = Instant::now();

        client
            .get(url)
            .send()
            .await
            .inspect(move |_| {
                counter!("token_list_load_total").increment(1);
                histogram!("token_list_loaded_time_in_ms").record(t0.elapsed().as_millis() as f64);
                tracing::info!(
                    time_ms = ?t0.elapsed().as_millis(),
                    url = ?url,
                    "token list loaded"
                );
            })
            .map_err(|err| {
                counter!("token_list_load_failed_total").increment(1);
                FetcherError::UnableToLoadList(url.clone(), err.to_string())
            })?
            .json()
            .await
            .map_err(|err| FetcherError::UnableToLoadList(url.clone(), err.to_string()))
    }

    async fn collect_from_cache(&self, urls: &[String], network: EvmNetwork) -> HashSet<Address> {
        let cached_lists = self.cache.read().await;

        let mut result: HashSet<Address> = HashSet::new();
        for url in urls {
            if let Some(cached) = cached_lists.get(url) {
                if let Some(cached_by_chain) = cached.list.get(&network.chain_id()) {
                    result.extend(cached_by_chain.iter().cloned());
                }
            }
        }

        result
    }
}
