use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OnlineStoreRestClientBuilder {}

impl OnlineStoreRestClientBuilder {
    pub async fn build(&self) -> OnlineStoreRestClient {
        OnlineStoreRestClient {
            inner: reqwest::Client::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OnlineStoreRestClient {
    inner: reqwest::Client,
}
