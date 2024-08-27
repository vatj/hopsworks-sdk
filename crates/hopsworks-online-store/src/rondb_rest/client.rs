use color_eyre::Result;
use tracing::{debug, instrument};
use reqwest::{header::HeaderValue, Method};
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
pub struct OnlineStoreRestClient {
    api_key: HeaderValue,
    url: String,
    #[builder(default)]
    client: reqwest::Client,
    #[builder(default = "0.1.0".to_string())]
    api_version: String,
}


impl OnlineStoreRestClient {
    #[instrument]
    pub async fn request(
        &self,
        method: Method,
        url: &str,
        with_authorization: bool,
    ) -> Result<reqwest::RequestBuilder> {
        let request_builder = self
            .client
            .request(method, self.build_full_url(url)?);
        if with_authorization {
            Ok(
                request_builder
                    .header("X-API-KEY", self.api_key.clone()),
            )
        } else {
            Ok(request_builder)
        }
    }

    fn build_full_url(&self, url: &str) -> Result<String> {
        let full_url = format!("https://{}:5005/{}/{}", &self.url, &self.api_version, url);
        debug!("RonDB Client Endpoint URL: {}", full_url);
        Ok(full_url)
    }
}