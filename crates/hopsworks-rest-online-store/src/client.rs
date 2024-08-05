use color_eyre::Result;
use typed_builder::TypedBuilder;

static DEFAULT_API_VERSION: &str = "0.1.0";
static DEFAULT_HOSTNAME: &str = "app.hopsworks.ai";
static DEFAULT_PORT: u16 = 5005;

#[derive(Debug, Clone, TypedBuilder)]
pub struct OnlineStoreRestClient {
    api_key: String,
    api_version: String,
    hostname: String,
    port: u16,
    #[builder(setter(suffix = "_client"))]
    inner: reqwest::Client,
}

impl OnlineStoreRestClient {
    pub async fn request(&self, method: reqwest::Method, path: &str, with_auth: bool) -> Result<reqwest::RequestBuilder> {
        let url = self.build_url(path);
        let mut request_builder = self.inner.request(method, &url);
        if with_auth {
            request_builder = request_builder.header("X-API-KEY", &self.api_key);
        }
        Ok(request_builder)
    }

    fn build_url(&self, path: &str) -> String {
        format!("https://{}:{}/{}/{}", self.hostname, self.port, self.api_version, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_request() {
        let client = OnlineStoreRestClient::builder()
            .api_key("invalid_key".to_string())
            .hostname("app.hopsworks.ai".to_string())
            .port(5005)
            .api_version("0.1.0".to_string())
            .inner_client(reqwest::Client::new())
            .build();
        let request = client.request(reqwest::Method::GET, "test", true).await.unwrap().build().unwrap();

        assert_eq!(request.url().as_str(), "https://app.hopsworks.ai:5005/0.1.0/test");
        assert_eq!(request.headers().get("X-API-KEY").unwrap(), "invalid_key");
    }
}
