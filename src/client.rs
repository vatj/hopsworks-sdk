use log::{info, warn};
use reqwest::header::HeaderValue;
use serde::Serialize;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

pub const DEFAULT_HOPSWORKS_API_PREFIX: &str = "https://localhost:8182/hopsworks-api/api/";

#[derive(Clone, Debug)]
pub struct HopsworksClientConfig {
    prefix: String,
}

impl Default for HopsworksClientConfig {
    fn default() -> Self {
        Self {
            prefix: String::from(DEFAULT_HOPSWORKS_API_PREFIX),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HopsworksClient {
    pub client: reqwest::Client,
    pub token: Arc<Mutex<Option<HeaderValue>>>,
    pub api_key: Arc<Mutex<Option<HeaderValue>>>,
    pub config: HopsworksClientConfig,
}

impl Default for HopsworksClient {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
            token: Arc::new(Mutex::new(None)),
            api_key: Arc::new(Mutex::new(None)),
            config: HopsworksClientConfig::default(),
        }
    }
}

impl HopsworksClient {
    fn get_client(&self) -> &reqwest::Client {
        &self.client
    }

    fn get_token(&self) -> Arc<Mutex<Option<HeaderValue>>> {
        Arc::clone(&self.token)
    }

    fn get_api_key(&self) -> Arc<Mutex<Option<HeaderValue>>> {
        Arc::clone(&self.api_key)
    }

    fn get_config(&self) -> &HopsworksClientConfig {
        &self.config
    }

    fn endpoint_url(&self, url: &str) -> String {
        // Using the client's prefix in case it's a relative route.
        if url.starts_with("https") {
            url.to_string()
        } else {
            self.get_config().prefix.clone() + url
        }
    }

    async fn get_authorization_header_value(&self) -> HeaderValue {
        if self.get_token().lock().await.is_some() {
            self.get_token()
                .lock()
                .await
                .as_ref()
                .unwrap_or(&HeaderValue::from_static(""))
                .clone()
        } else if self.get_api_key().lock().await.is_some() {
            self.get_api_key()
                .lock()
                .await
                .as_ref()
                .unwrap_or(&HeaderValue::from_static(""))
                .clone()
        } else {
            HeaderValue::from_static("")
        }
    }

    pub async fn get(&self, url: &str) -> Result<reqwest::Response, reqwest::Error> {
        let absolute_url = self.endpoint_url(url);
        info!("GET : {}", absolute_url);
        self.get_client()
            .get(absolute_url)
            .header("authorization", self.get_authorization_header_value().await)
            .send()
            .await
    }

    pub async fn post_form<T: Serialize + ?Sized>(
        &self,
        url: &str,
        form: &T,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let absolute_url = self.endpoint_url(url);
        info!("POST : {}", absolute_url);
        self.get_client()
            .post(absolute_url)
            .header("authorization", self.get_authorization_header_value().await)
            .form(&form)
            .send()
            .await
    }

    pub async fn post_json<T: Serialize + ?Sized>(
        &self,
        url: &str,
        payload: &T,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let absolute_url = self.endpoint_url(url);
        info!("POST : {}", absolute_url);
        self.get_client()
            .post(absolute_url)
            .header("authorization", self.get_authorization_header_value().await)
            .json(&payload)
            .send()
            .await
    }

    pub async fn login_with_email_and_password(
        &self,
        email: &String,
        password: &String,
    ) -> Result<(), reqwest::Error> {
        let mut login_params = HashMap::new();
        login_params.insert("email", email);
        login_params.insert("password", password);

        let response = self.post_form("auth/login", &login_params).await?;

        if response.status() == reqwest::StatusCode::OK {
            let new_token = response.headers().get("authorization");

            if new_token.is_none() {
                warn!("login_with_email_and_password failed to return a token")
            } else {
                *self.get_token().lock().await = Some(new_token.unwrap().to_owned());
                info!("A new bearer token has been saved.");
            }
        } else {
            warn!(
                "login_with_email_and_password failed with status : {}",
                response.status()
            )
        }

        Ok(())
    }

    pub async fn set_api_key(&self, new_api_key: String) -> Result<(), reqwest::Error> {
        let header_key = HeaderValue::from_str(format!("ApiKey {}", new_api_key).as_str());

        if header_key.is_ok() {
            *self.get_api_key().lock().await = Some(header_key.unwrap());
            info!("A new API key has been saved.");
        } else {
            warn!("The provided Apikey is not valid : {}", new_api_key);
        }

        Ok(())
    }
}
