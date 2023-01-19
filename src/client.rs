use color_eyre::Result;
use log::{info, warn};
use reqwest::header::HeaderValue;
use serde::Serialize;
use std::{collections::HashMap, env, sync::Arc};
use tokio::sync::Mutex;

use crate::{
    api::project::entities::Project,
    domain::{
        credentials::controller::write_locally_project_credentials_on_login,
        project::controller::get_project_list,
    },
    repositories::project::entities::ProjectDTO,
};

// pub const DEFAULT_HOPSWORKS_API_PREFIX: &str = "https://localhost:8182/hopsworks-api/api/";
pub const DEFAULT_HOPSWORKS_API_PREFIX: &str =
    "https://staging.cloud.hopsworks.ai/hopsworks-api/api/";
pub const DEFAULT_HOPSWORKS_CERT_DIR: &str = "/tmp/";

#[derive(Clone, Debug)]
pub struct HopsworksClientConfig {
    prefix: String,
    cert_dir: String,
}

impl Default for HopsworksClientConfig {
    fn default() -> Self {
        Self {
            prefix: String::from(DEFAULT_HOPSWORKS_API_PREFIX),
            cert_dir: String::from(DEFAULT_HOPSWORKS_CERT_DIR),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HopsworksClient {
    pub client: reqwest::Client,
    pub token: Arc<Mutex<Option<HeaderValue>>>,
    pub api_key: Arc<Mutex<Option<HeaderValue>>>,
    pub project_id: Arc<Mutex<i32>>,
    pub config: HopsworksClientConfig,
}

impl Default for HopsworksClient {
    fn default() -> Self {
        Self {
            config: HopsworksClientConfig::default(),
            client: reqwest::Client::new(),
            token: Arc::new(Mutex::new(None)),
            api_key: Arc::new(Mutex::new(None)),
            project_id: Arc::new(Mutex::new(0)),
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

    pub fn get_project_id(&self) -> Arc<Mutex<i32>> {
        Arc::clone(&self.project_id)
    }

    fn get_config(&self) -> &HopsworksClientConfig {
        &self.config
    }

    async fn endpoint_url(&self, url: &str, with_project_id: bool) -> String {
        // Using the client's prefix in case it's a relative route.
        if url.starts_with("https") {
            url.to_string()
        } else {
            // with_project_id only applies for relative url
            if with_project_id {
                format!(
                    "{}project/{}/{url}",
                    self.get_config().prefix.clone(),
                    self.get_project_id().lock().await
                )
            } else {
                self.get_config().prefix.clone() + url
            }
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

    pub async fn send_get(
        &self,
        url: &str,
        with_project_id: bool,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let absolute_url = self.endpoint_url(url, with_project_id).await;
        info!("GET : {}", absolute_url);
        self.get_client()
            .get(absolute_url)
            .header("authorization", self.get_authorization_header_value().await)
            .send()
            .await
    }

    pub async fn send_get_with_query_params<T: Serialize + ?Sized>(
        &self,
        url: &str,
        query_params: &T,
        with_project_id: bool,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let absolute_url = self.endpoint_url(url, with_project_id).await;
        info!("GET : {}", absolute_url);
        self.get_client()
            .get(absolute_url)
            .query(query_params)
            .header("authorization", self.get_authorization_header_value().await)
            .send()
            .await
    }

    pub async fn post_with_project_id_and_auth(
        &self,
        url: &str,
        with_project_id: bool,
        with_auth_header: bool,
    ) -> Result<reqwest::RequestBuilder, reqwest::Error> {
        let absolute_url: String = self.endpoint_url(url, with_project_id).await;
        info!("POST : {}", absolute_url);

        let request_builder = self.get_client().post(absolute_url);

        if with_auth_header {
            Ok(
                request_builder
                    .header("authorization", self.get_authorization_header_value().await),
            )
        } else {
            Ok(request_builder)
        }
    }

    pub async fn put_with_project_id_and_auth(
        &self,
        url: &str,
        with_project_id: bool,
        with_auth_header: bool,
    ) -> Result<reqwest::RequestBuilder, reqwest::Error> {
        let absolute_url: String = self.endpoint_url(url, with_project_id).await;
        info!("PUT : {}", absolute_url);

        let request_builder = self.get_client().put(absolute_url);

        if with_auth_header {
            Ok(
                request_builder
                    .header("authorization", self.get_authorization_header_value().await),
            )
        } else {
            Ok(request_builder)
        }
    }

    pub async fn get_with_project_id_and_auth(
        &self,
        url: &str,
        with_project_id: bool,
        with_auth_header: bool,
    ) -> Result<reqwest::RequestBuilder, reqwest::Error> {
        let absolute_url: String = self.endpoint_url(url, with_project_id).await;
        info!("GET : {}", absolute_url);

        let request_builder = self.get_client().get(absolute_url);

        if with_auth_header {
            Ok(
                request_builder
                    .header("authorization", self.get_authorization_header_value().await),
            )
        } else {
            Ok(request_builder)
        }
    }

    pub async fn send_post_form<T: Serialize + ?Sized>(
        &self,
        url: &str,
        form: &T,
        with_project_id: bool,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let absolute_url = self.endpoint_url(url, with_project_id).await;
        info!("POST : {}", absolute_url);
        self.get_client()
            .post(absolute_url)
            .header("authorization", self.get_authorization_header_value().await)
            .form(&form)
            .send()
            .await
    }

    pub async fn send_post_json<T: Serialize + ?Sized>(
        &self,
        url: &str,
        payload: &T,
        with_project_id: bool,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let absolute_url = self.endpoint_url(url, with_project_id).await;
        info!("POST : {}", absolute_url);
        self.get_client()
            .post(absolute_url)
            .header("authorization", self.get_authorization_header_value().await)
            .json(&payload)
            .send()
            .await
    }

    pub async fn send_empty_post(
        &self,
        url: &str,
        with_project_id: bool,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let absolute_url = self.endpoint_url(url, with_project_id).await;
        info!("POST : {}", absolute_url);
        self.get_client()
            .post(absolute_url)
            .header("authorization", self.get_authorization_header_value().await)
            .send()
            .await
    }

    pub async fn login_with_email_and_password(
        &self,
        email: &String,
        password: &String,
    ) -> Result<()> {
        let mut login_params = HashMap::new();
        login_params.insert("email", email);
        login_params.insert("password", password);

        let response = self
            .send_post_form("auth/login", &login_params, false)
            .await?;

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

    pub async fn set_api_key(&self, new_api_key: String) -> Result<()> {
        let header_key = HeaderValue::from_str(format!("ApiKey {}", new_api_key).as_str());

        if header_key.is_ok() {
            *self.get_api_key().lock().await = Some(header_key.unwrap());
            info!("A new API key has been saved.");
        } else {
            warn!("The provided Apikey is not valid : {}", new_api_key);
        }

        Ok(())
    }

    pub async fn set_project_id(&self, project_id: i32) -> Result<()> {
        *self.get_project_id().lock().await = project_id;

        Ok(())
    }

    pub async fn login(&self) -> Result<Project> {
        let email = env::var("HOPSWORKS_EMAIL").unwrap_or_default();
        let password = env::var("HOPSWORKS_PASSWORD").unwrap_or_default();
        let api_key = env::var("HOPSWORKS_API_KEY").unwrap_or_default();

        if email.len() > 1 && password.len() > 1 {
            self.login_with_email_and_password(&email, &password)
                .await?;
        } else if api_key.len() > 1 {
            self.set_api_key(api_key).await?;
        } else {
            panic!("Use a combination of email and password or an API key to authenticate.")
        }

        let project = self.get_the_project_or_default().await?;
        self.set_project_id(project.id).await?;

        write_locally_project_credentials_on_login(&project.project_name, &self.config.cert_dir)
            .await?;

        Ok(project)
    }

    async fn get_the_project_or_default(&self) -> Result<Project> {
        let projects: Vec<ProjectDTO> = get_project_list().await?;

        if projects.is_empty() {
            panic!("No project found for this user, please create a project in the UI first.");
        }

        let project_name: String = env::var("HOPSWORKS_PROJECT_NAME").unwrap_or_default();

        if project_name.is_empty() {
            Ok(Project::from(projects[0].clone()))
        } else {
            let project_match: Vec<&ProjectDTO> = projects
                .iter()
                .filter(|project| project.name == project_name)
                .collect();

            if project_match.is_empty() {
                panic!("No project with name {project_name} found for this user.");
            }

            Ok(Project::from(project_match[0].to_owned()))
        }
    }
}
