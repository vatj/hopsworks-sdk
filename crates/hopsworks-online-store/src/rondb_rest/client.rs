use color_eyre::Result;
use tracing::{debug, info, warn};
use reqwest::{header::HeaderValue, Method};
use std::{path::Path, sync::Arc};
use tokio::sync::Mutex;

use crate::controller::platform::credentials::write_locally_project_credentials_on_login;
use crate::cluster_api::platform::project::{{ProjectAndUserDTO, ProjectDTO}, service::get_project_and_user_list};

pub const DEFAULT_CLIENT_URL: &str = "https://c.app.hopsworks.ai/hopsworks-api/api";
pub const DEFAULT_CLIENT_CERT_DIR: &str = "/tmp/";
pub const DEFAULT_ENV_HOPSWORKS_API_KEY: &str = "HOPSWORKS_API_KEY";
pub const DEFAULT_ENV_HOPSWORKS_PROJECT_NAME: &str = "HOPSWORKS_PROJECT_NAME";
pub const DEFAULT_ENV_HOPSWORKS_URL: &str = "HOPSWORKS_URL";

#[derive(Debug, Clone)]
pub struct RonDBClientBuilder {
    url: String,
    api_key: Option<String>,
    cert_dir: String,
    project_name: Option<String>,
}

impl Default for RonDBClientBuilder {
    fn default() -> Self {
        RonDBClientBuilder {
            url: DEFAULT_CLIENT_URL.to_string(),
            api_key: None,
            cert_dir: DEFAULT_CLIENT_CERT_DIR.to_string(),
            project_name: None,
        }
    }
}

impl RonDBClientBuilder {
    pub fn new() -> Self {
        RonDBClientBuilder::default()
    }

    /// Create a new RonDBClientBuilder using environment variables if available
    /// or the provided backup values.
    /// Note:
    ///   - The backup values are only used if the environment variables are not set.
    ///   - The project_name is optional and can be set to None,
    ///     it will default to the last used project for this user.
    pub fn new_from_env_or_provided(
        backup_api_key: &str,
        backup_url: &str,
        backup_project_name: Option<&str>,
    ) -> Self {
        let api_key = std::env::var(DEFAULT_ENV_HOPSWORKS_API_KEY)
            .ok()
            .or_else(|| Some(backup_api_key.to_string()));
        let url = std::env::var(DEFAULT_ENV_HOPSWORKS_URL).unwrap_or(backup_url.to_string());
        let project_name = std::env::var(DEFAULT_ENV_HOPSWORKS_PROJECT_NAME)
            .ok()
            .or_else(|| backup_project_name.map(|s| s.to_string()));

        debug!("RonDBClientBuilder: New client from environment variables.\n url: {}\n  api_key: {:?}\n  project_name: {:?}", url, api_key, project_name);

        RonDBClientBuilder {
            url,
            api_key,
            cert_dir: DEFAULT_CLIENT_CERT_DIR.to_string(),
            project_name,
        }
    }

    pub fn with_url(mut self, url: &str) -> Self {
        self.url = url.to_string();
        self
    }

    pub fn with_api_key(mut self, api_key: &str) -> Self {
        self.api_key = Some(api_key.to_string());
        self
    }

    pub fn with_cert_dir(mut self, cert_dir: &str) -> Self {
        self.cert_dir = cert_dir.to_string();
        self
    }

    pub fn with_project_name(mut self, project_name: &str) -> Self {
        self.project_name = Some(project_name.to_string());
        self
    }

    pub async fn build(self) -> Result<RonDBClient> {
        let api_key = std::env::var(DEFAULT_ENV_HOPSWORKS_API_KEY)
            .ok()
            .or(self.api_key);
        if api_key.is_none() {
            return Err(color_eyre::eyre::eyre!(
                "No API key provided. Provide an API key using the {} environment variable or the with_api_key() method of the RonDBClientBuilder.",
                DEFAULT_ENV_HOPSWORKS_API_KEY
            ));
        }

        debug!(
            "RonDBClientBuilder: Building client with url: {}",
            self.url.as_str()
        );
        let client = RonDBClient::new(self.url, self.cert_dir);

        client.set_api_key(api_key.as_deref()).await;
        Ok(client)
    }
}

#[derive(Debug, Clone)]
pub struct RonDBClient {
    client: reqwest::Client,
    pub(crate) url: String,
    pub(crate) cert_dir: Arc<Mutex<String>>,
    api_key: Arc<Mutex<Option<HeaderValue>>>,
    project_id: Arc<Mutex<Option<i32>>>,
    cert_key: Arc<Mutex<Option<String>>>,
    project_name: Arc<Mutex<Option<String>>>,
}

impl Default for RonDBClient {
    fn default() -> Self {
        RonDBClient {
            // client: reqwest::Client::new(),
            client: reqwest::ClientBuilder::new()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
            url: DEFAULT_CLIENT_URL.to_string(),
            cert_dir: Arc::new(Mutex::new(DEFAULT_CLIENT_CERT_DIR.to_string())),
            api_key: Arc::new(Mutex::new(None)),
            project_id: Arc::new(Mutex::new(None)),
            cert_key: Arc::new(Mutex::new(None)),
            project_name: Arc::new(Mutex::new(None)),
        }
    }
}

impl RonDBClient {
    fn new(url: String, cert_dir: String) -> Self {
        let mut client = RonDBClient::default();
        if !url.eq(DEFAULT_CLIENT_URL) {
            debug!(
                "RonDBClient: New client overrides default url with: {}",
                url
            );
            client.url = url;
        }
        if !cert_dir.eq(DEFAULT_CLIENT_CERT_DIR) {
            debug!(
                "RonDBClient: New client overrides default cert_dir with: {}",
                cert_dir
            );
            client.cert_dir = Arc::new(Mutex::new(cert_dir));
        }
        client
    }

    pub fn builder() -> RonDBClientBuilder {
        RonDBClientBuilder::new()
    }

    pub async fn login(&self) -> Result<ProjectDTO> {
        info!("Connecting to RonDB...");

        if self.get_api_key().lock().await.is_none() {
            panic!("Use an API key to authenticate. You can provide it via the {} environment variable or via RonDBClientBuilder.", DEFAULT_ENV_HOPSWORKS_API_KEY)
        }

        let project = self
            .get_the_project_or_default(self.get_project_name().lock().await.as_deref())
            .await?;
        self.set_project_id(Some(project.id)).await;
        info!(
            "Connected to RonDB project : {} at url {} !",
            project.name,
            self.url
        );

        let cert_dir = self.get_cert_dir().lock().await.clone();
        self.set_cert_dir(
            Path::new(cert_dir.as_str())
                .join(project.name.as_str())
                .to_str()
                .unwrap()
                .to_string(),
        )
        .await;

        let cert_key =
            write_locally_project_credentials_on_login(self.get_cert_dir().lock().await.as_str())
                .await?;
        self.set_cert_key(Some(cert_key)).await;

        Ok(project)
    }

    fn get_api_key(&self) -> Arc<Mutex<Option<HeaderValue>>> {
        Arc::clone(&self.api_key)
    }

    pub(crate) fn get_cert_key(&self) -> Arc<Mutex<Option<String>>> {
        Arc::clone(&self.cert_key)
    }

    pub fn get_project_id(&self) -> Arc<Mutex<Option<i32>>> {
        Arc::clone(&self.project_id)
    }

    pub fn get_project_name(&self) -> Arc<Mutex<Option<String>>> {
        Arc::clone(&self.project_name)
    }

    pub fn get_cert_dir(&self) -> Arc<Mutex<String>> {
        Arc::clone(&self.cert_dir)
    }

    async fn set_project_id(&self, project_id: Option<i32>) {
        debug!("Setting RonDBClient project id to {:?}", project_id);
        *self.get_project_id().lock().await = project_id;
    }

    async fn set_cert_key(&self, cert_key: Option<String>) {
        debug!("Setting RonDBClient cert_key");
        *self.get_cert_key().lock().await = cert_key;
    }

    async fn set_cert_dir(&self, cert_dir: String) {
        debug!("Setting RonDBClient cert_dir");
        *self.get_cert_dir().lock().await = cert_dir;
    }

    async fn set_api_key(&self, new_api_key: Option<&str>) {
        if new_api_key.is_none() {
            debug!("Removing RonDBClient API key.");
            *self.get_api_key().lock().await = None;
            return;
        }
        let header_key = HeaderValue::from_str(format!("ApiKey {}", new_api_key.unwrap()).as_str());

        if let Ok(header_key) = header_key {
            *self.get_api_key().lock().await = Some(header_key);
            info!("Setting RonDBClient api key for authenticated request.");
        } else {
            warn!(
                "The provided Apikey is not valid : {}",
                new_api_key.unwrap()
            );
        }
    }

    pub async fn request(
        &self,
        method: Method,
        url: &str,
        with_authorization: bool,
        with_project_id: bool,
    ) -> Result<reqwest::RequestBuilder> {
        let request_builder = self
            .client
            .request(method, self.endpoint_url(url, with_project_id).await?);
        if with_authorization {
            Ok(
                request_builder
                    .header("authorization", self.get_authorization_header_value().await),
            )
        } else {
            Ok(request_builder)
        }
    }

    async fn get_authorization_header_value(&self) -> HeaderValue {
        if self.get_api_key().lock().await.is_some() {
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

    async fn endpoint_url(&self, url: &str, with_project_id: bool) -> Result<String> {
        // Using the client's prefix in case it's a relative route.
        let full_url;
        if url.starts_with("https") || url.starts_with("http") {
            full_url = url.to_string();
        } else {
            // with_project_id only applies for relative url
            if with_project_id {
                let project_id = self
                    .get_project_id()
                    .lock()
                    .await
                    .expect("Project id not set, please login first.");

                full_url = format!("{}/project/{}/{url}", self.url.clone(), project_id,)
            } else {
                full_url = format!("{}/{}", self.url.clone(), url)
            }
        }
        debug!("RonDB Client Endpoint URL: {}", full_url);
        Ok(full_url)
    }

    async fn get_the_project_or_default(&self, project_name: Option<&str>) -> Result<ProjectDTO> {
        let projects: Vec<ProjectAndUserDTO> = get_project_and_user_list().await?;

        if projects.is_empty() {
            panic!("No project found for this user, please create a project in the UI first.");
        } else if project_name.is_none() {
             Ok(projects[0].project.to_owned())
        } else {
            let name = project_name.unwrap();

            let opt_match = projects
                .iter()
                .find(|project| project.project.name == name);

            if let Some(the_project) = opt_match {
                Ok(the_project.project.to_owned())
            } else {
                panic!("No project with name {} found for this user.", name);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use color_eyre::Result;

    #[tokio::test]
    async fn test_builder() {
        let builder = RonDBClientBuilder::new()
            .with_url(DEFAULT_CLIENT_URL)
            .with_api_key("api_key");

        // Verify that the builder constructs the client with the correct URL and API key.
        let client = builder.build().await.unwrap();
        assert_eq!(client.url, DEFAULT_CLIENT_URL);
        assert_eq!(
            client
                .get_api_key()
                .lock()
                .await
                .as_ref()
                .unwrap()
                .to_str()
                .unwrap(),
            "ApiKey api_key"
        );
    }

    #[tokio::test]
    async fn test_client() {
        let client = RonDBClient::new(
            DEFAULT_CLIENT_URL.to_string(),
            DEFAULT_CLIENT_CERT_DIR.to_string(),
        );
        client.set_api_key(Some("api_key")).await;
        client.set_project_id(Some(42)).await;

        // Test request building.
        let request_builder = client
            .request(Method::GET, "some_path", true, true)
            .await
            .unwrap();

        // Verify that the request builder includes the authorization header and project ID in the URL.
        let request = request_builder.build().unwrap();
        assert_eq!(
            request
                .headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "ApiKey api_key"
        );
        assert_eq!(
            request.url().path(),
            "/hopsworks-api/api/project/42/some_path"
        );
    }

    #[tokio::test]
    async fn test_set_api_key() -> Result<()> {
        let builder = RonDBClientBuilder::new().with_api_key("basic_api_key");
        let client = builder.build().await.unwrap();

        client.set_api_key(Some("valid_api_key")).await;
        assert!(client.get_api_key().lock().await.is_some());

        client.set_api_key(None).await; // Invalid key
        assert!(client.get_api_key().lock().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_set_project_id() {
        let client = RonDBClient::default();

        client.set_project_id(Some(42)).await;
        assert_eq!(
            *client.project_id.lock().await,
            Some(42),
            "Project ID should be set to Some(42)"
        );

        client.set_project_id(None).await;
        assert_eq!(
            *client.project_id.lock().await,
            None,
            "Project ID should be set to None"
        );
    }

    #[tokio::test]
    async fn test_request_with_authorization() -> Result<()> {
        let client = RonDBClient::default();
        client.set_api_key(Some("api_key")).await;
        client.set_project_id(Some(42)).await;

        let request = client
            .request(Method::GET, "/path", true, true)
            .await?
            .build()?;

        let headers = request.headers();
        assert!(
            headers.contains_key("authorization"),
            "Authorization header should be present"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_request_without_authorization() -> Result<()> {
        let client = RonDBClient::default();
        client.set_api_key(None).await;
        client.set_project_id(Some(42)).await;

        let request = client
            .request(Method::GET, "/path", false, true)
            .await?
            .build()?;

        let headers = request.headers();
        println!("{:#?}", headers);
        assert!(
            !headers.contains_key("authorization"),
            "Authorization header should not be present"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_endpoint_url_with_project_id() -> Result<()> {
        let client = RonDBClient::default();
        client.set_project_id(Some(42)).await;

        let endpoint_url = client.endpoint_url("path", true).await?;
        assert_eq!(
            endpoint_url,
            DEFAULT_CLIENT_URL.to_string() + "/project/42/path",
            "Endpoint URL should include project ID"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_endpoint_url_without_project_id() -> Result<()> {
        let client = RonDBClient::default();

        let endpoint_url = client.endpoint_url("path", false).await?;
        assert_eq!(
            endpoint_url,
            DEFAULT_CLIENT_URL.to_string() + "/path",
            "Endpoint URL should not include project ID"
        );

        Ok(())
    }

    #[tokio::test]
    #[should_panic(
        expected = "No API key provided. Provide an API key using the HOPSWORKS_API_KEY environment variable or the with_api_key() method."
    )]
    async fn test_default_builder() {
        let api_key = std::env::var(DEFAULT_ENV_HOPSWORKS_API_KEY).unwrap_or_default();
        std::env::remove_var(DEFAULT_ENV_HOPSWORKS_API_KEY);

        let builder = RonDBClientBuilder::new();

        let client = builder.build().await;

        std::env::set_var(DEFAULT_ENV_HOPSWORKS_API_KEY, api_key);

        // Unwrap Panic because the builder does not have an API key.
        client.unwrap();
    }
}
