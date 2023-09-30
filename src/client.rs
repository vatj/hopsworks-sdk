use color_eyre::Result;
use log::{debug, info, warn};
use reqwest::{header::HeaderValue, Method};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    api::project::entities::Project,
    domain::{
        credentials::controller::write_locally_project_credentials_on_login,
        project::controller::get_project_list,
    },
    repositories::project::entities::ProjectDTO,
};

pub const DEFAULT_CLIENT_URL: &str = "https://c.app.hopsworks.ai/hopsworks-api/api";
pub const DEFAULT_CLIENT_CERT_DIR: &str = "/tmp/";
pub const DEFAULT_ENV_HOPSWORKS_API_KEY: &str = "HOPSWORKS_API_KEY";

#[derive(Debug, Clone)]
pub struct HopsworksClientBuilder {
    url: String,
    api_key: Option<String>,
    cert_dir: String,
}

impl Default for HopsworksClientBuilder {
    fn default() -> Self {
        HopsworksClientBuilder {
            url: DEFAULT_CLIENT_URL.to_string(),
            api_key: None,
            cert_dir: DEFAULT_CLIENT_CERT_DIR.to_string(),
        }
    }
}

impl HopsworksClientBuilder {
    pub fn new() -> Self {
        HopsworksClientBuilder::default()
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

    pub async fn build(self) -> Result<HopsworksClient> {
        let api_key = std::env::var(DEFAULT_ENV_HOPSWORKS_API_KEY).unwrap_or_default();
        if self.api_key.is_none() && std::env::var(DEFAULT_ENV_HOPSWORKS_API_KEY).is_err() {
            return Err(color_eyre::eyre::eyre!(
                "No API key provided. Provide an API key using the HOPSWORKS_API_KEY environment variable or the with_api_key() method."
            ));
        }

        let client = HopsworksClient::new(self.url, self.cert_dir);
        client
            .set_api_key(Some(self.api_key.unwrap_or(api_key).as_str()))
            .await;
        Ok(client)
    }
}

#[derive(Debug, Clone)]
pub struct HopsworksClient {
    client: reqwest::Client,
    pub(crate) url: String,
    pub(crate) cert_dir: String,
    api_key: Arc<Mutex<Option<HeaderValue>>>,
    project_id: Arc<Mutex<Option<i32>>>,
}

impl Default for HopsworksClient {
    fn default() -> Self {
        HopsworksClient {
            client: reqwest::Client::new(),
            url: DEFAULT_CLIENT_URL.to_string(),
            cert_dir: DEFAULT_CLIENT_CERT_DIR.to_string(),
            api_key: Arc::new(Mutex::new(None)),
            project_id: Arc::new(Mutex::new(None)),
        }
    }
}

impl HopsworksClient {
    fn new(url: String, cert_dir: String) -> Self {
        let mut client = HopsworksClient::default();
        if !url.eq(DEFAULT_CLIENT_URL) {
            debug!(
                "HopsworksClient: New client overrides default url with: {}",
                url
            );
            client.url = url;
        }
        if !cert_dir.eq(DEFAULT_CLIENT_CERT_DIR) {
            debug!(
                "HopsworksClient: New client overrides default cert_dir with: {}",
                cert_dir
            );
            client.cert_dir = cert_dir;
        }
        client
    }

    pub fn builder() -> HopsworksClientBuilder {
        HopsworksClientBuilder::new()
    }

    pub async fn login(&self) -> Result<Project> {
        info!("Connecting to Hopsworks...");

        if self.get_api_key().lock().await.is_none() {
            panic!("Use an API key to authenticate.")
        }

        let project = self.get_the_project_or_default().await?;
        self.set_project_id(Some(project.id)).await;
        info!(
            "Connected to Hopsworks project : {} at url {} !",
            project.project_name, self.url
        );

        write_locally_project_credentials_on_login(&project.project_name, self.cert_dir.as_str())
            .await?;

        Ok(project)
    }

    fn get_api_key(&self) -> Arc<Mutex<Option<HeaderValue>>> {
        Arc::clone(&self.api_key)
    }

    fn get_project_id(&self) -> Arc<Mutex<Option<i32>>> {
        Arc::clone(&self.project_id)
    }

    async fn set_project_id(&self, project_id: Option<i32>) {
        debug!("Setting HopsworksClient project id to {:?}", project_id);
        *self.get_project_id().lock().await = project_id;
    }

    async fn set_api_key(&self, new_api_key: Option<&str>) {
        if new_api_key.is_none() {
            debug!("Removing HopsworksClient API key.");
            *self.get_api_key().lock().await = None;
            return;
        }
        let header_key = HeaderValue::from_str(format!("ApiKey {}", new_api_key.unwrap()).as_str());

        if header_key.is_ok() {
            *self.get_api_key().lock().await = Some(header_key.unwrap());
            info!("Setting HopsworksClient api key for authenticated request.");
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
        debug!("Hopsworks Client Endpoint URL: {}", full_url);
        Ok(full_url)
    }

    async fn get_the_project_or_default(&self) -> Result<Project> {
        let projects: Vec<ProjectDTO> = get_project_list().await?;

        if projects.is_empty() {
            panic!("No project found for this user, please create a project in the UI first.");
        }

        let project_name: String = std::env::var("HOPSWORKS_PROJECT_NAME").unwrap_or_default();

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

#[cfg(test)]
mod tests {
    use super::*;
    use color_eyre::Result;

    #[tokio::test]
    async fn test_builder() {
        let builder = HopsworksClientBuilder::new()
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
        let client = HopsworksClient::new(
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
        let builder = HopsworksClientBuilder::new().with_api_key("basic_api_key");
        let client = builder.build().await.unwrap();

        client.set_api_key(Some("valid_api_key")).await;
        assert!(client.get_api_key().lock().await.is_some());

        client.set_api_key(None).await; // Invalid key
        assert!(client.get_api_key().lock().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_set_project_id() {
        let client = HopsworksClient::default();

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
        let client = HopsworksClient::default();
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
        let client = HopsworksClient::default();
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
        let client = HopsworksClient::default();
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
        let client = HopsworksClient::default();

        let endpoint_url = client.endpoint_url("path", false).await?;
        assert_eq!(
            endpoint_url,
            DEFAULT_CLIENT_URL.to_string() + "/path",
            "Endpoint URL should not include project ID"
        );

        Ok(())
    }

    // Multi-thread test to bypass env variable unset side-effects
    #[tokio::test(flavor = "multi_thread")]
    #[should_panic(
        expected = "No API key provided. Provide an API key using the HOPSWORKS_API_KEY environment variable or the with_api_key() method."
    )]
    async fn test_default_builder() {
        std::env::remove_var(DEFAULT_ENV_HOPSWORKS_API_KEY);

        let builder = HopsworksClientBuilder::new();

        // Unwrap Panic because the builder does not have an API key.
        builder.build().await.unwrap();
    }
}
