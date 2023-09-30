use color_eyre::Result;
use arrow_flight::FlightClient;

use tonic::transport::{channel::ClientTlsConfig, Identity, Endpoint, Certificate};

use crate::{get_hopsworks_client, repositories::variables};

#[derive(Debug, Clone, Default)]
pub struct HopsworksArrowFlightClientBuilder {}

impl HopsworksArrowFlightClientBuilder {
    pub fn new() -> Self {
        Self {}
    }

    async fn build_client_tls_config(&self, url:&str, cert_dir: &str) -> Result<ClientTlsConfig> {
        let client_cert_content = Certificate::from_pem(tokio::fs::read_to_string(format!("{}/{}", cert_dir, "client_cert.pem")).await?);
        let client_key_content = Certificate::from_pem(tokio::fs::read_to_string(format!("{}/{}", cert_dir, "client_key.pem")).await?);
        let ca_chain_content = Certificate::from_pem(tokio::fs::read_to_string(format!("{}/{}", cert_dir, "ca_chain.pem")).await?);

        let identity = Identity::from_pem(client_cert_content, client_key_content);
        let tls_config = ClientTlsConfig::new()
            .domain_name(url)
            .ca_certificate(ca_chain_content)
            .identity(identity);

        Ok(tls_config)
    }

    async fn check_flyingduck_enabled(&self) -> Result<()> {
        let is_enabled = variables::service::get_flyingduck_enabled().await?;
        if !is_enabled {
            return Err(color_eyre::Report::msg("Flying Duck is not enabled"));
        }
        Ok(())
    }

    pub async fn build(self) -> Result<HopsworksArrowFlightClient> {
        self.check_flyingduck_enabled().await?;

        let hopsworks_client = get_hopsworks_client().await;
        let arrow_flight_url = format!("{}:{}", hopsworks_client.url.as_str(), "5005");
        
        let endpoint = Endpoint::from_shared(arrow_flight_url)?
            .tls_config(
                self.build_client_tls_config(
                    hopsworks_client.url.as_str(), 
                    hopsworks_client.cert_dir.as_str()
                ).await?)?;
        let channel = endpoint.connect().await?;

        let client = FlightClient::new(channel);

        Ok(HopsworksArrowFlightClient {
            client,
        })
    }
}

#[derive(Debug)]
pub struct HopsworksArrowFlightClient {
    pub client: FlightClient,
}