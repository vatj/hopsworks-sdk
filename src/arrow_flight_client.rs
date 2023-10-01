use color_eyre::Result;
use arrow_flight::{FlightClient, Action};

use log::info;
use tonic::transport::{channel::ClientTlsConfig, Identity, Endpoint, Certificate};

use crate::{get_hopsworks_client, repositories::{variables, credentials::entities::RegisterArrowFlightClientCertificatePayload}};

#[derive(Debug, Clone, Default)]
pub struct HopsworksArrowFlightClientBuilder {}

impl HopsworksArrowFlightClientBuilder {
    pub fn new() -> Self {
        Self {}
    }

    async fn build_client_tls_config(&self, cert_dir: &str) -> Result<ClientTlsConfig> {
        let client_cert_content = Certificate::from_pem(tokio::fs::read_to_string(format!("{}/{}", cert_dir, "client_cert.pem")).await?);
        let client_key_content = Certificate::from_pem(tokio::fs::read_to_string(format!("{}/{}", cert_dir, "client_key.pem")).await?);
        let ca_chain_content = Certificate::from_pem(tokio::fs::read_to_string(format!("{}/{}", cert_dir, "ca_chain.pem")).await?);

        let identity = Identity::from_pem(client_cert_content, client_key_content);
        let tls_config = ClientTlsConfig::new()
            .domain_name("flyingduck.service.consul")
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

    async fn get_arrow_flight_url(&self) -> Result<String> {
        let load_balancer_url = variables::service::get_loadbalancer_external_domain().await?;
        let arrow_flight_url = format!("grpc+tls://{load_balancer_url}:5005");
        Ok(arrow_flight_url)
    }

    pub async fn build(self) -> Result<HopsworksArrowFlightClient> {
        self.check_flyingduck_enabled().await?;

        let hopsworks_client = get_hopsworks_client().await;
        let arrow_flight_url = self.get_arrow_flight_url().await?;
        
        let endpoint = Endpoint::from_shared(arrow_flight_url)?
            .tls_config(
                self.build_client_tls_config( 
                    hopsworks_client.cert_dir.as_str()
                ).await?)?;
        let channel = endpoint.connect().await?;

        let mut hopsworks_arrow_client = HopsworksArrowFlightClient {
            client: FlightClient::new(channel),
        };
        hopsworks_arrow_client.health_check().await?;
        hopsworks_arrow_client.register_certificates(hopsworks_client.cert_dir.as_str()).await?;

        Ok(hopsworks_arrow_client)
    }
}

#[derive(Debug)]
pub struct HopsworksArrowFlightClient {
    pub client: FlightClient,
}

impl HopsworksArrowFlightClient {
    async fn health_check(&mut self) -> Result<()> {
        info!("Health checking arrow flight client...");
        let _health_check = self.client.do_action(Action::new("healthcheck", "")).await?;
        info!("Arrow flight client health check successful.");
        Ok(())
    }

    async fn register_certificates(&mut self, cert_dir: &str) -> Result<()> {
        info!("Registering arrow flight client certificates...");
        let register_client_certificates_action = Action::new(
            "register-client-certificates",  
            serde_json::to_string(&RegisterArrowFlightClientCertificatePayload::new(
                tokio::fs::read_to_string(format!("{}/{}", cert_dir, "trust_store.jks")).await?, 
                tokio::fs::read_to_string(format!("{}/{}", cert_dir, "key_store.jks")).await?,
                tokio::fs::read_to_string(format!("{}/{}", cert_dir, "cert_key.pem")).await?
            ))?
        );
        self.client.do_action(register_client_certificates_action).await?;
        info!("Arrow flight client certificates registered.");
        Ok(())
    }



}
