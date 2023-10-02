use color_eyre::Result;
use arrow_flight::{FlightClient, Action, FlightDescriptor, decode::FlightRecordBatchStream};
use futures::stream::StreamExt;
use log::info;
use tonic::transport::{channel::ClientTlsConfig, Identity, Endpoint, Certificate};

use crate::{get_hopsworks_client, repositories::{variables, credentials::entities::RegisterArrowFlightClientCertificatePayload, training_datasets::payloads::TrainingDatasetArrowFlightPayload}, api::{feature_view::entities::FeatureView, training_dataset::entities::TrainingDataset, query::entities::Query}};

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

    pub async fn read_query(&mut self, query_object: &str) -> Result<()> {
        let descriptor = FlightDescriptor::new_cmd(query_object.to_string());
        self._get_dataset(descriptor).await?;
        Ok(())
    }

    pub async fn read_path(&mut self, path: &str) -> Result<()> {
        let descriptor = FlightDescriptor::new_path(vec![path.to_string()]);
        self._get_dataset(descriptor).await?;
        Ok(())
    }

    async fn _get_dataset(&mut self, descriptor: FlightDescriptor) -> Result<()> {
        let flight_info = self.client.get_flight_info(descriptor).await?;
        let opt_endpoint = flight_info.endpoint.get(0);

        if let Some(endpoint) = opt_endpoint {
            if let Some(ticket) = endpoint.ticket.clone() {
                let flight_data_stream = self.client.do_get(ticket).await?.into_inner();
                let mut record_batch_stream = FlightRecordBatchStream::new(
                    flight_data_stream
                );
                
                // Read back RecordBatches
                while let Some(batch) = record_batch_stream.next().await {
                    match batch {
                    Ok(_) => { todo!() },
                    Err(_) => { todo!() },
                    };
                }
            } else {
                let flight_descriptor_cmd: String;
                if let Some(flight_descriptor) = flight_info.flight_descriptor {
                    flight_descriptor_cmd = std::str::from_utf8(&flight_descriptor.cmd).unwrap().to_string();
                } else {
                    flight_descriptor_cmd = "(No flight descriptor in flight info)".to_string();
                }
                return Err(color_eyre::Report::msg(format!("No ticket found in flight {} endpoint.", flight_descriptor_cmd)));
            }
        } else {
            return Err(color_eyre::Report::msg("No endpoint found"));
        }
        Ok(())
    }

    pub async fn create_training_dataset(&mut self, feature_view_obj: FeatureView, training_dataset_obj: TrainingDataset, query_obj: Query) -> Result<()> {
        let training_dataset_payload = TrainingDatasetArrowFlightPayload::new(
            training_dataset_obj.feature_store_name,
            feature_view_obj.name,
            feature_view_obj.version,
            training_dataset_obj.version,
            serde_json::to_string(&query_obj)?
        );

        let action = Action::new("create-training-dataset", serde_json::to_string(&training_dataset_payload)?);
        let mut result = self.client.do_action(action).await?;
        while let Some(batch) = result.next().await {
            match batch {
            Ok(_) => { todo!() },
            Err(_) => { todo!() },
            };
        }
        Ok(())
    }


}
