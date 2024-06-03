use arrow_flight::{Action, FlightClient, FlightDescriptor};
use bytes::Bytes;
use color_eyre::Result;
use futures::stream::{StreamExt, TryStreamExt};
use log::{debug, info};
use polars::prelude::DataFrame;
use std::collections::HashMap;
use std::time::Duration;
use tonic::transport::{channel::ClientTlsConfig, Certificate, Endpoint, Identity};

use crate::arrow_flight::{decoder, utils, payloads::{FeatureGroupConnectorArrowFlightPayload, TrainingDatasetArrowFlightPayload, QueryArrowFlightPayload}};
use hopsworks_core::{get_hopsworks_client, util};
use hopsworks_core::cluster_api::credentials::RegisterArrowFlightClientCertificatePayload;
use hopsworks_core::cluster_api::platform::variables;

#[derive(Debug, Clone, Default)]
pub struct HopsworksArrowFlightClientBuilder {}

impl HopsworksArrowFlightClientBuilder {
    async fn build_client_tls_config(&self, cert_dir: &str) -> Result<ClientTlsConfig> {
        debug!("my_cert_dir: {}/{}", cert_dir, "client_cert.pem");
        let client_cert_content =
            tokio::fs::read(format!("{}/{}", cert_dir, "client_cert.pem")).await?;
        let client_key_content =
            tokio::fs::read(format!("{}/{}", cert_dir, "client_key.pem")).await?;
        let ca_chain_content = Certificate::from_pem(
            tokio::fs::read(format!("{}/{}", cert_dir, "ca_chain.pem")).await?,
        );

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
        let load_balancer_url_from_env = std::env::var("HOPSWORKS_EXTERNAL_LOADBALANCER_URL");
        let arrow_flight_url = format!(
            "https://{}:5005",
            load_balancer_url_from_env.unwrap_or(load_balancer_url)
        );
        debug!("Arrow flight url: {}", arrow_flight_url);
        Ok(arrow_flight_url)
    }

    pub async fn build(self) -> Result<HopsworksArrowFlightClient> {
        self.check_flyingduck_enabled().await?;

        let hopsworks_client = get_hopsworks_client().await;
        let arrow_flight_url = self.get_arrow_flight_url().await?;

        let endpoint = Endpoint::from_shared(arrow_flight_url)?
            .tls_config(
                self.build_client_tls_config(hopsworks_client.get_cert_dir().lock().await.as_str())
                    .await?,
            )?
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(20))
            .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
            .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
            .http2_keep_alive_interval(Duration::from_secs(300))
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(true);

        debug!("Arrow flight endpoint: {:#?}", endpoint.uri().host());
        let channel = endpoint
            .connect()
            .await
            .expect("Tonic channel failed to connect to Arrow Flight server");

        let mut hopsworks_arrow_client = HopsworksArrowFlightClient {
            client: FlightClient::new(channel),
        };

        hopsworks_arrow_client
            .client
            .add_header("grpc-accept-encoding", "identity, deflate, gzip")?;
        info!(
            "flight client metadata : {:#?}",
            hopsworks_arrow_client.client.metadata()
        );

        hopsworks_arrow_client.health_check().await?;
        hopsworks_arrow_client
            .register_certificates(hopsworks_client.get_cert_dir().lock().await.as_str())
            .await?;

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
        let _health_check: Vec<Bytes> = self
            .client
            .do_action(Action::new("healthcheck", ""))
            .await?
            .try_collect()
            .await?;
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
                tokio::fs::read_to_string(format!("{}/{}", cert_dir, "material_passwd")).await?,
            ))?,
        );
        let _registration: Vec<Bytes> = self
            .client
            .do_action(register_client_certificates_action)
            .await?
            .try_collect()
            .await?;
        info!("Arrow flight client certificates registered.");
        Ok(())
    }

    pub async fn read_query(
        &mut self,
        query_payload: QueryArrowFlightPayload,
    ) -> Result<DataFrame> {
        info!("Arrow flight client read_query");
        debug!("Query payload: {:#?}", query_payload);
        let descriptor = FlightDescriptor::new_cmd(serde_json::to_string(&query_payload)?);
        let df = self._get_dataset(descriptor).await?;
        Ok(df)
    }

    pub async fn read_path(&mut self, path: &str) -> Result<DataFrame> {
        info!("Arrow flight client read_path: {}", path);
        let descriptor = FlightDescriptor::new_path(vec![path.to_string()]);
        let df = self._get_dataset(descriptor).await?;
        Ok(df)
    }

    async fn _get_dataset(&mut self, descriptor: FlightDescriptor) -> Result<DataFrame> {
        debug!("Getting dataset with descriptor: {:#?}", descriptor);
        let flight_info = self.client.get_flight_info(descriptor).await?;
        let opt_endpoint = flight_info.endpoint.get(0);

        if let Some(endpoint) = opt_endpoint {
            debug!("Endpoint: {:#?}", endpoint);
            if let Some(ticket) = endpoint.ticket.clone() {
                debug!("Ticket: {:#?}", ticket);

                let decoded = decoder::FlightDataDecoder::new(
                    self.client
                        .inner_mut()
                        .do_get(ticket.clone())
                        .await?
                        .into_inner(),
                );

                let mut df_stream = decoder::FlightDataFrameStream::new(decoded);

                if let Some(result_df) = df_stream.next().await {
                    match result_df {
                        Ok(df) => {
                            info!("Retrieved polars df: {:#?}", df.head(Some(5)));
                            return Ok(df);
                        }
                        Err(e) => return Err(color_eyre::Report::new(e)),
                    }
                }

                Err(color_eyre::Report::msg(
                    "No dataframe found in flight data stream",
                ))

                // Left here if we want to use the apache arrow implementation to get record batches
                // and then convert the record batch to a dataframe.
                //
                // let flight_data_stream = self.client.do_get(ticket).await?.into_inner();
                // let mut record_batch_stream = FlightRecordBatchStream::new(flight_data_stream);
                // // Read back RecordBatches
                // while let Some(batch) = record_batch_stream.next().await {
                //     // while let Some(batch) = flight_data_stream.next().await {
                //     match batch {
                //         Ok(rec_batch) => {
                //             info!("Record batch: {:#?}", rec_batch);
                //         }
                //         Err(_) => {
                //             todo!()
                //         }
                //     };
                // }
            } else {
                let flight_descriptor_cmd: String;
                if let Some(flight_descriptor) = flight_info.flight_descriptor {
                    flight_descriptor_cmd = std::str::from_utf8(&flight_descriptor.cmd)
                        .unwrap()
                        .to_string();
                } else {
                    flight_descriptor_cmd = "(No flight descriptor in flight info)".to_string();
                }

                Err(color_eyre::Report::msg(format!(
                    "No ticket found in flight {} endpoint.",
                    flight_descriptor_cmd
                )))
            }
        } else {
            Err(color_eyre::Report::msg("No endpoint found"))
        }
    }

    pub async fn create_training_dataset(
        &mut self,
        feature_view_obj: FeatureView,
        training_dataset_obj: TrainingDataset,
        query_obj: Query,
    ) -> Result<()> {
        let training_dataset_payload = TrainingDatasetArrowFlightPayload::new(
            util::strip_feature_store_suffix(training_dataset_obj.feature_store_name()),
            feature_view_obj.name().to_string(),
            feature_view_obj.version(),
            training_dataset_obj.version(),
            serde_json::to_string(&query_obj)?,
        );

        let action = Action::new(
            "create-training-dataset",
            serde_json::to_string(&training_dataset_payload)?,
        );
        let mut result = self.client.do_action(action).await?;
        while let Some(batch) = result.next().await {
            match batch {
                Ok(_) => {
                    todo!()
                }
                Err(_) => {
                    todo!()
                }
            };
        }
        Ok(())
    }

    pub fn create_query_object(
        &self,
        query: Query,
        query_str: String,
        on_demand_fg_aliases: Vec<String>,
    ) -> Result<QueryArrowFlightPayload> {
        info!(
            "Creating arrow flight query payload for query with left_feature_group {}",
            query.left_feature_group().name()
        );
        let mut feature_names: HashMap<String, Vec<String>> = HashMap::new();
        let mut connectors: HashMap<String, FeatureGroupConnectorArrowFlightPayload> =
            HashMap::new();
        for feature_group in query.feature_groups() {
            let fg_name = utils::serialize_feature_group_name(feature_group);
            feature_names.insert(
                fg_name.clone(),
                feature_group
                    .features()
                    .iter()
                    .map(|feature| feature.name().to_string())
                    .collect(),
            );
            let fg_connector = utils::serialize_feature_group_connector(
                feature_group,
                &query,
                on_demand_fg_aliases.clone(),
            )?;
            connectors.insert(fg_name, fg_connector);
        }
        let filters = match query.filters() {
            Some(filters) => utils::serialize_filter_expression(filters.clone(), &query, false)?,
            None => None,
        };
        Ok(QueryArrowFlightPayload::new(
            utils::translate_to_duckdb(&query, query_str)?,
            feature_names,
            Some(connectors),
            filters,
        ))
    }
}
