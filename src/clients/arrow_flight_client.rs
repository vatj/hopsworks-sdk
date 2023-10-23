use arrow_flight::{decode::FlightRecordBatchStream, Action, FlightClient, FlightDescriptor};
use bytes::Bytes;
use color_eyre::Result;
use futures::stream::{StreamExt, TryStreamExt};
use log::{debug, info};

use std::collections::HashMap;
use std::time::Duration;
use tonic::transport::{channel::ClientTlsConfig, Certificate, Endpoint, Identity};

use crate::{
    api::{
        feature_group::entities::{Feature, FeatureGroup},
        feature_view::entities::FeatureView,
        query::entities::{Query, QueryFilter, QueryFilterOrLogic, QueryLogic},
        training_dataset::entities::TrainingDataset,
    },
    get_hopsworks_client,
    repositories::{
        credentials::entities::RegisterArrowFlightClientCertificatePayload,
        query::payloads::{
            QueryArrowFlightPayload, QueryFilterArrowFlightPayload,
            QueryFilterOrLogicArrowFlightPayload, QueryLogicArrowFlightPayload,
        },
        storage_connector::payloads::FeatureGroupConnectorArrowFlightPayload,
        training_dataset::payloads::TrainingDatasetArrowFlightPayload,
        variables,
    },
    util,
};

#[derive(Debug, Clone, Default)]
pub struct HopsworksArrowFlightClientBuilder {}

impl HopsworksArrowFlightClientBuilder {
    pub fn new() -> Self {
        Self {}
    }

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

    pub async fn read_query(&mut self, query_payload: QueryArrowFlightPayload) -> Result<()> {
        info!("Arrow flight client read_query");
        debug!("Query payload: {:#?}", query_payload);
        let descriptor = FlightDescriptor::new_cmd(serde_json::to_string(&query_payload)?);
        self._get_dataset(descriptor).await?;
        Ok(())
    }

    pub async fn read_path(&mut self, path: &str) -> Result<()> {
        info!("Arrow flight client read_path: {}", path);
        let descriptor = FlightDescriptor::new_path(vec![path.to_string()]);
        self._get_dataset(descriptor).await?;
        Ok(())
    }

    async fn _get_dataset(&mut self, descriptor: FlightDescriptor) -> Result<()> {
        debug!("Getting dataset with descriptor: {:#?}", descriptor);
        let flight_info = self.client.get_flight_info(descriptor).await?;
        let opt_endpoint = flight_info.endpoint.get(0);

        if let Some(endpoint) = opt_endpoint {
            debug!("Endpoint: {:#?}", endpoint);
            if let Some(ticket) = endpoint.ticket.clone() {
                debug!("Ticket: {:#?}", ticket);
                let flight_data_stream = self.client.do_get(ticket).await?.into_inner();
                let mut record_batch_stream = FlightRecordBatchStream::new(flight_data_stream);

                // Read back RecordBatches
                while let Some(batch) = record_batch_stream.next().await {
                    match batch {
                        Ok(rec_batch) => {
                            info!("Record batch: {:#?}", rec_batch);
                        }
                        Err(_) => {
                            todo!()
                        }
                    };
                }
            } else {
                let flight_descriptor_cmd: String;
                if let Some(flight_descriptor) = flight_info.flight_descriptor {
                    flight_descriptor_cmd = std::str::from_utf8(&flight_descriptor.cmd)
                        .unwrap()
                        .to_string();
                } else {
                    flight_descriptor_cmd = "(No flight descriptor in flight info)".to_string();
                }
                return Err(color_eyre::Report::msg(format!(
                    "No ticket found in flight {} endpoint.",
                    flight_descriptor_cmd
                )));
            }
        } else {
            return Err(color_eyre::Report::msg("No endpoint found"));
        }
        Ok(())
    }

    pub async fn create_training_dataset(
        &mut self,
        feature_view_obj: FeatureView,
        training_dataset_obj: TrainingDataset,
        query_obj: Query,
    ) -> Result<()> {
        let training_dataset_payload = TrainingDatasetArrowFlightPayload::new(
            util::strip_feature_store_suffix(&training_dataset_obj.feature_store_name),
            feature_view_obj.name,
            feature_view_obj.version,
            training_dataset_obj.version,
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
            query.left_feature_group.name.clone()
        );
        let mut feature_names: HashMap<String, Vec<String>> = HashMap::new();
        let mut connectors: HashMap<String, FeatureGroupConnectorArrowFlightPayload> =
            HashMap::new();
        for feature_group in query.feature_groups() {
            let fg_name = self.serialize_feature_group_name(feature_group.clone());
            feature_names.insert(
                fg_name.clone(),
                feature_group
                    .get_features()
                    .iter()
                    .map(|feature| feature.name.clone())
                    .collect(),
            );
            let fg_connector = self.serialize_feature_group_connector(
                feature_group,
                query.clone(),
                on_demand_fg_aliases.clone(),
            )?;
            connectors.insert(fg_name, fg_connector);
        }
        let filters = self.serialize_filter_expression(query.filters(), query.clone(), false)?;
        Ok(QueryArrowFlightPayload::new(
            self.translate_to_duckdb(query.clone(), query_str)?,
            feature_names,
            Some(connectors),
            filters,
        ))
    }

    fn serialize_feature_group_connector(
        &self,
        _feature_group: FeatureGroup,
        _query: Query,
        _on_demand_fg_aliases: Vec<String>,
    ) -> Result<FeatureGroupConnectorArrowFlightPayload> {
        Ok(FeatureGroupConnectorArrowFlightPayload::new_hudi_connector())
    }

    fn serialize_feature_group_name(&self, feature_group: FeatureGroup) -> String {
        format!(
            "{}.{}_{}",
            feature_group.get_project_name(),
            feature_group.name,
            feature_group.version
        )
    }

    fn serialize_feature_name(
        &self,
        feature: Feature,
        query_obj: Query,
        short_name: bool,
    ) -> Result<String> {
        if short_name {
            debug!("Serializing short feature name: {}", feature.name);
            Ok(feature.name)
        } else {
            let opt_fg = query_obj.get_feature_group_by_feature(feature.clone());
            if let Some(fg) = opt_fg {
                let name = format!("{}.{}", self.serialize_feature_group_name(fg), feature.name);
                debug!("Serializing full feature name: {}", name);
                Ok(name)
            } else {
                Err(color_eyre::Report::msg(format!(
                    "Feature {} not found in query object",
                    feature.name
                )))
            }
        }
    }

    fn serialize_filter_expression(
        &self,
        filters: Vec<QueryFilterOrLogic>,
        query: Query,
        short_name: bool,
    ) -> Result<Option<Vec<QueryFilterOrLogicArrowFlightPayload>>> {
        debug!(
            "Serializing list of query filters and logic: {:#?}",
            filters
        );
        if filters.is_empty() {
            debug!("No filters found");
            return Ok(None);
        }
        let mut serialized_filters = vec![];
        for filter in filters {
            match filter {
                QueryFilterOrLogic::Filter(filter) => {
                    debug!("Found filter: {:#?}", filter);
                    serialized_filters.push(QueryFilterOrLogicArrowFlightPayload::Filter(
                        self.serialize_filter(filter, query.clone(), short_name)?,
                    ));
                }
                QueryFilterOrLogic::Logic(logic) => {
                    debug!("Found logic: {:#?}", logic);
                    serialized_filters.push(self.serialize_logic(
                        logic,
                        query.clone(),
                        short_name,
                    )?);
                }
            }
        }
        Ok(Some(serialized_filters))
    }

    fn serialize_filter(
        &self,
        filter: QueryFilter,
        query: Query,
        short_name: bool,
    ) -> Result<QueryFilterArrowFlightPayload> {
        debug!(
            "Serializing query filter: {:#?}, with short_name: {}",
            filter, short_name
        );
        Ok(QueryFilterArrowFlightPayload::new(
            filter.condition,
            filter.value,
            self.serialize_feature_name(filter.feature, query.clone(), short_name)?,
        ))
    }

    fn serialize_logic(
        &self,
        logic: QueryLogic,
        query: Query,
        short_name: bool,
    ) -> Result<QueryFilterOrLogicArrowFlightPayload> {
        debug!(
            "Serializing query logic: {:#?}, with short_name: {}",
            logic, short_name
        );
        let left_filter = self.serialize_filter_or_logic(
            logic.left_filter,
            logic.left_logic.as_deref().cloned(),
            query.clone(),
            short_name,
        )?;
        let right_filter = self.serialize_filter_or_logic(
            logic.right_filter,
            logic.right_logic.as_deref().cloned(),
            query.clone(),
            short_name,
        )?;
        Ok(QueryFilterOrLogicArrowFlightPayload::Logic(
            QueryLogicArrowFlightPayload::new(logic.logic_type, left_filter, right_filter),
        ))
    }

    fn serialize_filter_or_logic(
        &self,
        opt_filter: Option<QueryFilter>,
        opt_logic: Option<QueryLogic>,
        query: Query,
        short_name: bool,
    ) -> Result<Option<Box<QueryFilterOrLogicArrowFlightPayload>>> {
        debug!(
            "Serializing query filter or logic, with short_name: {}",
            short_name
        );
        if opt_filter.is_none() && opt_logic.is_none() {
            debug!("No filter or logic found");
            return Ok(None);
        }
        if let Some(filter) = opt_filter {
            debug!("Found filter: {:#?}", filter);
            return Ok(Some(Box::new(
                QueryFilterOrLogicArrowFlightPayload::Filter(
                    self.serialize_filter(filter, query, short_name)?,
                ),
            )));
        }
        debug!("Found logic: {:#?}", opt_logic);
        Ok(Some(Box::new(self.serialize_logic(
            opt_logic.unwrap(),
            query,
            short_name,
        )?)))
    }

    fn translate_to_duckdb(&self, query: Query, query_str: String) -> Result<String> {
        debug!("Translating query to duckdb sql style: {:#?}", query);
        Ok(query_str
            .replace(
                format!("`{}`.`", query.left_feature_group.featurestore_name).as_str(),
                format!("`{}.", query.left_feature_group.get_project_name()).as_str(),
            )
            .replace('`', '"'.to_string().as_str()))
    }
}
