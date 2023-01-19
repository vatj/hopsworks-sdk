use color_eyre::Result;

use crate::{
    api::query::entities::Query,
    repositories::{
        query,
        query::{entities::FeatureStoreQueryDTO, payloads::NewQueryPayload},
    },
};

pub async fn construct_query(query: Query) -> Result<FeatureStoreQueryDTO> {
    let query_payload = NewQueryPayload::from(query);

    Ok(query::service::construct_query(query_payload).await?)
}
