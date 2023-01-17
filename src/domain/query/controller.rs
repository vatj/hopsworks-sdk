use color_eyre::Result;

use crate::{
    api::query::entities::Query,
    repositories::{query, query::payloads::NewQueryPayload},
};

pub async fn construct_query(query: Query) -> Result<()> {
    let query_payload = NewQueryPayload::from(query);

    query::service::construct_query(query_payload).await?;
    Ok(())
}
