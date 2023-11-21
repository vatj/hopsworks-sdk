use color_eyre::Result;
use log::debug;

use crate::{
    api::{
        feature_group::{feature::Feature, FeatureGroup},
        query::entities::{Query, QueryFilter, QueryFilterOrLogic, QueryLogic},
    },
    repositories::{
        query::payloads::{
            QueryFilterArrowFlightPayload, QueryFilterOrLogicArrowFlightPayload,
            QueryLogicArrowFlightPayload,
        },
        storage_connector::payloads::FeatureGroupConnectorArrowFlightPayload,
    },
};

pub(super) fn serialize_feature_group_connector(
    _feature_group: FeatureGroup,
    _query: Query,
    _on_demand_fg_aliases: Vec<String>,
) -> Result<FeatureGroupConnectorArrowFlightPayload> {
    Ok(FeatureGroupConnectorArrowFlightPayload::new_hudi_connector())
}

pub(super) fn serialize_feature_group_name(feature_group: FeatureGroup) -> String {
    format!(
        "{}.{}_{}",
        feature_group.get_project_name(),
        feature_group.name,
        feature_group.version
    )
}

pub(super) fn serialize_feature_name(
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
            let name = format!("{}.{}", serialize_feature_group_name(fg), feature.name);
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

pub(super) fn serialize_filter_expression(
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
                    serialize_filter(filter, query.clone(), short_name)?,
                ));
            }
            QueryFilterOrLogic::Logic(logic) => {
                debug!("Found logic: {:#?}", logic);
                serialized_filters.push(serialize_logic(logic, query.clone(), short_name)?);
            }
        }
    }
    Ok(Some(serialized_filters))
}

pub(super) fn serialize_filter(
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
        serialize_feature_name(filter.feature, query.clone(), short_name)?,
    ))
}

pub(super) fn serialize_logic(
    logic: QueryLogic,
    query: Query,
    short_name: bool,
) -> Result<QueryFilterOrLogicArrowFlightPayload> {
    debug!(
        "Serializing query logic: {:#?}, with short_name: {}",
        logic, short_name
    );
    let left_filter = serialize_filter_or_logic(
        logic.left_filter,
        logic.left_logic.as_deref().cloned(),
        query.clone(),
        short_name,
    )?;
    let right_filter = serialize_filter_or_logic(
        logic.right_filter,
        logic.right_logic.as_deref().cloned(),
        query.clone(),
        short_name,
    )?;
    Ok(QueryFilterOrLogicArrowFlightPayload::Logic(
        QueryLogicArrowFlightPayload::new(logic.logic_type, left_filter, right_filter),
    ))
}

pub(super) fn serialize_filter_or_logic(
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
            QueryFilterOrLogicArrowFlightPayload::Filter(serialize_filter(
                filter, query, short_name,
            )?),
        )));
    }
    debug!("Found logic: {:#?}", opt_logic);
    Ok(Some(Box::new(serialize_logic(
        opt_logic.unwrap(),
        query,
        short_name,
    )?)))
}

pub(super) fn translate_to_duckdb(query: Query, query_str: String) -> Result<String> {
    debug!("Translating query to duckdb sql style: {:#?}", query);
    Ok(query_str
        .replace(
            format!("`{}`.`", query.left_feature_group.featurestore_name).as_str(),
            format!("`{}.", query.left_feature_group.get_project_name()).as_str(),
        )
        .replace('`', '"'.to_string().as_str()))
}
