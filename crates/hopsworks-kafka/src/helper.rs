use rdkafka::{message::{Header, OwnedHeaders}, producer::FutureRecord};

pub(crate) fn make_future_record_from_encoded<'a>(
    composite_key: &'a str,
    encoded_payload: &'a Vec<u8>,
    topic_name: &'a str,
    headers: OwnedHeaders,
) -> FutureRecord<'a, str, Vec<u8>> {
    FutureRecord::to(topic_name)
        .payload(encoded_payload)
        .key(composite_key)
        .headers(headers)
}

pub(crate) fn make_custom_headers(
    project_id: i32,
    feature_group_id: i32,
    subject_id: i32,
    version: i32,
) -> OwnedHeaders {
    OwnedHeaders::new()
        .insert(Header {
            key: "version",
            value: Some(&version.to_string()),
        })
        .insert(Header {
            key: "projectId",
            value: Some(&project_id.to_string()),
        })
        .insert(Header {
            key: "featureGroupId",
            value: Some(&feature_group_id.to_string()),
        })
        .insert(Header {
            key: "subjectId",
            value: Some(&subject_id.to_string()),
        })
}