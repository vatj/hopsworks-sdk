
use rdkafka::message::{Header, OwnedHeaders};

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
