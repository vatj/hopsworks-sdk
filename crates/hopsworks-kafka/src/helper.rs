use color_eyre::Result;
use rdkafka::{message::{Header, OwnedHeaders}, producer::FutureRecord};

pub(crate) fn make_future_record_from_encoded<'a>(
    composite_key: &'a str,
    encoded_payload: &'a Vec<u8>,
    topic_name: &'a str,
    headers: OwnedHeaders,
) -> Result<FutureRecord<'a, str, Vec<u8>>> {
    Ok(FutureRecord::to(topic_name)
        .payload(encoded_payload)
        .key(composite_key)
        .headers(headers))
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

#[cfg(test)]
mod tests {
    use rdkafka::message::{Header, Headers};

    use super::*;
    
    #[tokio::test]
    async fn test_make_future_record_from_encoded() {
        // Arrange
        let composite_key = "some_key";
        let encoded_payload = vec![1, 2, 3];
        let topic_name = "test_topic";
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "test_key",
                value: Some("test_value".as_bytes()),
            });

        // Act
        let result = make_future_record_from_encoded(
            composite_key,
            &encoded_payload,
            topic_name,
            headers,
        );

        // Assert
        assert!(result.is_ok());

        let future_record = result.unwrap();

        assert_eq!(future_record.key, Some(composite_key));
        assert_eq!(future_record.payload, Some(&encoded_payload));
        assert_eq!(future_record.topic, topic_name);

        assert!(future_record.headers.is_some());
        let headers = future_record
            .headers;
        assert!(headers.is_some());
        let headers = headers.unwrap();
        let single_header = headers.get(0);
        assert!(single_header.key == "test_key");
        assert!(single_header.value == Some("test_value".as_bytes()));
    }
}