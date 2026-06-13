//! Metadata handler (API Key 3) — maps pqueue queues to Kafka topics.
//!
//! Each queue is exposed as a single-partition topic. The broker is always
//! self (node_id=0, host=127.0.0.1) since pqueue-kafka is single-node.

use bytes::Bytes;
use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{BrokerId, MetadataResponse, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};

pub struct BrokerMeta {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub cluster_id: String,
}

/// Build a Metadata response from a list of (topic_name) strings.
pub fn handle(
    api_version: i16,
    body: &[u8],
    queues: &[String],
    broker: &BrokerMeta,
) -> MetadataResponse {
    let request = if body.is_empty() {
        MetadataRequest::default()
    } else {
        let mut buf = Bytes::copy_from_slice(body);
        MetadataRequest::decode(&mut buf, api_version).unwrap_or_default()
    };

    let mut response = MetadataResponse::default();

    let mut b = MetadataResponseBroker::default();
    b.node_id = BrokerId(broker.node_id);
    b.host = StrBytes::from_string(broker.host.clone());
    b.port = broker.port;
    response.brokers.push(b);

    if api_version >= 2 {
        response.cluster_id = Some(StrBytes::from_string(broker.cluster_id.clone()));
    }
    if api_version >= 1 {
        response.controller_id = BrokerId(broker.node_id);
    }

    // Determine which topics to include.
    let requested: Option<Vec<String>> = match &request.topics {
        None => None,
        Some(ts) if ts.is_empty() => None,
        Some(ts) => Some(
            ts.iter()
                .filter_map(|t| t.name.as_ref().map(|n| n.0.to_string()))
                .collect(),
        ),
    };

    let topics_to_include: Vec<&String> = match &requested {
        None => queues.iter().collect(),
        Some(names) => queues.iter().filter(|q| names.contains(q)).collect(),
    };

    for name in topics_to_include {
        let mut topic = MetadataResponseTopic::default();
        topic.name = Some(TopicName(StrBytes::from_string(name.clone())));
        topic.error_code = 0;

        let mut partition = MetadataResponsePartition::default();
        partition.partition_index = 0;
        partition.error_code = 0;
        partition.leader_id = BrokerId(broker.node_id);
        partition.replica_nodes = vec![BrokerId(broker.node_id)];
        partition.isr_nodes = vec![BrokerId(broker.node_id)];
        topic.partitions.push(partition);

        response.topics.push(topic);
    }

    response
}
