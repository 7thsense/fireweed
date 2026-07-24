//! Redis-Cluster slot routing + CLUSTER bootstrap (TD-006 §1A). So a STOCK cluster-aware client bootstraps
//! against pqueue and computes the SAME slot the server does, this implements the exact Redis algorithm:
//! `slot = crc16(hashtag(key)) % 16384`, with the standard hash-tag rule (`{...}` selects the substring to
//! hash). The canonical per-queue routing key is `{tenant/queue}` (TD-006), a hash-tag so co-locating a
//! tenant's queues is possible and the client's key→slot matches the server's.
//!
//! SCOPE (BQ-30 — do not overstate): this delivers the slot computation + the CLUSTER bootstrap REPLIES for
//! a SINGLE-NODE cluster (this node advertises itself owning all 16384 slots, so a stock client bootstraps
//! and every key routes locally). The MULTI-node slot→owner view and the per-queue `-MOVED` redirect to the
//! recorded `active_owner` are BQ-31 + the server-runtime follow-up — [`queue_slot`] is provided for BQ-31
//! to compute a redirect's slot. NOTE: the current wire key form is `tenant:queue` (colon); a `-MOVED`
//! redirect must compute the slot from the LITERAL key the client sent (via [`hash_slot`]) so it matches the
//! client, OR the wire form migrates to the `{tenant/queue}` hash-tag — reconciling that is BQ-31's concern
//! (single-node bootstrap is unaffected: all slots are local).

use fireweed_engine::QueueKey;

use crate::Resp;

/// The Redis cluster slot space.
pub const SLOT_COUNT: u16 = 16384;

/// CRC16-CCITT / XMODEM lookup table (poly 0x1021, init 0, no reflection) — the exact table Redis uses.
const fn crc16_table() -> [u16; 256] {
    let mut table = [0u16; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC16_TABLE: [u16; 256] = crc16_table();

/// Redis CRC16 of `buf` (the same function Redis cluster clients use). `crc16("123456789") == 0x31C3`.
pub fn crc16(buf: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in buf {
        crc = (crc << 8) ^ CRC16_TABLE[(((crc >> 8) ^ b as u16) & 0xFF) as usize];
    }
    crc
}

/// The Redis hash-tag rule: if `key` contains `{`, then a non-empty `}` after it, hash only the substring
/// BETWEEN them; otherwise hash the whole key (matches Redis `keyHashSlot` exactly, incl. the empty-tag and
/// no-close-brace fallbacks).
fn hashtag(key: &[u8]) -> &[u8] {
    let Some(open) = key.iter().position(|&c| c == b'{') else {
        return key; // no '{' → whole key
    };
    // Find the first '}' AFTER the '{'.
    match key[open + 1..].iter().position(|&c| c == b'}') {
        // '{...}' with NON-empty content → hash the content.
        Some(rel) if rel > 0 => &key[open + 1..open + 1 + rel],
        // '{}' (empty) or no '}' → whole key.
        _ => key,
    }
}

/// The cluster slot for `key` (Redis `keyHashSlot`): `crc16(hashtag(key)) % 16384`. A stock cluster client
/// computes this from the same key bytes, so the server's routing matches the client's.
pub fn hash_slot(key: &[u8]) -> u16 {
    crc16(hashtag(key)) % SLOT_COUNT
}

/// The canonical TD-006 routing key for a queue: `{tenant/queue}` (a hash-tag).
pub fn queue_routing_key(shard: &QueueKey) -> String {
    format!(
        "{{{}/{}}}",
        shard.tenant_id.as_str(),
        shard.queue_id.as_str()
    )
}

/// The cluster slot for a queue's CANONICAL [`queue_routing_key`] (`{tenant/queue}`) — i.e.
/// `crc16("tenant/queue") % 16384`. This is the slot a client gets ONLY if it addresses the queue with the
/// hash-tagged key form.
///
/// NOT used for `-MOVED`: BQ-31 resolved the wire-key tension by always echoing [`hash_slot`] of the LITERAL
/// key the client sent (so the redirect slot matches whatever key form the client used — colon or
/// hash-tag — and never loops). There is no planned migration of the wire form. `queue_slot` is retained for
/// advertising / for a deployment that adopts the `{tenant/queue}` key form; routing does not call it.
pub fn queue_slot(shard: &QueueKey) -> u16 {
    hash_slot(queue_routing_key(shard).as_bytes())
}

/// A cluster node's advertised identity (the endpoint a redirected client connects to + its stable id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    /// A 40-hex Redis-style node id (stable per endpoint).
    pub id: String,
    pub host: String,
    pub port: u16,
}

impl ClusterNode {
    /// Build a node identity for `host:port`, deriving a stable 40-hex id from the endpoint (no RNG — the id
    /// is reproducible across restarts for the same advertised endpoint).
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let host = host.into();
        let id = stable_node_id(&host, port);
        ClusterNode { id, host, port }
    }
}

/// A deterministic 40-lowercase-hex node id from the endpoint (5 FNV-1a rounds × 8 hex = 40 hex). Stable +
/// well-distributed; not cryptographic (a node id only needs to be unique + stable, like Redis's random id).
fn stable_node_id(host: &str, port: u16) -> String {
    let base = format!("{host}:{port}");
    let mut id = String::with_capacity(40);
    for salt in 0u8..5 {
        let mut h: u32 = 0x811c_9dc5;
        h = h.wrapping_mul(0x0100_0193) ^ salt as u32;
        for b in base.bytes() {
            h ^= b as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
        id.push_str(&format!("{h:08x}"));
    }
    id
}

/// `CLUSTER SLOTS` reply for a SINGLE-NODE cluster: one slot range `0..=16383` served by `node` (no
/// replicas). Shape (Redis): `[[start, end, [host, port, id]], ...]`.
pub(crate) fn cluster_slots_single_node(node: &ClusterNode) -> Resp {
    Resp::Array(vec![Resp::Array(vec![
        Resp::Int(0),
        Resp::Int((SLOT_COUNT - 1) as i64),
        Resp::Array(vec![
            Resp::Bulk(node.host.clone().into_bytes()),
            Resp::Int(node.port as i64),
            Resp::Bulk(node.id.clone().into_bytes()),
        ]),
    ])])
}

/// `CLUSTER SHARDS` reply for a SINGLE-NODE cluster: one shard owning slots `0..=16383` with one master
/// node. Shape (Redis 7+): `[["slots", [start, end], "nodes", [[id..., role master, ...]]], ...]`.
pub(crate) fn cluster_shards_single_node(node: &ClusterNode) -> Resp {
    let node_map = Resp::Array(vec![
        Resp::Bulk(b"id".to_vec()),
        Resp::Bulk(node.id.clone().into_bytes()),
        Resp::Bulk(b"port".to_vec()),
        Resp::Int(node.port as i64),
        Resp::Bulk(b"ip".to_vec()),
        Resp::Bulk(node.host.clone().into_bytes()),
        Resp::Bulk(b"endpoint".to_vec()),
        Resp::Bulk(node.host.clone().into_bytes()),
        Resp::Bulk(b"role".to_vec()),
        Resp::Bulk(b"master".to_vec()),
        Resp::Bulk(b"replication-offset".to_vec()),
        Resp::Int(0),
        Resp::Bulk(b"health".to_vec()),
        Resp::Bulk(b"online".to_vec()),
    ]);
    Resp::Array(vec![Resp::Array(vec![
        Resp::Bulk(b"slots".to_vec()),
        Resp::Array(vec![Resp::Int(0), Resp::Int((SLOT_COUNT - 1) as i64)]),
        Resp::Bulk(b"nodes".to_vec()),
        Resp::Array(vec![node_map]),
    ])])
}

/// `CLUSTER NODES` reply line for the single node: the canonical Redis nodes-file format, one master line
/// serving all slots, flagged `myself,master`.
pub(crate) fn cluster_nodes_single_node(node: &ClusterNode) -> Resp {
    // <id> <ip:port@cport> myself,master - 0 0 0 connected 0-16383
    let line = format!(
        "{} {}:{}@{} myself,master - 0 0 0 connected 0-{}\n",
        node.id,
        node.host,
        node.port,
        node.port as u32 + 10000,
        SLOT_COUNT - 1
    );
    Resp::Bulk(line.into_bytes())
}

/// `CLUSTER INFO` reply: a healthy single-node cluster owning all slots.
pub(crate) fn cluster_info_single_node() -> Resp {
    let body = "cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:16384\r\n\
        cluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\n\
        cluster_known_nodes:1\r\ncluster_size:1\r\n";
    Resp::Bulk(body.as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::{QueueId, TenantId};
    use fireweed_engine::QueueKey;

    #[test]
    fn crc16_matches_the_redis_reference_vector() {
        // The canonical CRC16-CCITT/XMODEM check value Redis documents.
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_eq!(crc16(b""), 0);
    }

    #[test]
    fn hash_slot_matches_known_redis_keyslot_values() {
        // Well-known Redis CLUSTER KEYSLOT values.
        assert_eq!(hash_slot(b"foo"), 12182);
        assert_eq!(hash_slot(b"bar"), 5061);
        // Every slot is within the 16384-slot space.
        for k in ["", "a", "some:queue", "{t/q}", "x".repeat(300).as_str()] {
            assert!(hash_slot(k.as_bytes()) < SLOT_COUNT);
        }
    }

    #[test]
    fn hash_tag_rule_matches_redis_exactly() {
        // A hash-tag makes two keys co-locate to one slot.
        assert_eq!(
            hash_slot(b"{user1000}.following"),
            hash_slot(b"{user1000}.followers")
        );
        assert_eq!(hash_slot(b"{user1000}.following"), hash_slot(b"user1000"));
        // Empty first tag `{}` → hash the WHOLE key (no co-location with `bar`).
        assert_eq!(hash_slot(b"foo{}{bar}"), crc16(b"foo{}{bar}") % SLOT_COUNT);
        assert_ne!(hash_slot(b"foo{}{bar}"), hash_slot(b"bar"));
        assert_eq!(hash_slot(b"{}foo"), crc16(b"{}foo") % SLOT_COUNT);
        // `foo{bar}{zap}` → first non-empty tag is `bar`.
        assert_eq!(hash_slot(b"foo{bar}{zap}"), hash_slot(b"bar"));
        // `foo{{bar}}zap` → content between first `{` and first `}` is `{bar`.
        assert_eq!(hash_slot(b"foo{{bar}}zap"), crc16(b"{bar") % SLOT_COUNT);
        // No close brace → whole key.
        assert_eq!(hash_slot(b"foo{bar"), crc16(b"foo{bar") % SLOT_COUNT);
    }

    fn qk(t: &str, q: &str) -> QueueKey {
        QueueKey::new(TenantId::new(t).unwrap(), QueueId::new(q).unwrap())
    }

    #[test]
    fn queue_routing_key_is_the_hash_tagged_form_and_slot_matches_the_content() {
        let shard = qk("t1", "q1");
        assert_eq!(queue_routing_key(&shard), "{t1/q1}");
        // A client sending the routing key `{t1/q1}` hashes "t1/q1" (the hash-tag content) — the server's
        // queue_slot matches that, byte-for-byte.
        assert_eq!(queue_slot(&shard), hash_slot(b"{t1/q1}"));
        assert_eq!(queue_slot(&shard), crc16(b"t1/q1") % SLOT_COUNT);
        // Two queues of one tenant generally differ; one queue is stable.
        assert_eq!(queue_slot(&shard), queue_slot(&qk("t1", "q1")));
    }

    #[test]
    fn node_id_is_stable_and_40_hex() {
        let n1 = ClusterNode::new("127.0.0.1", 6380);
        let n2 = ClusterNode::new("127.0.0.1", 6380);
        assert_eq!(n1.id, n2.id, "the id is stable per endpoint");
        assert_eq!(n1.id.len(), 40);
        assert!(n1.id.chars().all(|c| c.is_ascii_hexdigit()));
        // A different endpoint gets a different id.
        assert_ne!(n1.id, ClusterNode::new("127.0.0.1", 6381).id);
    }

    #[test]
    fn cluster_slots_reply_covers_the_whole_space_with_this_node() {
        let node = ClusterNode::new("10.0.0.1", 7000);
        let Resp::Array(ranges) = cluster_slots_single_node(&node) else {
            panic!("array");
        };
        assert_eq!(ranges.len(), 1, "single-node: one range");
        let Resp::Array(r) = &ranges[0] else {
            panic!("range array")
        };
        assert_eq!(r[0], Resp::Int(0));
        assert_eq!(
            r[1],
            Resp::Int(16383),
            "covers the full 0..=16383 slot space"
        );
        let Resp::Array(ep) = &r[2] else {
            panic!("endpoint")
        };
        assert_eq!(ep[0], Resp::Bulk(b"10.0.0.1".to_vec()));
        assert_eq!(ep[1], Resp::Int(7000));
        assert_eq!(ep[2], Resp::Bulk(node.id.into_bytes()));
    }

    #[test]
    fn cluster_shards_reply_has_one_master_shard_over_all_slots() {
        let node = ClusterNode::new("10.0.0.1", 7000);
        let Resp::Array(shards) = cluster_shards_single_node(&node) else {
            panic!("array");
        };
        assert_eq!(shards.len(), 1);
        let Resp::Array(shard) = &shards[0] else {
            panic!("shard")
        };
        assert_eq!(shard[0], Resp::Bulk(b"slots".to_vec()));
        assert_eq!(shard[1], Resp::Array(vec![Resp::Int(0), Resp::Int(16383)]));
        assert_eq!(shard[2], Resp::Bulk(b"nodes".to_vec()));
    }
}
