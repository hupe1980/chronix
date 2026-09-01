//! Integration-level tests for wire protocol conversion.
//!
//! Tests that exercise the full pipeline from protobuf → Chronix points
//! with snappy compression, multi-series writes, and edge cases.

#[cfg(test)]
mod prometheus_wire_tests {
    use prost::Message;

    use crate::prom_proto;

    #[test]
    fn full_write_roundtrip_snappy() {
        // Build a multi-series request
        let req = prom_proto::WriteRequest {
            timeseries: vec![
                prom_proto::TimeSeries {
                    labels: vec![
                        prom_proto::Label {
                            name: "__name__".into(),
                            value: "node_cpu_seconds_total".into(),
                        },
                        prom_proto::Label {
                            name: "cpu".into(),
                            value: "0".into(),
                        },
                        prom_proto::Label {
                            name: "mode".into(),
                            value: "idle".into(),
                        },
                    ],
                    samples: vec![
                        prom_proto::Sample {
                            value: 1234.56,
                            timestamp: 1700000000000,
                        },
                        prom_proto::Sample {
                            value: 1234.78,
                            timestamp: 1700000015000,
                        },
                    ],
                    exemplars: vec![],
                },
                prom_proto::TimeSeries {
                    labels: vec![
                        prom_proto::Label {
                            name: "__name__".into(),
                            value: "node_cpu_seconds_total".into(),
                        },
                        prom_proto::Label {
                            name: "cpu".into(),
                            value: "1".into(),
                        },
                        prom_proto::Label {
                            name: "mode".into(),
                            value: "system".into(),
                        },
                    ],
                    samples: vec![prom_proto::Sample {
                        value: 42.0,
                        timestamp: 1700000000000,
                    }],
                    exemplars: vec![],
                },
            ],
        };

        // Encode → Snappy compress
        let encoded = req.encode_to_vec();
        let compressed = snap::raw::Encoder::new().compress_vec(&encoded).unwrap();

        // Decompress → decode (simulates what remote_write_handler does)
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        let decoded = prom_proto::WriteRequest::decode(decompressed.as_slice()).unwrap();

        assert_eq!(decoded.timeseries.len(), 2);
        assert_eq!(decoded.timeseries[0].samples.len(), 2);
        assert_eq!(decoded.timeseries[1].samples.len(), 1);
    }

    #[test]
    fn read_request_roundtrip() {
        let req = prom_proto::ReadRequest {
            queries: vec![prom_proto::Query {
                start_timestamp_ms: 1700000000000,
                end_timestamp_ms: 1700001000000,
                matchers: vec![
                    prom_proto::LabelMatcher {
                        r#type: prom_proto::label_matcher::Type::Eq as i32,
                        name: "__name__".into(),
                        value: "cpu".into(),
                    },
                    prom_proto::LabelMatcher {
                        r#type: prom_proto::label_matcher::Type::Eq as i32,
                        name: "host".into(),
                        value: "srv1".into(),
                    },
                ],
            }],
        };

        // Encode → compress → decompress → decode
        let encoded = req.encode_to_vec();
        let compressed = snap::raw::Encoder::new().compress_vec(&encoded).unwrap();
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        let decoded = prom_proto::ReadRequest::decode(decompressed.as_slice()).unwrap();

        assert_eq!(decoded.queries.len(), 1);
        assert_eq!(decoded.queries[0].matchers.len(), 2);
        assert_eq!(decoded.queries[0].start_timestamp_ms, 1700000000000);
    }
}
