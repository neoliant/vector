use super::*;
use chrono::{offset::TimeZone, DateTime, Utc};

fn ts() -> DateTime<Utc> {
    Utc.ymd(2018, 11, 14).and_hms_nano(8, 9, 10, 11)
}

fn tags() -> BTreeMap<String, String> {
    vec![
        ("normal_tag".to_owned(), "value".to_owned()),
        ("true_tag".to_owned(), "true".to_owned()),
        ("empty_tag".to_owned(), "".to_owned()),
    ]
    .into_iter()
    .collect()
}

#[test_env_log::test]
fn merge_counters() {
    let mut counter = Metric {
        name: "counter".into(),
        namespace: None,
        timestamp: None,
        tags: None,
        kind: MetricKind::Incremental,
        value: MetricValue::Counter { value: 1.0 },
    };

    let delta = Metric {
        name: "counter".into(),
        namespace: Some("vector".to_string()),
        timestamp: Some(ts()),
        tags: Some(tags()),
        kind: MetricKind::Incremental,
        value: MetricValue::Counter { value: 2.0 },
    };

    counter.add(&delta);
    assert_eq!(
        counter,
        Metric {
            name: "counter".into(),
            namespace: None,
            timestamp: None,
            tags: None,
            kind: MetricKind::Incremental,
            value: MetricValue::Counter { value: 3.0 },
        }
    )
}

#[test_env_log::test]
fn merge_gauges() {
    let mut gauge = Metric {
        name: "gauge".into(),
        namespace: None,
        timestamp: None,
        tags: None,
        kind: MetricKind::Incremental,
        value: MetricValue::Gauge { value: 1.0 },
    };

    let delta = Metric {
        name: "gauge".into(),
        namespace: Some("vector".to_string()),
        timestamp: Some(ts()),
        tags: Some(tags()),
        kind: MetricKind::Incremental,
        value: MetricValue::Gauge { value: -2.0 },
    };

    gauge.add(&delta);
    assert_eq!(
        gauge,
        Metric {
            name: "gauge".into(),
            namespace: None,
            timestamp: None,
            tags: None,
            kind: MetricKind::Incremental,
            value: MetricValue::Gauge { value: -1.0 },
        }
    )
}

#[test_env_log::test]
fn merge_sets() {
    let mut set = Metric {
        name: "set".into(),
        namespace: None,
        timestamp: None,
        tags: None,
        kind: MetricKind::Incremental,
        value: MetricValue::Set {
            values: vec!["old".into()].into_iter().collect(),
        },
    };

    let delta = Metric {
        name: "set".into(),
        namespace: Some("vector".to_string()),
        timestamp: Some(ts()),
        tags: Some(tags()),
        kind: MetricKind::Incremental,
        value: MetricValue::Set {
            values: vec!["new".into()].into_iter().collect(),
        },
    };

    set.add(&delta);
    assert_eq!(
        set,
        Metric {
            name: "set".into(),
            namespace: None,
            timestamp: None,
            tags: None,
            kind: MetricKind::Incremental,
            value: MetricValue::Set {
                values: vec!["old".into(), "new".into()].into_iter().collect()
            },
        }
    )
}

#[test_env_log::test]
fn merge_histograms() {
    let mut dist = Metric {
        name: "hist".into(),
        namespace: None,
        timestamp: None,
        tags: None,
        kind: MetricKind::Incremental,
        value: MetricValue::Distribution {
            values: vec![1.0],
            sample_rates: vec![10],
            statistic: StatisticKind::Histogram,
        },
    };

    let delta = Metric {
        name: "hist".into(),
        namespace: Some("vector".to_string()),
        timestamp: Some(ts()),
        tags: Some(tags()),
        kind: MetricKind::Incremental,
        value: MetricValue::Distribution {
            values: vec![1.0],
            sample_rates: vec![20],
            statistic: StatisticKind::Histogram,
        },
    };

    dist.add(&delta);
    assert_eq!(
        dist,
        Metric {
            name: "hist".into(),
            namespace: None,
            timestamp: None,
            tags: None,
            kind: MetricKind::Incremental,
            value: MetricValue::Distribution {
                values: vec![1.0, 1.0],
                sample_rates: vec![10, 20],
                statistic: StatisticKind::Histogram
            },
        }
    )
}

#[test_env_log::test]
fn display() {
    assert_eq!(
        format!(
            "{}",
            Metric {
                name: "one".into(),
                namespace: None,
                timestamp: None,
                tags: Some(tags()),
                kind: MetricKind::Absolute,
                value: MetricValue::Counter { value: 1.23 },
            }
        ),
        r#"one{empty_tag="",normal_tag="value",true_tag="true"} = 1.23"#
    );

    assert_eq!(
        format!(
            "{}",
            Metric {
                name: "two word".into(),
                namespace: None,
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Incremental,
                value: MetricValue::Gauge { value: 2.0 }
            }
        ),
        r#"2018-11-14T08:09:10.000000011Z "two word"{} + 2"#
    );

    assert_eq!(
        format!(
            "{}",
            Metric {
                name: "namespace".into(),
                namespace: Some("vector".to_string()),
                timestamp: None,
                tags: None,
                kind: MetricKind::Absolute,
                value: MetricValue::Counter { value: 1.23 },
            }
        ),
        r#"vector_namespace{} = 1.23"#
    );

    assert_eq!(
        format!(
            "{}",
            Metric {
                name: "namespace".into(),
                namespace: Some("vector host".to_string()),
                timestamp: None,
                tags: None,
                kind: MetricKind::Absolute,
                value: MetricValue::Counter { value: 1.23 },
            }
        ),
        r#""vector host"_namespace{} = 1.23"#
    );

    let mut values = BTreeSet::<String>::new();
    values.insert("v1".into());
    values.insert("v2_two".into());
    values.insert("thrəë".into());
    values.insert("four=4".into());
    assert_eq!(
        format!(
            "{}",
            Metric {
                name: "three".into(),
                namespace: None,
                timestamp: None,
                tags: None,
                kind: MetricKind::Absolute,
                value: MetricValue::Set { values }
            }
        ),
        r#"three{} = "four=4" "thrəë" v1 v2_two"#
    );

    assert_eq!(
        format!(
            "{}",
            Metric {
                name: "four".into(),
                namespace: None,
                timestamp: None,
                tags: None,
                kind: MetricKind::Absolute,
                value: MetricValue::Distribution {
                    values: vec![1.0, 2.0],
                    sample_rates: vec![3, 4],
                    statistic: StatisticKind::Histogram,
                }
            }
        ),
        r#"four{} = histogram 3@1 4@2"#
    );

    assert_eq!(
        format!(
            "{}",
            Metric {
                name: "five".into(),
                namespace: None,
                timestamp: None,
                tags: None,
                kind: MetricKind::Absolute,
                value: MetricValue::AggregatedHistogram {
                    buckets: vec![51.0, 52.0],
                    counts: vec![53, 54],
                    count: 107,
                    sum: 103.0,
                }
            }
        ),
        r#"five{} = count=107 sum=103 53@51 54@52"#
    );

    assert_eq!(
        format!(
            "{}",
            Metric {
                name: "six".into(),
                namespace: None,
                timestamp: None,
                tags: None,
                kind: MetricKind::Absolute,
                value: MetricValue::AggregatedSummary {
                    quantiles: vec![1.0, 2.0],
                    values: vec![63.0, 64.0],
                    count: 2,
                    sum: 127.0,
                }
            }
        ),
        r#"six{} = count=2 sum=127 1@63 2@64"#
    );
}
