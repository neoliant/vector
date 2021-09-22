use crate::config::{DataType, TransformConfig, TransformContext, TransformDescription};
use crate::event::Event;
use crate::transforms::{TaskTransform, Transform};

use async_stream::stream;
use futures::{stream, Stream, StreamExt};
use governor::*;
use nonzero_ext::nonzero;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::time::Duration;

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields, default)]
pub struct ThrottleConfig {}

inventory::submit! {
    TransformDescription::new::<ThrottleConfig>("throttle")
}

impl_generate_config_from_default!(ThrottleConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "throttle")]
impl TransformConfig for ThrottleConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Throttle::new(self).map(Transform::task)
    }

    fn input_type(&self) -> DataType {
        DataType::Any
    }

    fn output_type(&self) -> DataType {
        DataType::Any
    }

    fn transform_type(&self) -> &'static str {
        "throttle"
    }
}

#[derive(Clone, Debug)]
pub struct Throttle {}

impl Throttle {
    pub fn new(_config: &ThrottleConfig) -> crate::Result<Self> {
        Ok(Self {})
    }
}

impl TaskTransform for Throttle {
    fn transform(
        self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
    where
        Self: 'static,
    {
        let lim = RateLimiter::direct(Quota::per_second(nonzero!(10u32)));

        let mut flush_stream = tokio::time::interval(Duration::from_millis(1000));

        Box::pin(
            stream! {
              loop {
                let mut output = Vec::new();
                let done = tokio::select! {
                    _ = flush_stream.tick() => {
                        false
                    }
                    maybe_event = input_rx.next() => {
                        match maybe_event {
                            None => true,
                            Some(event) => {
                                match lim.check() {
                                    Ok(()) => {
                                        output.push(event);
                                        false
                                    }
                                    _ => {
                                        dbg!("RATE LIMITED");
                                        // Dropping event
                                        false
                                    }
                                }
                            }
                        }
                    }
                };
                yield stream::iter(output.into_iter());
                if done { break }
              }
            }
            .flatten(),
        )
    }
}
