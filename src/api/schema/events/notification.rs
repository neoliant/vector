use async_graphql::{Enum, SimpleObject};

#[derive(Enum, Debug, Copy, Clone, PartialEq, Eq)]
/// Event notification type
pub enum EventNotificationType {
    /// A component was found that matched the provided pattern
    Matched,
    /// There isn't currently a component that matches this pattern
    NotMatched,
}

#[derive(Debug, SimpleObject)]
/// A notification regarding events observation
pub struct EventNotification {
    /// Component pattern that raised the event
    component_pattern: String,

    /// Event notification type
    notification: EventNotificationType,
}

impl EventNotification {
    pub fn new(component_pattern: String, notification: EventNotificationType) -> Self {
        Self {
            component_pattern,
            notification,
        }
    }
}
