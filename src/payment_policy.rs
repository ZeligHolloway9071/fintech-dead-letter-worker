use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaymentEvent {
    pub payment_id: String,
    pub amount_minor: u64,
    pub currency: String,
    pub risk_score: u8,
    pub attempt: u8,
    pub failure_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureAction {
    Retry,
    DeadLetter { reason: &'static str },
}

pub fn decide_failure(event: &PaymentEvent) -> FailureAction {
    if event.risk_score >= 80 {
        FailureAction::DeadLetter { reason: "high_risk_payment" }
    } else if event.attempt >= 3 {
        FailureAction::DeadLetter { reason: "retry_budget_exhausted" }
    } else {
        FailureAction::Retry
    }
}

#[derive(Debug, Serialize)]
pub struct DeadLetterNotification<'a> {
    pub event: &'a PaymentEvent,
    pub source_message_id: &'a str,
    pub decision: &'static str,
    pub decision_reason: &'static str,
}

