use fintech_dlq_worker::infrai_queue::{InfraiQueue, QueueError};
use fintech_dlq_worker::payment_policy::{
    decide_failure, DeadLetterNotification, FailureAction, PaymentEvent,
};

#[tokio::main]
async fn main() -> Result<(), QueueError> {
    let queue = InfraiQueue::from_env()?;
    let messages = queue.consume::<PaymentEvent>(10, 60).await?;

    for message in messages {
        match decide_failure(&message.payload) {
            FailureAction::Retry => {
                println!(
                    "{}",
                    serde_json::json!({
                        "payment_id": message.payload.payment_id,
                        "message_id": message.message_id,
                        "action": "retry"
                    })
                );
            }
            FailureAction::DeadLetter { reason } => {
                let notification = DeadLetterNotification {
                    event: &message.payload,
                    source_message_id: &message.message_id,
                    decision: "dead_letter",
                    decision_reason: reason,
                };
                let idempotency_key = format!("payment-dlq-{}", message.message_id);
                queue.publish(&notification, &idempotency_key).await?;
                queue.ack(&message.message_id).await?;
                println!("{}", serde_json::to_string(&notification).expect("serializable audit record"));
            }
        }
    }
    Ok(())
}

