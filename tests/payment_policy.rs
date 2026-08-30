use fintech_dlq_worker::payment_policy::{decide_failure, FailureAction, PaymentEvent};

#[test]
fn high_risk_payment_is_dead_lettered_on_first_failure() {
    let payment = PaymentEvent {
        payment_id: "pay_1042".into(),
        amount_minor: 125_00,
        currency: "USD".into(),
        risk_score: 91,
        attempt: 1,
        failure_reason: "issuer_declined".into(),
    };

    assert_eq!(
        decide_failure(&payment),
        FailureAction::DeadLetter { reason: "high_risk_payment" }
    );
}

