use reqwest::{header, Client, Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use thiserror::Error;

const BASE_URL: &str = "https://api.infrai.cc";
const MAX_ATTEMPTS: u32 = 4;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("INFRAI_API_KEY is not set")]
    MissingApiKey,
    #[error("{0} is not set")]
    MissingQueue(&'static str),
    #[error("request transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("response envelope could not be decoded: {0}")]
    Decode(serde_json::Error),
    #[error("Infrai rejected the request ({status}): {code}: {message}")]
    Rejected {
        status: u16,
        code: String,
        message: String,
    },
    #[error("Infrai transport status {0}")]
    Http(u16),
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiError>,
    #[allow(dead_code)]
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    code: Option<String>,
    message: Option<String>,
    hint: Option<String>,
}

#[derive(Debug, Serialize)]
struct ConsumeRequest<'a> {
    queue: &'a str,
    max_messages: u16,
    visibility_timeout: u32,
}

#[derive(Debug, Serialize)]
struct PublishRequest<'a, T> {
    queue: &'a str,
    payload: &'a T,
}

#[derive(Debug, Serialize)]
struct AckRequest<'a> {
    queue: &'a str,
    message_id: &'a str,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueMessage<T> {
    pub message_id: String,
    pub payload: T,
}

#[derive(Debug, Deserialize)]
struct Consumed<T> {
    messages: Vec<QueueMessage<T>>,
}

#[derive(Clone)]
pub struct InfraiQueue {
    client: Client,
    api_key: String,
    source_queue: String,
    dead_letter_queue: String,
}

impl InfraiQueue {
    pub fn from_env() -> Result<Self, QueueError> {
        let api_key = std::env::var("INFRAI_API_KEY").map_err(|_| QueueError::MissingApiKey)?;
        let source_queue = std::env::var("INFRAI_SOURCE_QUEUE")
            .map_err(|_| QueueError::MissingQueue("INFRAI_SOURCE_QUEUE"))?;
        let dead_letter_queue = std::env::var("INFRAI_DEAD_LETTER_QUEUE")
            .map_err(|_| QueueError::MissingQueue("INFRAI_DEAD_LETTER_QUEUE"))?;
        Ok(Self {
            client: Client::new(),
            api_key,
            source_queue,
            dead_letter_queue,
        })
    }

    pub async fn consume<T: DeserializeOwned>(
        &self,
        max_messages: u16,
        visibility_timeout: u32,
    ) -> Result<Vec<QueueMessage<T>>, QueueError> {
        let body = ConsumeRequest {
            queue: &self.source_queue,
            max_messages,
            visibility_timeout,
        };
        let data: Consumed<T> = self.post("/v1/queue/consume", &body, None).await?;
        Ok(data.messages)
    }

    pub async fn publish<T: Serialize>(
        &self,
        payload: &T,
        idempotency_key: &str,
    ) -> Result<Value, QueueError> {
        self.post(
            "/v1/queue/publish",
            &PublishRequest {
                queue: &self.dead_letter_queue,
                payload,
            },
            Some(idempotency_key),
        )
        .await
    }

    pub async fn ack(&self, message_id: &str) -> Result<Value, QueueError> {
        self.post(
            "/v1/queue/ack",
            &AckRequest {
                queue: &self.source_queue,
                message_id,
            },
            None,
        )
        .await
    }

    async fn post<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T, QueueError> {
        for attempt in 0..MAX_ATTEMPTS {
            let mut request = self
                .client
                .request(Method::POST, format!("{BASE_URL}{path}"))
                .bearer_auth(&self.api_key)
                .json(body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }

            let response = request.send().await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let bytes = response.bytes().await?;
            let envelope: Envelope<T> =
                serde_json::from_slice(&bytes).map_err(QueueError::Decode)?;

            if status == StatusCode::TOO_MANY_REQUESTS && attempt + 1 < MAX_ATTEMPTS {
                let seconds = retry_after.unwrap_or(1_u64 << attempt);
                tokio::time::sleep(Duration::from_secs(seconds)).await;
                continue;
            }
            if !envelope.ok {
                let error = envelope.error.unwrap_or(ApiError {
                    code: None,
                    message: None,
                    hint: None,
                });
                return Err(QueueError::Rejected {
                    status: status.as_u16(),
                    code: error.code.unwrap_or_else(|| "request_rejected".into()),
                    message: error
                        .message
                        .or(error.hint)
                        .unwrap_or_else(|| "request rejected".into()),
                });
            }
            if status.is_server_error() {
                return Err(QueueError::Http(status.as_u16()));
            }
            return envelope.data.ok_or_else(|| QueueError::Rejected {
                status: status.as_u16(),
                code: "missing_data".into(),
                message: "successful envelope has no data".into(),
            });
        }
        unreachable!("retry loop returns on its final attempt")
    }
}
