use kube::Error;
use serde_json::{Value, json, to_vec};

#[derive(Debug)]
pub struct Failure {
    message: String,
    reason: String,
    code: Option<u16>,
}

impl Failure {
    pub fn payload(self) -> Value {
        json!({"error": self.message.chars().take(2048).collect::<String>(), "reason": self.reason.chars().take(128).collect::<String>(), "code": self.code})
    }
}

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Self {
            message,
            reason: "InvalidRequest".into(),
            code: None,
        }
    }
}

impl From<&str> for Failure {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        match error {
            Error::Api(status) => Self {
                message: status.message,
                reason: status.reason,
                code: Some(status.code),
            },
            other => Self {
                message: other.to_string(),
                reason: "ClientError".into(),
                code: None,
            },
        }
    }
}

pub fn compact(item: &Value) -> Value {
    let mut row =
        json!({"name":item["metadata"]["name"], "namespace":item["metadata"]["namespace"]});
    for key in [
        "phase",
        "readyReplicas",
        "availableReplicas",
        "replicas",
        "succeeded",
        "failed",
    ] {
        if let Some(value) = item["status"]
            .get(key)
            .filter(|v| v.is_string() || v.is_number())
        {
            row[key] = value.clone();
        }
    }
    row
}

pub fn bounded(value: Value) -> Result<Value, Failure> {
    if to_vec(&value).map_err(|e| e.to_string())?.len() <= 256 * 1024 {
        return Ok(value);
    }
    Err(Failure { message: "Response exceeds 256 KiB. For lists, retry the same page with compact=true or a smaller limit.".into(), reason: "ResponseTooLarge".into(), code: None })
}
