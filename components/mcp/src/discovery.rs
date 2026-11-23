use std::{collections::BTreeMap, collections::HashSet, time::Duration};

use rmcp::{model::PaginatedRequestParams, model::Tool};
use serde_json::{Value, to_value, to_vec};
use sha2::{Digest, Sha256};
use tokio::time::timeout;

use crate::{ClientLimits, McpError, Result, transport::Session};

#[derive(Clone)]
pub struct ToolSnapshot {
    pub revision: String,
    pub tools: Vec<Tool>,
}
impl ToolSnapshot {
    pub fn find(&self, name: &str) -> Option<&Tool> {
        self.tools.iter().find(|tool| tool.name.as_ref() == name)
    }
}

pub(crate) async fn discover(session: &Session, limits: &ClientLimits) -> Result<ToolSnapshot> {
    timeout(Duration::from_millis(limits.discovery_timeout_ms), async {
        let mut cursor = None;
        let mut cursors = HashSet::new();
        let mut tools = Vec::new();
        let mut bytes = 0usize;
        for _ in 0..limits.max_discovery_pages {
            let page = session
                .list_tools(Some(PaginatedRequestParams::default().with_cursor(cursor)))
                .await
                .map_err(|_| McpError::Connection)?;
            bytes = bytes.saturating_add(to_vec(&page).map_err(|_| McpError::Protocol)?.len());
            if bytes > limits.max_discovery_bytes
                || tools.len().saturating_add(page.tools.len()) > limits.max_tools
            {
                return Err(McpError::DiscoveryLimit);
            }
            tools.extend(page.tools);
            cursor = page.next_cursor;
            match &cursor {
                Some(value) if !cursors.insert(value.clone()) => return Err(McpError::Protocol),
                Some(_) => {}
                None => return snapshot(tools),
            }
        }
        Err(McpError::DiscoveryLimit)
    })
    .await
    .map_err(|_| McpError::DiscoveryLimit)?
}
pub(crate) fn snapshot(mut tools: Vec<Tool>) -> Result<ToolSnapshot> {
    tools.sort_by(|a, b| a.name.cmp(&b.name));
    let mut previous: Option<&str> = None;
    for tool in &tools {
        let name = tool.name.as_ref();
        if name.is_empty()
            || name.len() > 1024
            || name.chars().any(char::is_control)
            || previous == Some(name)
        {
            return Err(McpError::Protocol);
        }
        previous = Some(name);
    }
    let canonical = canonical(to_value(&tools).map_err(|_| McpError::Protocol)?);
    let bytes = to_vec(&canonical).map_err(|_| McpError::Protocol)?;
    Ok(ToolSnapshot {
        revision: format!("{:x}", Sha256::digest(bytes)),
        tools,
    })
}
fn canonical(value: Value) -> Value {
    match value {
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(key, value)| (key, canonical(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
        other => other,
    }
}
