//! Real child-process protocol fixture, including environment isolation and cleanup.
#[cfg(test)]
mod tests {
    use crate::{ClientLimits, JsonObject, ServerConfig, ServerSession, fixture::Secrets};
    use serde_json::{Value, from_str};
    use std::{
        collections::BTreeMap, fs::write, path::Path, process::Command, sync::Arc, time::Duration,
    };
    use tempfile::tempdir;
    use tokio::time::{sleep, timeout};
    use tokio_util::sync::CancellationToken;

    const SERVER: &str = r#"
import json, os, sys
for line in sys.stdin:
    message=json.loads(line)
    if 'id' not in message:
        continue
    method=message.get('method')
    if method=='initialize':
        result={'protocolVersion':message['params']['protocolVersion'],'capabilities':{'tools':{}},'serverInfo':{'name':'stdio-fixture','version':'1'}}
    elif method=='tools/list':
        result={'tools':[{'name':'environment','description':'Only explicit environment data','inputSchema':{'type':'object'}}]}
    elif method=='tools/call':
        result={'content':[{'type':'text','text':json.dumps({'visible':os.environ.get('VISIBLE'),'path':os.environ.get('PATH'),'pid':os.getpid()})}],'isError':False}
    elif method=='ping':
        result={}
    else:
        print(json.dumps({'jsonrpc':'2.0','id':message['id'],'error':{'code':-32601,'message':'unknown fixture method'}}),flush=True)
        continue
    print(json.dumps({'jsonrpc':'2.0','id':message['id'],'result':result}),flush=True)
"#;
    #[tokio::test]
    async fn stdio_clears_inherited_environment_resolves_explicit_references_and_reaps_its_child() {
        let python = Command::new("python3")
            .args(["-c", "import sys;print(sys.executable)"])
            .output()
            .expect("stdio tests require Python 3");
        assert!(python.status.success());
        let python = String::from_utf8(python.stdout).unwrap().trim().to_owned();
        let dir = tempdir().unwrap();
        write(dir.path().join("server.py"), SERVER).unwrap();
        let config = ServerConfig::Stdio {
            command: python,
            args: vec!["server.py".into()],
            env: BTreeMap::from([("VISIBLE".into(), "VISIBLE_REFERENCE".into())]),
            inherited_env: vec![],
        };
        let client = ServerSession::connect(
            config,
            dir.path(),
            ClientLimits::default(),
            Arc::new(Secrets(BTreeMap::from([(
                "VISIBLE_REFERENCE".into(),
                "explicit-fixture-value".into(),
            )]))),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let result = client
            .call(
                &client.snapshot().revision,
                "environment",
                JsonObject::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let result: Value = from_str(&result.content[0].raw.as_text().unwrap().text).unwrap();
        assert_eq!(result["visible"], "explicit-fixture-value");
        assert!(result["path"].is_null());
        let pid = result["pid"].as_u64().unwrap();
        client.close().await.unwrap();
        #[cfg(target_os = "linux")]
        timeout(Duration::from_secs(3), async {
            while Path::new(&format!("/proc/{pid}")).exists() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the MCP child must be reaped after closing its transport");
    }
}
