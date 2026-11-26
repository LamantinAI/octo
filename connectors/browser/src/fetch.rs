use serde_json::{Value, json};
use zendriver::Tab;

pub(super) async fn render_tab(tab: &Tab, url: &str, want_html: bool) -> Result<Value, String> {
    tab.goto(url).await.map_err(|e| format!("navigate: {e}"))?;
    tab.wait_for_load()
        .await
        .map_err(|e| format!("load: {e}"))?;
    let title: String = tab.evaluate("document.title").await.unwrap_or_default();
    let text: String = tab
        .evaluate("document.body ? document.body.innerText : ''")
        .await
        .map_err(|e| format!("extract text: {e}"))?;
    let final_url: String = tab
        .evaluate("location.href")
        .await
        .unwrap_or_else(|_| url.to_owned());
    let mut result = json!({
        "url": url,
        "final_url": final_url,
        "title": collapse_ws(&title),
        "text": text.trim(),
        "extraction_status": if text.trim().is_empty() { "empty" } else { "text" },
    });
    if want_html {
        let html = tab
            .content()
            .await
            .map_err(|e| format!("extract HTML: {e}"))?;
        result["html"] = json!(html);
    }
    Ok(result)
}

fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}
