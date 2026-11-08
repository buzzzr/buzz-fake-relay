//! Scripted mode: a fake Buzz relay for clients that read over HTTP (`buzzr`, the `buzz` CLI) and take live pushes over a WebSocket.
//!
//!   cargo run -- --script data/community.json [127.0.0.1:8787]
//!
//! It speaks just enough of the relay's wire protocol:
//!   * `POST /query`  - an in-memory event store filtered by the request's Nostr filters
//!   * `POST /events` - accepted, and echoed to live subscribers (so sends, edits and deletes show up)
//!   * `GET  /`       - a NIP-11 info document
//!   * a WebSocket on the same port: NIP-42 challenge, AUTH -> OK, REQ -> EOSE, and pushes
//!
//! Everything on screen comes from a JSON script (users, channels, messages, threads, reactions). Ids are not real hashes and no
//! signature is checked, which works for clients that trust the relay. Nothing is persisted; it is meant to bind to loopback.

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;

const MESSAGE_KINDS: [u64; 5] = [9, 40002, 40008, 45001, 45003];
const AUX_KINDS: [u64; 4] = [7, 40003, 5, 9005];

#[derive(Clone)]
struct State {
    events: Arc<Mutex<Vec<Value>>>,
    info: Value,
    tx: broadcast::Sender<String>,
}

/// A deterministic 64-hex string from a seed (ids do not need to be real signatures here).
fn hid(seed: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in seed.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut out = String::with_capacity(64);
    let mut x = h ^ (seed.len() as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    while out.len() < 64 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        out.push_str(&format!("{:016x}", x));
    }
    out.truncate(64);
    out
}

fn ev(id: &str, pubkey: &str, created_at: u64, kind: u64, content: &str, tags: Vec<Vec<String>>) -> Value {
    json!({ "id": id, "pubkey": pubkey, "created_at": created_at, "kind": kind, "content": content, "tags": tags })
}

fn relay_pk() -> String {
    hid("relay")
}

/// Build the whole event store from the script.
fn build_events(script: &Value) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    let mut users: HashMap<String, String> = HashMap::new();
    for u in script["users"].as_array().cloned().unwrap_or_default() {
        let key = u["key"].as_str().unwrap_or("").to_string();
        let pk = u["pubkey"].as_str().map(str::to_string).unwrap_or_else(|| hid(&format!("user:{key}")));
        let name = u["name"].as_str().unwrap_or(&key).to_string();
        users.insert(key.clone(), pk.clone());
        out.push(ev(&hid(&format!("profile:{key}")), &pk, 1_700_000_000, 0, &json!({ "display_name": name }).to_string(), Vec::new()));
    }
    for c in script["channels"].as_array().cloned().unwrap_or_default() {
        let cid = c["id"].as_str().unwrap_or("").to_string();
        let name = c["name"].as_str().unwrap_or("").to_string();
        let about = c["about"].as_str().unwrap_or("").to_string();
        let created = c["created_at"].as_u64().unwrap_or(1_700_000_000);
        out.push(ev(
            &hid(&format!("chan:{cid}")),
            &relay_pk(),
            created,
            39000,
            "",
            vec![
                vec!["d".into(), cid.clone()],
                vec!["name".into(), name.clone()],
                vec!["about".into(), about.clone()],
            ],
        ));
        let mut ptags = vec![vec!["d".into(), cid.clone()]];
        for m in c["members"].as_array().cloned().unwrap_or_default() {
            let key = m["user"].as_str().unwrap_or("");
            let role = m["role"].as_str().unwrap_or("member");
            if let Some(pk) = users.get(key) {
                ptags.push(vec!["p".into(), pk.clone(), String::new(), role.into()]);
            }
        }
        out.push(ev(&hid(&format!("members:{cid}")), &relay_pk(), created, 39002, "", ptags));

        for (i, m) in c["messages"].as_array().cloned().unwrap_or_default().iter().enumerate() {
            let mid = m["id"].as_str().map(str::to_string).unwrap_or_else(|| hid(&format!("msg:{cid}:{i}")));
            let from = m["from"].as_str().unwrap_or("");
            let pk = users.get(from).cloned().unwrap_or_else(|| hid(&format!("anon:{from}")));
            let at = m["at"].as_u64().unwrap_or(created + i as u64);
            let text = m["text"].as_str().unwrap_or("");
            out.push(ev(&mid, &pk, at, 9, text, vec![vec!["h".into(), cid.clone()]]));

            let mut last_reply = at;
            let mut reply_count = 0u64;
            let mut participants: Vec<String> = vec![pk.clone()];
            for (j, r) in m["replies"].as_array().cloned().unwrap_or_default().iter().enumerate() {
                let rid = r["id"].as_str().map(str::to_string).unwrap_or_else(|| hid(&format!("reply:{cid}:{i}:{j}")));
                let rfrom = r["from"].as_str().unwrap_or("");
                let rpk = users.get(rfrom).cloned().unwrap_or_else(|| hid(&format!("anon:{rfrom}")));
                let rat = r["at"].as_u64().unwrap_or(at + 1 + j as u64);
                let rtext = r["text"].as_str().unwrap_or("");
                out.push(ev(
                    &rid,
                    &rpk,
                    rat,
                    9,
                    rtext,
                    vec![vec!["h".into(), cid.clone()], vec!["e".into(), mid.clone(), String::new(), "reply".into()]],
                ));
                reply_count += 1;
                last_reply = rat;
                participants.push(rpk.clone());
                for (emoji, who) in r["reactions"].as_object().cloned().unwrap_or_default() {
                    for w in who.as_array().cloned().unwrap_or_default() {
                        let wpk = users.get(w.as_str().unwrap_or("")).cloned().unwrap_or_default();
                        out.push(ev(
                            &hid(&format!("react:{rid}:{emoji}:{wpk}")),
                            &wpk,
                            rat + 5,
                            7,
                            &emoji,
                            vec![vec!["e".into(), rid.clone()], vec!["h".into(), cid.clone()]],
                        ));
                    }
                }
            }
            if reply_count > 0 {
                out.push(ev(
                    &hid(&format!("sum:{mid}")),
                    &relay_pk(),
                    last_reply,
                    39005,
                    &json!({ "reply_count": reply_count, "last_reply_at": last_reply, "participants": participants }).to_string(),
                    vec![vec!["e".into(), mid.clone()], vec!["d".into(), mid.clone()]],
                ));
            }
            for (emoji, who) in m["reactions"].as_object().cloned().unwrap_or_default() {
                for w in who.as_array().cloned().unwrap_or_default() {
                    let wpk = users.get(w.as_str().unwrap_or("")).cloned().unwrap_or_default();
                    out.push(ev(
                        &hid(&format!("react:{mid}:{emoji}:{wpk}")),
                        &wpk,
                        at + 20,
                        7,
                        &emoji,
                        vec![vec!["e".into(), mid.clone()], vec!["h".into(), cid.clone()]],
                    ));
                }
            }
        }
    }
    out
}

fn tag_values(e: &Value, name: &str) -> Vec<String> {
    e["tags"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|t| {
                    let t = t.as_array()?;
                    if t.first()?.as_str()? == name {
                        t.get(1)?.as_str().map(str::to_string)
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn e_marked(e: &Value, marker: &str) -> bool {
    e["tags"].as_array().map(|a| {
        a.iter().any(|t| {
            let t = match t.as_array() { Some(t) => t, None => return false };
            t.first().and_then(|x| x.as_str()) == Some("e") && t.get(3).and_then(|x| x.as_str()) == Some(marker)
        })
    }).unwrap_or(false)
}

fn is_top_level(e: &Value) -> bool {
    !e_marked(e, "root") && !e_marked(e, "reply")
}

fn matches(e: &Value, f: &Value) -> bool {
    if let Some(kinds) = f.get("kinds").and_then(|v| v.as_array()) {
        let k = e["kind"].as_u64().unwrap_or(0);
        if !kinds.iter().any(|x| x.as_u64() == Some(k)) {
            return false;
        }
    }
    if let Some(ids) = f.get("ids").and_then(|v| v.as_array()) {
        let id = e["id"].as_str().unwrap_or("");
        if !ids.iter().any(|x| x.as_str() == Some(id)) {
            return false;
        }
    }
    if let Some(authors) = f.get("authors").and_then(|v| v.as_array()) {
        let pk = e["pubkey"].as_str().unwrap_or("");
        if !authors.iter().any(|x| x.as_str() == Some(pk)) {
            return false;
        }
    }
    for (key, tag) in [("#h", "h"), ("#e", "e"), ("#d", "d")] {
        if let Some(vals) = f.get(key).and_then(|v| v.as_array()) {
            let have = tag_values(e, tag);
            let want: Vec<&str> = vals.iter().filter_map(|x| x.as_str()).collect();
            if !want.iter().any(|w| have.iter().any(|h| h == w)) {
                return false;
            }
        }
    }
    if let Some(q) = f.get("search").and_then(|v| v.as_str()) {
        let c = e["content"].as_str().unwrap_or("").to_ascii_lowercase();
        if !c.contains(&q.to_ascii_lowercase()) {
            return false;
        }
    }
    true
}

fn by_newest(a: &&Value, b: &&Value) -> std::cmp::Ordering {
    b["created_at"].as_u64().unwrap_or(0).cmp(&a["created_at"].as_u64().unwrap_or(0))
}

fn answer_query(state: &State, filters: &[Value]) -> Vec<Value> {
    let store = state.events.lock().unwrap();
    let mut out: Vec<Value> = Vec::new();
    for f in filters {
        let limit = f.get("limit").and_then(|v| v.as_u64()).unwrap_or(500) as usize;
        if f.get("top_level").and_then(|v| v.as_bool()) == Some(true) {
            // a channel window: top-level rows, their summaries, their reactions/edits, and the bounds event
            let mut rows: Vec<&Value> = store
                .iter()
                .filter(|e| MESSAGE_KINDS.contains(&e["kind"].as_u64().unwrap_or(0)) && matches(e, f) && is_top_level(e))
                .collect();
            rows.sort_by(by_newest);
            rows.truncate(limit);
            let ids: Vec<String> = rows.iter().map(|e| e["id"].as_str().unwrap_or("").to_string()).collect();
            out.extend(rows.iter().map(|e| (*e).clone()));
            for e in store.iter() {
                if e["kind"].as_u64() == Some(39005) && tag_values(e, "e").iter().any(|t| ids.contains(t)) {
                    out.push(e.clone());
                }
            }
            for e in store.iter() {
                if AUX_KINDS.contains(&e["kind"].as_u64().unwrap_or(0)) && tag_values(e, "e").iter().any(|t| ids.contains(t)) {
                    out.push(e.clone());
                }
            }
            out.push(ev(
                &hid("bounds:head"),
                &relay_pk(),
                0,
                39006,
                &json!({ "has_more": false }).to_string(),
                vec![vec!["d".into(), "fake:head".into()]],
            ));
        } else {
            let mut rows: Vec<&Value> = store.iter().filter(|e| matches(e, f)).collect();
            rows.sort_by(by_newest);
            rows.truncate(limit);
            out.extend(rows.iter().map(|e| (*e).clone()));
        }
    }
    let mut seen = std::collections::HashSet::new();
    out.retain(|e| seen.insert(e["id"].as_str().unwrap_or("").to_string()));
    out
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    (0..=hay.len() - needle.len()).find(|&i| &hay[i..i + needle.len()] == needle)
}

async fn read_http(stream: &mut TcpStream) -> std::io::Result<(String, String, usize, Vec<u8>)> {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 4096];
    let head_end = loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "closed"));
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(i) = find_subslice(&buf, b"\r\n\r\n") {
            break i + 4;
        }
        if buf.len() > 1 << 20 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "header too big"));
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let reqline = lines.next().unwrap_or("");
    let mut parts = reqline.split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("/").to_string();
    let mut content_length = 0usize;
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }
    let mut body = buf[head_end..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);
    Ok((method, path, head_end, body))
}

async fn http_session(mut stream: TcpStream, state: State) -> std::io::Result<()> {
    let (method, path, _he, body) = read_http(&mut stream).await?;
    let (code, payload) = if method == "GET" {
        (200, state.info.to_string())
    } else if method == "POST" && path.starts_with("/query") {
        match serde_json::from_slice::<Value>(&body) {
            Ok(v) => {
                let filters: Vec<Value> = v.as_array().cloned().unwrap_or_default();
                (200, Value::Array(answer_query(&state, &filters)).to_string())
            }
            Err(e) => (400, json!({ "error": format!("bad query: {e}") }).to_string()),
        }
    } else if method == "POST" && path.starts_with("/events") {
        match serde_json::from_slice::<Value>(&body) {
            Ok(v) => {
                let id = v["id"].as_str().unwrap_or("").to_string();
                let kind = v["kind"].as_u64().unwrap_or(0);
                if kind != 20002 {
                    state.events.lock().unwrap().push(v.clone());
                    let _ = state.tx.send(json!(["EVENT", "live", v]).to_string());
                }
                (200, json!({ "accepted": true, "event_id": id }).to_string())
            }
            Err(e) => (400, json!({ "error": format!("bad event: {e}") }).to_string()),
        }
    } else {
        (404, json!({ "error": "not found" }).to_string())
    };
    let resp = format!(
        "HTTP/1.1 {code} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        payload.len(),
        payload
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await?;
    let _ = stream.shutdown().await;
    Ok(())
}

async fn ws_session(stream: TcpStream, state: State) -> std::io::Result<()> {
    let ws = tokio_tungstenite::accept_async(stream)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    let (mut sink, mut src) = ws.split();
    let _ = sink.send(Message::Text(json!(["AUTH", hid("challenge")]).to_string().into())).await;
    let mut rx = state.tx.subscribe();
    loop {
        tokio::select! {
            frame = src.next() => match frame {
                Some(Ok(Message::Text(t))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(t.as_str()) {
                        match v.get(0).and_then(|x| x.as_str()) {
                            Some("AUTH") => {
                                let id = v.get(1).and_then(|e| e.get("id")).and_then(|x| x.as_str()).unwrap_or("").to_string();
                                let _ = sink.send(Message::Text(json!(["OK", id, true, ""]).to_string().into())).await;
                            }
                            Some("REQ") => {
                                let sub = v.get(1).and_then(|x| x.as_str()).unwrap_or("live").to_string();
                                let _ = sink.send(Message::Text(json!(["EOSE", sub]).to_string().into())).await;
                            }
                            _ => {}
                        }
                    }
                }
                Some(Ok(Message::Ping(p))) => { let _ = sink.send(Message::Pong(p)).await; }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(_)) => break,
                _ => {}
            },
            msg = rx.recv() => {
                if let Ok(m) = msg {
                    if sink.send(Message::Text(m.into())).await.is_err() { break; }
                }
            }
        }
    }
    Ok(())
}

async fn handle(stream: TcpStream, state: State) -> std::io::Result<()> {
    // Peek (without consuming) to tell a WebSocket upgrade from a plain HTTP request.
    let mut head = vec![0u8; 8192];
    let mut n = 0usize;
    for _ in 0..500 {
        n = stream.peek(&mut head).await?;
        if n == 0 {
            return Ok(());
        }
        if find_subslice(&head[..n], b"\r\n\r\n").is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let lower = String::from_utf8_lossy(&head[..n]).to_ascii_lowercase();
    if lower.contains("upgrade: websocket") {
        ws_session(stream, state).await
    } else {
        http_session(stream, state).await
    }
}

pub async fn run(script_path: &str, addr: &str) -> std::io::Result<()> {
    let script: Value = serde_json::from_str(&std::fs::read_to_string(script_path)?)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let events = build_events(&script);
    let (tx, _rx) = broadcast::channel(256);
    let state = State {
        events: Arc::new(Mutex::new(events)),
        info: json!({
            "name": script["relay"]["name"].as_str().unwrap_or("Fake Relay"),
            "description": "A local fake relay",
            "software": "buzz-fake-relay",
            "version": "0.1.0",
            "supported_nips": [1, 9, 11, 42, 50, 98]
        }),
        tx,
    };
    let listener = TcpListener::bind(addr).await?;
    eprintln!("scripted relay listening on http://{addr}  (ws://{addr})  script={script_path}");
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, state).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}
