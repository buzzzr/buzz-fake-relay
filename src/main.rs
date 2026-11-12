//! A tiny fake Buzz relay for exercising any Buzz client by hand or from a script.
//!
//!   cargo run -- 127.0.0.1:7447
//!
//! It speaks both halves of the relay's surface on one port:
//!   * WebSocket — NIP-42 challenge, any AUTH -> OK, a verified EVENT -> OK and
//!     into the store, REQ -> EOSE answered from the store (NIP-29 groups over
//!     WebSocket);
//!   * HTTP — `GET /` (NIP-11 info), `POST /query` (the store filtered by the
//!     request's Nostr filters, NIP-98 header ignored) and `POST /events`
//!     (`{"accepted":true,...}`), which is how the `buzz` CLI and clients built
//!     on it read history and send.
//! Both are served from a fixed set of NIP-29 channels and legacy hidden DM
//! rooms, seeded at startup. Newest-message times are chosen so channels and DMs
//! interleave when ordered by recent activity. An event a client sends is
//! checked (id and signature) and added to the store, so a query reads back what
//! was published; live events are still not pushed to open subscriptions.
use futures_util::{SinkExt, StreamExt};
use nostr::prelude::*;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message as Ws;

fn keys(seed: u8) -> Keys {
    Keys::parse(&format!("{seed:02x}").repeat(32)).unwrap()
}

fn sign(k: &Keys, kind: u16, content: &str, at: u64, tags: Vec<Vec<&str>>) -> Event {
    EventBuilder::new(Kind::from_u16(kind), content)
        .tags(tags.into_iter().map(|t| Tag::parse(t).unwrap()))
        .custom_created_at(Timestamp::from_secs(at))
        .finalize(k)
        .unwrap()
}

fn seed() -> Vec<Event> {
    let relay = keys(1);
    // (seed, name, about)
    let people = [
        (2u8, "alice", "Design. I sail at the weekend."),
        (3, "bob", "Backend. Ask me about the relay."),
        (4, "carol", "Ops. I keep the lights on."),
    ];
    let now = Timestamp::now().as_secs();
    let mut out = Vec::new();
    for (s, name, about) in people {
        let k = keys(s);
        out.push(sign(
            &k,
            0,
            &format!(r#"{{"name":"{name}","about":"{about}"}}"#),
            now - 9000,
            vec![],
        ));
    }
    // (id, name, hidden, newest message age in seconds, author seed)
    let rooms: [(&str, &str, bool, u64, u8); 8] = [
        ("c-general", "general", false, 600, 2),
        ("dm-bob", "DM", true, 120, 3),
        ("c-random", "random", false, 1200, 3),
        ("dm-alice", "DM", true, 300, 2),
        ("c-design", "design", false, 1800, 4),
        ("dm-carol", "DM", true, 2400, 4),
        ("c-ops", "ops", false, 3600, 2),
        ("c-dev", "dev", false, 60, 4),
    ];
    for (id, name, hidden, age, author) in rooms {
        let mut tags = vec![vec!["d", id], vec!["name", name]];
        if hidden {
            tags.push(vec!["hidden"]);
            tags.push(vec!["private"]);
        }
        out.push(sign(&relay, 39000, "", now - 9000, tags));
        if hidden {
            let peer = keys(author).public_key().to_hex();
            out.push(sign(
                &relay,
                39002,
                "",
                now - 9000,
                vec![vec!["d", id], vec!["p", &peer, "member"]],
            ));
        }
        let a = keys(author);
        out.push(sign(
            &a,
            9,
            &format!("hello from {id}"),
            now - age,
            vec![vec!["h", id]],
        ));
    }
    // `random` is the one open channel with a roster: the relay names its members, so a client that is neither of them can
    // tell it is on a channel it is not in. The other open channels carry no roster, which is what a relay that does not
    // publish a group's members leaves a client with.
    let members: Vec<String> = [keys(2), keys(3)].iter().map(|k| k.public_key().to_hex()).collect();
    let mut random = vec![vec!["d", "c-random"]];
    random.extend(members.iter().map(|p| vec!["p", p.as_str(), "member"]));
    out.push(sign(&relay, 39002, "", now - 9000, random));
    // A short thread in `general`: the message above plus two replies, so a client can exercise opening a thread.
    if let Some(root) = out
        .iter()
        .find(|e| e.kind.as_u16() == 9 && e.tags.iter().any(|t| t.kind() == "h" && t.content() == Some("c-general")))
        .map(|e| e.id.to_hex())
    {
        out.push(sign(&keys(3), 9, "the sidebar keeps a fixed order", now - 420, vec![vec!["h", "c-general"], vec!["e", root.as_str(), "", "reply"]]));
        out.push(sign(&keys(2), 9, "and Enter opens the thread", now - 180, vec![vec!["h", "c-general"], vec!["e", root.as_str(), "", "reply"]]));
    }
    out
}

#[tokio::main]
async fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7447".into());
    let store = Arc::new(Mutex::new(seed()));
    let listener = TcpListener::bind(&addr).await.unwrap();
    eprintln!(
        "fake relay on http://{addr} and ws://{addr} with {} events",
        store.lock().unwrap().len()
    );
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let store = store.clone();
        tokio::spawn(async move {
            // The two protocols share the port. Tell them apart from the first
            // bytes without consuming them: a WebSocket upgrade carries
            // `Sec-WebSocket-Key`; anything else is a plain HTTP request.
            let mut head = [0u8; 1024];
            let n = match stream.peek(&mut head).await {
                Ok(n) => n,
                Err(_) => return,
            };
            let probe = String::from_utf8_lossy(&head[..n]).to_ascii_lowercase();
            if probe.contains("sec-websocket-key") {
                ws_session(stream, store).await;
            } else {
                http_session(stream, store).await;
            }
        });
    }
}

/// One WebSocket client: NIP-42 challenge, AUTH -> OK, an EVENT that verifies ->
/// OK and into the store, REQ -> the store, then EOSE. Live events are not pushed.
async fn ws_session(stream: TcpStream, store: Arc<Mutex<Vec<Event>>>) {
    let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
        return;
    };
    let (mut tx, mut rx) = ws.split();
    let _ = tx
        .send(Ws::text(RelayMessage::auth("challenge").as_json()))
        .await;
    while let Some(Ok(frame)) = rx.next().await {
        let Ws::Text(text) = frame else { continue };
        let Ok(msg) = ClientMessage::from_json(text.as_bytes()) else {
            continue;
        };
        match msg {
            ClientMessage::Auth(e) => {
                let _ = tx
                    .send(Ws::text(RelayMessage::ok(e.id, true, "").as_json()))
                    .await;
            }
            ClientMessage::Event(e) => {
                let e = e.into_owned();
                let id = e.id;
                let (status, why) = match save(&store, e) {
                    Ok(()) => (true, String::new()),
                    Err(why) => (false, why),
                };
                let _ = tx
                    .send(Ws::text(RelayMessage::ok(id, status, why).as_json()))
                    .await;
            }
            ClientMessage::Req {
                subscription_id,
                filters,
            } => {
                let filters: Vec<Filter> = filters.into_iter().map(|f| f.into_owned()).collect();
                // Read the store, then let the lock go: the sends below await, and a lock held across them would
                // block every other connection.
                let hits = {
                    let events = store.lock().unwrap();
                    query(&filters, &events)
                };
                for e in hits {
                    let m = RelayMessage::event(subscription_id.clone().into_owned(), e);
                    let _ = tx.send(Ws::text(m.as_json())).await;
                }
                let m = RelayMessage::eose(subscription_id.into_owned());
                let _ = tx.send(Ws::text(m.as_json())).await;
            }
            _ => {}
        }
    }
}

/// One plain HTTP request, the surface the `buzz` CLI and clients built on it
/// use: `GET /` (NIP-11), `POST /query` and `POST /events`. NIP-98 headers are
/// ignored, like the WebSocket AUTH.
async fn http_session(mut stream: TcpStream, store: Arc<Mutex<Vec<Event>>>) {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
        match find(&buf, b"\r\n\r\n") {
            Some(p) => break p + 4,
            None if buf.len() > 64 * 1024 => return,
            None => {}
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let request = lines.next().unwrap_or_default();
    let method = request.split_whitespace().next().unwrap_or_default();
    let path = request
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/");
    let mut want = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                want = v.trim().parse().unwrap_or(0);
            }
        }
    }
    let mut body = buf[head_end..].to_vec();
    while body.len() < want {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
        }
    }
    body.truncate(want);

    let (status, kind, payload) = match (method, path) {
        ("GET", "/") => ("200 OK", "application/nostr+json", nip11()),
        ("POST", "/query") => match filters_of(&body) {
            Some(filters) => {
                let events = store.lock().unwrap();
                ("200 OK", "application/json", events_json(&query(&filters, &events)))
            }
            None => ("400 Bad Request", "application/json", json!({ "message": "the body must be a JSON array of filters" }).to_string()),
        },
        ("POST", "/events") => match Event::from_json(&body) {
            Ok(event) => {
                let id = event.id.to_hex();
                match save(&store, event) {
                    Ok(()) => ("200 OK", "application/json", accepted(&id)),
                    Err(why) => ("400 Bad Request", "application/json", refused(&why)),
                }
            }
            Err(e) => ("400 Bad Request", "application/json", refused(&format!("the body is not a Nostr event: {e}"))),
        },
        _ => ("404 Not Found", "text/plain", "not found".to_owned()),
    };
    let head = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {kind}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        payload.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(payload.as_bytes()).await;
    let _ = stream.flush().await;
}

/// The events a set of filters matches, newest first, each filter limited by its
/// own `limit` (the OR the WebSocket REQ and `POST /query` both use).
fn query(filters: &[Filter], store: &[Event]) -> Vec<Event> {
    let opts = MatchEventOptions::new();
    let mut out: Vec<Event> = Vec::new();
    for f in filters {
        let mut hits: Vec<&Event> = store.iter().filter(|e| f.match_event(e, opts)).collect();
        hits.sort_by_key(|e| std::cmp::Reverse(e.created_at));
        hits.truncate(f.limit.unwrap_or(500));
        out.extend(hits.into_iter().cloned());
    }
    out
}

/// Parse a `POST /query` body — a JSON array of filters — through the same
/// `REQ` parser a WebSocket client's subscription uses.
fn filters_of(body: &[u8]) -> Option<Vec<Filter>> {
    let text = String::from_utf8_lossy(body);
    let items = text.trim().strip_prefix('[')?.strip_suffix(']')?.trim();
    let items = if items.is_empty() { "{}" } else { items };
    match ClientMessage::from_json(format!(r#"["REQ","http",{items}]"#).as_bytes()) {
        Ok(ClientMessage::Req { filters, .. }) => Some(filters.into_iter().map(|f| f.into_owned()).collect()),
        _ => None,
    }
}

fn events_json(events: &[Event]) -> String {
    let mut out = String::from("[");
    for (i, e) in events.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&e.as_json());
    }
    out.push(']');
    out
}

/// Take a client's event: check its id and signature, then keep it. An event sent again (the same id) replaces the copy
/// already held; a replaceable kind is not superseded — this is a test relay, not a real store.
fn save(store: &Mutex<Vec<Event>>, event: Event) -> Result<(), String> {
    event.verify().map_err(|e| format!("the event does not verify: {e}"))?;
    let mut events = store.lock().unwrap();
    match events.iter_mut().find(|e| e.id == event.id) {
        Some(held) => *held = event,
        None => events.push(event),
    }
    Ok(())
}

/// The answer `POST /events` gives an event it took: accepted, like the WebSocket `OK`.
fn accepted(id: &str) -> String {
    json!({ "accepted": true, "event_id": id }).to_string()
}

/// The answer `POST /events` gives an event it will not keep: the same shape, refused, like the WebSocket `OK false`.
fn refused(why: &str) -> String {
    json!({ "accepted": false, "message": why }).to_string()
}

fn nip11() -> String {
    r#"{"name":"buzz-fake-relay","description":"A tiny fake Buzz relay for exercising a Buzz client","software":"buzz-fake-relay","version":"0.1.0","supported_nips":[1,11,42,50]}"#.to_owned()
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A signed event of `kind`, as a client would send one.
    fn signed(kind: u16, content: &str) -> Event {
        EventBuilder::new(Kind::from_u16(kind), content).finalize(&keys(9)).unwrap()
    }

    #[test]
    fn a_published_event_is_kept_and_queried_back() {
        let store = Mutex::new(Vec::new());
        let event = signed(0, r#"{"name":"Scout"}"#);
        save(&store, event.clone()).unwrap();
        let filter = Filter::new().kind(Kind::from_u16(0)).author(event.pubkey);
        let hits = query(&[filter], &store.lock().unwrap());
        assert_eq!(hits.len(), 1, "the store holds just the event that was sent");
        assert_eq!(hits[0].id, event.id, "and it is the same event that comes back out");
    }

    #[test]
    fn an_event_that_does_not_verify_is_refused_and_not_kept() {
        let store = Mutex::new(Vec::new());
        let mut event = signed(0, r#"{"name":"Scout"}"#);
        event.content = r#"{"name":"Someone else"}"#.to_owned(); // the id and signature no longer match
        assert!(save(&store, event).is_err());
        assert!(store.lock().unwrap().is_empty(), "a refused event is not kept");
    }

    #[test]
    fn the_seed_gives_one_open_channel_a_roster() {
        let has_roster = |id: &str| {
            seed().iter().any(|e| {
                e.kind.as_u16() == 39002 && e.tags.iter().any(|t| t.kind() == "d" && t.content() == Some(id))
            })
        };
        assert!(has_roster("c-random"), "an open channel names its members, so a client can tell it is not in it");
        assert!(!has_roster("c-general"), "the others carry no roster, as a relay that does not publish members leaves them");
    }
}
