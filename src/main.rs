//! A tiny fake Buzz relay for exercising any Buzz client by hand or from a script.
//!
//!   cargo run -- 127.0.0.1:7447
//!
//! It issues the NIP-42 challenge, accepts any AUTH and any EVENT, serves a
//! fixed set of NIP-29 channels and legacy hidden DM rooms, and answers REQs
//! from that store. Newest-message times are chosen so channels and DMs
//! interleave when ordered by recent activity.
//!
//! With `--script FILE [ADDR]` it instead serves a scripted community over HTTP `/query` and `/events` plus a WebSocket that pushes
//! accepted writes (see `scripted.rs`), for clients like `buzzr`.
mod scripted;

use futures_util::{SinkExt, StreamExt};
use nostr::prelude::*;
use std::sync::Arc;
use tokio::net::TcpListener;
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
    let people = [(2u8, "alice"), (3, "bob"), (4, "carol")];
    let now = Timestamp::now().as_secs();
    let mut out = Vec::new();
    for (s, name) in people {
        let k = keys(s);
        out.push(sign(
            &k,
            0,
            &format!(r#"{{"name":"{name}"}}"#),
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
    out
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let first = args.next();
    if first.as_deref() == Some("--script") {
        let Some(file) = args.next() else {
            eprintln!("usage: buzz-fake-relay --script FILE [ADDR]");
            std::process::exit(2);
        };
        let addr = args.next().unwrap_or_else(|| "127.0.0.1:8787".into());
        if let Err(e) = scripted::run(&file, &addr).await {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
        return;
    }
    let addr = first.unwrap_or_else(|| "127.0.0.1:7447".into());
    let store = Arc::new(seed());
    let listener = TcpListener::bind(&addr).await.unwrap();
    eprintln!("fake relay on ws://{addr} with {} events", store.len());
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let store = store.clone();
        tokio::spawn(async move {
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
                        let _ = tx
                            .send(Ws::text(RelayMessage::ok(e.id, true, "").as_json()))
                            .await;
                    }
                    ClientMessage::Req {
                        subscription_id,
                        filters,
                    } => {
                        let opts = MatchEventOptions::new();
                        for f in &filters {
                            let mut hits: Vec<&Event> =
                                store.iter().filter(|e| f.match_event(e, opts)).collect();
                            hits.sort_by_key(|e| std::cmp::Reverse(e.created_at));
                            hits.truncate(f.limit.unwrap_or(500));
                            for e in hits {
                                let m = RelayMessage::event(
                                    subscription_id.clone().into_owned(),
                                    e.clone(),
                                );
                                let _ = tx.send(Ws::text(m.as_json())).await;
                            }
                        }
                        let m = RelayMessage::eose(subscription_id.into_owned());
                        let _ = tx.send(Ws::text(m.as_json())).await;
                    }
                    _ => {}
                }
            }
        });
    }
}
