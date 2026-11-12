# buzz-fake-relay

A tiny fake [Buzz](https://github.com/block/buzz) relay for exercising a Buzz client by hand or from a script. Not a real relay: it
issues the NIP-42 challenge, accepts any AUTH and any EVENT, serves a fixed NIP-29 data set, and answers REQs from it. Nothing is
persisted and live events are not pushed.

```bash
cargo run -- 127.0.0.1:7447          # default address
```

## What it speaks

Both halves of the relay's surface are served on the one port, chosen by looking at the first bytes of
each connection (a WebSocket upgrade carries `Sec-WebSocket-Key`; anything else is plain HTTP):

- **WebSocket** — NIP-42 challenge, any AUTH → `OK`, any EVENT → `OK`, `REQ` → the store, then `EOSE`.
- **HTTP** — `GET /` (the NIP-11 document), `POST /query` (the same store filtered by the request's
  Nostr filters, as a JSON array) and `POST /events` (`{"accepted":true,...}`). NIP-98 headers are
  ignored, like the WebSocket AUTH. This is the surface the `buzz` CLI and clients built on it read
  history and send on.

## Seed data

- Channels `general`, `random`, `design`, `ops`, `dev`.
- Three legacy hidden DM rooms (peers `bob`, `alice`, `carol`).
- Profiles (kind 0) for the three peers, each with a name and an about line, and one message per room.
- A short thread in `general`: its message plus two replies, so a client can exercise opening a thread.

Newest-message times are chosen so channels and DMs interleave when ordered by recent activity, which is what clients that sort by
activity show. Events are signed with fixed test keys (nothing here is secret).

## Point a client at it

Give your client `http://127.0.0.1:7447` (or `ws://127.0.0.1:7447`, if it only uses the socket) and any private key. Auth is not
checked.

## `scripts/pty_drive.py`

Runs any full-screen terminal program in a pty, presses keys, and prints the screen after each one, so a client's UI can be checked
from a script. It answers terminal capability queries so programs that probe on startup do not stall.

```bash
python3 -m venv .venv && .venv/bin/pip install pyte
.venv/bin/python scripts/pty_drive.py --env MY_RELAY=ws://127.0.0.1:7447 esc,h,j,j,j -- /path/to/client
```
