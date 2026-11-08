# buzz-fake-relay

A tiny fake [Buzz](https://github.com/block/buzz) relay for exercising a Buzz client by hand or from a script. Not a real relay: it
issues the NIP-42 challenge, accepts any AUTH and any EVENT, serves a fixed NIP-29 data set, and answers REQs from it. Nothing is
persisted and live events are not pushed (default mode; see "Scripted mode" for HTTP clients and live echo).

```bash
cargo run -- 127.0.0.1:7447          # default address
```

## Scripted mode (HTTP clients such as buzzr)

```bash
cargo run -- --script data/community.json [127.0.0.1:8787]
```

Serves a community from a JSON script (users, channels, messages, threads, reactions) instead of the fixed seed, over the endpoints
clients that read via HTTP use: `POST /query`, `POST /events`, `GET /` (NIP-11), plus a WebSocket on the same port. Accepted writes are
echoed to live subscribers, so sending, editing and deleting from a client behave normally. Ids are not real hashes and no signature is
checked; nothing is persisted. Edit `data/community.json` or pass your own script. Point the client at `http://127.0.0.1:8787` (or
`ws://`), any private key. For [buzzr](https://github.com/vthub/buzzr):

```bash
BUZZR_CONFIG_DIR=$(mktemp -d) BUZZ_RELAY_URL=http://127.0.0.1:8787 BUZZ_PRIVATE_KEY=<any hex> buzzr
```

## Seed data (default mode)

- Channels `general`, `random`, `design`, `ops`, `dev`.
- Three legacy hidden DM rooms (peers `bob`, `alice`, `carol`).
- Profiles (kind 0) for the three peers, and one message per room.

Newest-message times are chosen so channels and DMs interleave when ordered by recent activity, which is what clients that sort by
activity show. Events are signed with fixed test keys (nothing here is secret).

## Point a client at it

Give your client `ws://127.0.0.1:7447` and any private key. Auth is not checked.

## `scripts/pty_drive.py`

Runs any full-screen terminal program in a pty, presses keys, and prints the screen after each one, so a client's UI can be checked
from a script. It answers terminal capability queries so programs that probe on startup do not stall.

```bash
python3 -m venv .venv && .venv/bin/pip install pyte
.venv/bin/python scripts/pty_drive.py --env MY_RELAY=ws://127.0.0.1:7447 esc,h,j,j,j -- /path/to/client
```
