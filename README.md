# buzz-tui-fake-relay

A tiny fake Buzz relay for exercising [buzz-tui](https://github.com/vthub/buzz-tui) by hand or from a script. Not a real relay: it
issues the NIP-42 challenge, accepts any AUTH and any EVENT, serves a fixed NIP-29 data set, and answers REQs from it.

```bash
cargo run -- 127.0.0.1:7447          # default address
```

Seed data: channels `general random design ops dev` and three legacy hidden DM rooms (`bob`, `alice`, `carol`). Newest-message
times are chosen so channels and DMs interleave when ordered by recent activity (which is how buzz-tui's store orders them).
Events are signed with fixed test keys, so nothing here is secret.

## Run buzz-tui against it

```bash
BUZZTUI_HOME=$(mktemp -d) BUZZTUI_RELAY=ws://127.0.0.1:7447 BUZZTUI_NSEC=$(printf '05%.0s' {1..32}) buzztui
```

## Drive the UI from a script

`scripts/drive.py` runs a buzztui binary in a pty, answers the terminal capability queries, presses `Esc`, `h` (focus the sidebar)
and then Down/Up repeatedly, and prints the channel opened after each key. Used to check that the cursor walks the sidebar in
the order it is drawn.

```bash
python3 -m venv .venv && .venv/bin/pip install pyte
.venv/bin/python scripts/drive.py /path/to/buzztui 7447
```
