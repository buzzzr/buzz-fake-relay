#!/usr/bin/env python3
"""Run a full-screen terminal program in a pty, press keys, and print what is on screen.

    pty_drive.py [--env K=V]... [--size ROWSxCOLS] [--wait SECS] KEY[,KEY...] -- COMMAND [ARGS...]

KEYs are names (esc, enter, tab, up, down, left, right, bs, space) or literal text (j, h, hello). After each key the screen
is printed, so a script can read what happened. The program is killed at the end. Terminal capability queries (DA1, cursor
position, kitty graphics probe) are answered so programs that probe on startup do not stall.

Needs `pip install pyte`.
"""
import fcntl, os, pty, re, select, signal, struct, sys, termios, time
import pyte

NAMED = {"esc": b"\x1b", "enter": b"\r", "tab": b"\t", "up": b"\x1b[A", "down": b"\x1b[B", "right": b"\x1b[C",
         "left": b"\x1b[D", "bs": b"\x7f", "space": b" "}

def main(argv):
    env, rows, cols, wait = dict(os.environ, TERM="xterm-256color"), 36, 110, 4.0
    args = list(argv)
    while args and args[0].startswith("--"):
        flag = args.pop(0)
        if flag == "--":
            break
        val = args.pop(0)
        if flag == "--env":
            k, v = val.split("=", 1); env[k] = v
        elif flag == "--size":
            rows, cols = map(int, val.lower().split("x"))
        elif flag == "--wait":
            wait = float(val)
    if "--" in args:
        i = args.index("--"); keys, cmd = args[:i], args[i + 1:]
    else:
        keys, cmd = [], args
    if not cmd:
        sys.exit(__doc__)
    keys = [k for spec in keys for k in spec.split(",") if k]

    pid, fd = pty.fork()
    if pid == 0:
        os.execvpe(cmd[0], cmd, env)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
    screen = pyte.Screen(cols, rows); stream = pyte.ByteStream(screen)

    def pump(t):
        end = time.time() + t
        while time.time() < end:
            if not select.select([fd], [], [], 0.1)[0]:
                continue
            try: data = os.read(fd, 65536)
            except OSError: return
            if not data: return
            if b"\x1b[c" in data or b"\x1b[0c" in data: os.write(fd, b"\x1b[?6c")
            if b"\x1b[6n" in data: os.write(fd, b"\x1b[1;1R")
            if b"\x1b[16t" in data: os.write(fd, b"\x1b[6;20;10t")
            stream.feed(re.sub(rb"\x1b_.*?\x1b\\", b"", data, flags=re.S))

    def show(label):
        print(f"--- {label}")
        for line in screen.display:
            print(line.rstrip())

    pump(wait)
    show("start")
    for k in keys:
        os.write(fd, NAMED.get(k, k.encode())); pump(0.7)
        show(f"after {k}")
    os.kill(pid, signal.SIGKILL)

if __name__ == "__main__":
    main(sys.argv[1:])
