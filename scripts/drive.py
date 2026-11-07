import os, pty, sys, time, select, fcntl, termios, struct, signal, subprocess
import pyte

binary, port = sys.argv[1], sys.argv[2]
home = f"/tmp/buzztui-home-{os.path.basename(binary)}"
subprocess.run(["rm", "-rf", home]); os.makedirs(home)
open(home + "/config.toml", "w").write(f'''current = "fake"
[communities.fake]
relay = "ws://127.0.0.1:{port}"
[ui]
avatars = false
[media]
inline_images = false
protocol = "halfblocks"
''')
env = dict(os.environ, BUZZTUI_HOME=home, BUZZTUI_RELAY=f"ws://127.0.0.1:{port}",
           BUZZTUI_NSEC="05" * 32, TERM="xterm-256color", BUZZTUI_LOG="buzztui=debug")
ROWS, COLS = 36, 110
pid, fd = pty.fork()
if pid == 0:
    os.execve(binary, [binary], env)
fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS, COLS, 0, 0))
screen = pyte.Screen(COLS, ROWS); stream = pyte.ByteStream(screen)

def pump(t=0.6):
    end = time.time() + t
    while time.time() < end:
        r, _, _ = select.select([fd], [], [], 0.1)
        if r:
            try: data = os.read(fd, 65536)
            except OSError: return
            if not data: return
            import re
            if b"\x1b[c" in data or b"\x1b[0c" in data: os.write(fd, b"\x1b[?6c")
            if b"\x1b[6n" in data: os.write(fd, b"\x1b[1;1R")
            if b"\x1b[16t" in data: os.write(fd, b"\x1b[6;20;10t")
            stream.feed(re.sub(rb"\x1b_.*?\x1b\\", b"", data, flags=re.S))

def send(b, t=0.5):
    os.write(fd, b); pump(t)

def dump(label):
    print(f"--- {label}")
    for line in screen.display:
        print(line.rstrip())

def sidebar_rows():
    return [l[:28].rstrip() for l in screen.display[:ROWS - 2]]

def cursor_row():
    # the row whose first cells are highlighted (reverse video / selected bg) in the sidebar
    for y in range(ROWS):
        cells = [screen.buffer[y][x] for x in range(0, 26)]
        if any(c.reverse or c.bg not in ("default",) for c in cells) and screen.display[y][:26].strip():
            return screen.display[y][:26].strip()
    return None

pump(4.0)
for _ in range(6):
    if "NAVIGATE" in screen.display[ROWS-1]: break
    send(b"\x1b", 1.0)
    if "NAVIGATE" in screen.display[ROWS-1]: break
    send(b"\x1b[27u", 1.0)
print("footer:", screen.display[ROWS-1].strip()[:40])
for _ in range(5):
    send(b"h", 0.8)
    if "CHANNELS" in screen.display[ROWS-1]: break
print("footer:", screen.display[ROWS-1].strip()[:40])
dump("sidebar focused")
def active():
    for y in range(1, 14):
        cells = [screen.buffer[y][x] for x in range(1, 24)]
        if any(c.bg != "default" or c.reverse for c in cells) and screen.display[y][:24].strip():
            return screen.display[y][:24].strip()
    return "?"
path = []
for i in range(12):
    send(b"j", 0.7)
    path.append(screen.display[0].split("›")[-1].split("●")[0].strip())
print("DOWN path:", path)
path = []
for i in range(12):
    send(b"k", 0.7)
    path.append(screen.display[0].split("›")[-1].split("●")[0].strip())
print("UP path:  ", path)
os.kill(pid, signal.SIGKILL)
