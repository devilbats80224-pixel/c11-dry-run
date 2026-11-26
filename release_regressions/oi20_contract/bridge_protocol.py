"""Bounded newline frames over managed Docker attach pipes."""
import json
import os
import select
import time

MAX_FRAME = 64 * 1024


class Refused(RuntimeError):
    pass


def need(ok, reason):
    if not ok:
        raise Refused(reason)


class Pipe:
    def __init__(self, read_fd, write_fd):
        self.read_fd, self.write_fd = read_fd, write_fd
        self.buffer = b''

    def send(self, value):
        data = json.dumps(value, separators=(',', ':'), allow_nan=False).encode() + b'\n'
        need(len(data) <= MAX_FRAME, 'C11_BRIDGE_FRAME_LIMIT')
        while data:
            count = os.write(self.write_fd, data)
            data = data[count:]

    def receive(self, timeout):
        deadline = time.monotonic() + timeout
        while b'\n' not in self.buffer:
            remaining = deadline - time.monotonic()
            need(remaining > 0 and select.select([self.read_fd], [], [], remaining)[0],
                 'C11_BRIDGE_TIMEOUT')
            data = os.read(self.read_fd, min(8192, MAX_FRAME - len(self.buffer)))
            need(data, 'C11_BRIDGE_CLOSED_OR_OVERSIZE')
            self.buffer += data
        raw, self.buffer = self.buffer.split(b'\n', 1)
        return json.loads(raw)
