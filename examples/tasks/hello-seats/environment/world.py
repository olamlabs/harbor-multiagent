"""The world: notes posted by authors, kept in memory and in /logs/world/notes.jsonl."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

NOTES_PATH = Path("/logs/world/notes.jsonl")
NOTES: list[dict[str, str]] = []
LOCK = threading.Lock()


class World(BaseHTTPRequestHandler):
    def _reply(self, status: int, body: object) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._reply(200, {"ok": True})
        elif self.path == "/notes":
            with LOCK:
                self._reply(200, list(NOTES))
        else:
            self._reply(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/note":
            self._reply(404, {"error": "not found"})
            return
        try:
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            note = {"author": str(body["author"]), "text": str(body["text"])}
        except (KeyError, TypeError, ValueError):
            self._reply(400, {"error": 'send JSON {"author": ..., "text": ...}'})
            return
        with LOCK:
            NOTES.append(note)
            with NOTES_PATH.open("a") as notes_file:
                notes_file.write(json.dumps(note) + "\n")
        self._reply(201, note)


if __name__ == "__main__":
    NOTES_PATH.parent.mkdir(parents=True, exist_ok=True)
    NOTES_PATH.touch()
    ThreadingHTTPServer(("0.0.0.0", 8080), World).serve_forever()
