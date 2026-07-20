#!/usr/bin/env python3
"""Send one RESP request and read exactly one complete RESP2 response frame."""

from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import BinaryIO, Iterable


class TransportError(RuntimeError):
    """The peer closed before one complete RESP frame arrived."""


def _read_exact(stream: BinaryIO, length: int, context: str) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            received = length - remaining
            raise TransportError(
                f"transport_truncated context={context} expected={length} received={received}"
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_header(stream: BinaryIO, kind: bytes) -> bytes:
    header = stream.readline()
    if not header:
        raise TransportError(f"transport_eof context={kind.decode()}_header")
    if not header.endswith(b"\r\n"):
        raise TransportError(
            f"transport_truncated context={kind.decode()}_header missing_crlf=true"
        )
    return header


def read_one_frame(stream: BinaryIO) -> bytes:
    """Read one recursively complete RESP2 frame without an elapsed-time gate."""

    kind = stream.read(1)
    if not kind:
        raise TransportError("transport_eof context=frame_prefix")
    if kind not in {b"+", b"-", b":", b"$", b"*"}:
        raise TransportError(f"transport_invalid context=frame_prefix value={kind!r}")
    header = _read_header(stream, kind)
    frame = bytearray(kind + header)
    if kind in {b"+", b"-", b":"}:
        return bytes(frame)

    try:
        count = int(header[:-2])
    except ValueError as error:
        raise TransportError(
            f"transport_invalid context={kind.decode()}_length value={header[:-2]!r}"
        ) from error
    if count == -1:
        return bytes(frame)
    if count < -1:
        raise TransportError(
            f"transport_invalid context={kind.decode()}_length value={count}"
        )
    if kind == b"$":
        body = _read_exact(stream, count + 2, "bulk_body")
        if not body.endswith(b"\r\n"):
            raise TransportError("transport_invalid context=bulk_body missing_crlf=true")
        frame.extend(body)
        return bytes(frame)

    for _ in range(count):
        frame.extend(read_one_frame(stream))
    return bytes(frame)


def encode_request(args: Iterable[str]) -> bytes:
    encoded = [arg.encode() for arg in args]
    parts = [f"*{len(encoded)}\r\n".encode()]
    for arg in encoded:
        parts.extend((f"${len(arg)}\r\n".encode(), arg, b"\r\n"))
    return b"".join(parts)


def request_one_frame(port: int, args: Iterable[str]) -> bytes:
    """Use OS transport failure semantics; framing, not silence, ends the response."""

    with socket.create_connection(("127.0.0.1", port)) as connection:
        connection.sendall(encode_request(args))
        return read_one_frame(connection.makefile("rb"))


def main() -> None:
    port = int(os.environ["RESP_PORT"])
    output = Path(os.environ["RESP_OUT"])
    args = os.environ["RESP_ARGS"].splitlines()
    try:
        output.write_bytes(request_one_frame(port, args))
    except TransportError as error:
        raise SystemExit(f"resp_one_frame: classification=transport_error {error}") from error


if __name__ == "__main__":
    main()
