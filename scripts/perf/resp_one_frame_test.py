#!/usr/bin/env python3

import io
import socket
import threading
import time
import unittest
from unittest import mock

import resp_one_frame


class RaisingStream:
    def read(self, _length: int) -> bytes:
        raise ConnectionResetError("peer reset")

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None


class ResetConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def sendall(self, _request: bytes) -> None:
        return None

    def makefile(self, _mode: str) -> RaisingStream:
        return RaisingStream()


class SendFailureConnection(ResetConnection):
    def sendall(self, _request: bytes) -> None:
        raise BrokenPipeError("peer closed before request")


class RespOneFrameTests(unittest.TestCase):
    def test_reads_every_response_shape_used_by_failover_harness(self) -> None:
        frames = [
            b"+OK\r\n",
            b"-MOVED 4515 10.0.0.2:8080\r\n",
            b":0\r\n",
            b":-4\r\n",
            b"$5\r\nvalue\r\n",
            b"$-1\r\n",
            b"*3\r\n:1\r\n$3\r\ntwo\r\n*2\r\n+X\r\n$-1\r\n",
            b"*0\r\n",
            b"*-1\r\n",
        ]
        for frame in frames:
            with self.subTest(frame=frame):
                stream = io.BytesIO(frame + b"trailing bytes belong to a future frame")
                self.assertEqual(resp_one_frame.read_one_frame(stream), frame)

    def test_eof_and_truncation_are_typed_transport_errors(self) -> None:
        cases = [b"", b"$5\r\nabc", b"*2\r\n+OK\r\n", b"+missing-crlf"]
        for body in cases:
            with self.subTest(body=body):
                with self.assertRaisesRegex(
                    resp_one_frame.TransportError, "transport_(eof|truncated)"
                ):
                    resp_one_frame.read_one_frame(io.BytesIO(body))

    def test_rejects_noncanonical_or_control_bearing_headers(self) -> None:
        invalid = [
            b":+1\r\n",
            b":01\r\n",
            b":-0\r\n",
            b":1 2\r\n",
            b":\t1\r\n",
            b"+bad\ttext\r\n",
            b"-bad\x7ftext\r\n",
            b"$01\r\nx\r\n",
            b"*+1\r\n+X\r\n",
        ]
        for frame in invalid:
            with self.subTest(frame=frame):
                with self.assertRaisesRegex(
                    resp_one_frame.TransportError, "transport_invalid"
                ):
                    resp_one_frame.read_one_frame(io.BytesIO(frame))

    def test_enforces_every_resource_cap(self) -> None:
        cases = [
            (b"+123456\r\n", resp_one_frame.Limits(header_bytes=5)),
            (b"$4\r\ndata\r\n", resp_one_frame.Limits(bulk_bytes=3)),
            (b"*2\r\n+X\r\n+Y\r\n", resp_one_frame.Limits(array_elements=1)),
            (b"*1\r\n*1\r\n+X\r\n", resp_one_frame.Limits(recursion_depth=1)),
            (b"$4\r\ndata\r\n", resp_one_frame.Limits(frame_bytes=9)),
        ]
        for frame, limits in cases:
            with self.subTest(frame=frame, limits=limits):
                with self.assertRaisesRegex(
                    resp_one_frame.TransportError, "transport_limit"
                ):
                    resp_one_frame.read_one_frame(io.BytesIO(frame), limits)

    def test_connect_refusal_and_read_reset_are_transport_errors(self) -> None:
        with mock.patch.object(
            resp_one_frame.socket,
            "create_connection",
            side_effect=ConnectionRefusedError("refused"),
        ):
            with self.assertRaisesRegex(
                resp_one_frame.TransportError, "operation=connect"
            ):
                resp_one_frame.request_one_frame(1, ["PING"])

        with mock.patch.object(
            resp_one_frame.socket,
            "create_connection",
            return_value=SendFailureConnection(),
        ):
            with self.assertRaisesRegex(resp_one_frame.TransportError, "operation=send"):
                resp_one_frame.request_one_frame(1, ["PING"])

        with mock.patch.object(
            resp_one_frame.socket,
            "create_connection",
            return_value=ResetConnection(),
        ):
            with self.assertRaisesRegex(resp_one_frame.TransportError, "operation=read"):
                resp_one_frame.request_one_frame(1, ["PING"])

    def test_delayed_chunk_delivery_beyond_one_second_is_not_end_of_frame(self) -> None:
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        response = b"*2\r\n$5\r\nfirst\r\n$6\r\nsecond\r\n"

        def serve() -> None:
            with listener:
                connection, _ = listener.accept()
                with connection:
                    connection.recv(4096)
                    time.sleep(1.1)
                    connection.sendall(response[:9])
                    time.sleep(0.2)
                    connection.sendall(response[9:])

        server = threading.Thread(target=serve)
        server.start()
        try:
            self.assertEqual(
                resp_one_frame.request_one_frame(port, ["XLEN", "t1:q1"]), response
            )
        finally:
            server.join(timeout=5)
        self.assertFalse(server.is_alive())


if __name__ == "__main__":
    unittest.main()
