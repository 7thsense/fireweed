#!/usr/bin/env python3

import io
import socket
import threading
import time
import unittest

import resp_one_frame


class RespOneFrameTests(unittest.TestCase):
    def test_reads_every_response_shape_used_by_failover_harness(self) -> None:
        frames = [
            b"+OK\r\n",
            b"-MOVED 4515 10.0.0.2:8080\r\n",
            b":4\r\n",
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
