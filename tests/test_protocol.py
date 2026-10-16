import unittest
import socket
import threading
from common.protocol import send_message, MessageReader, ProtocolException

class TestProtocol(unittest.TestCase):
    def setUp(self):
        # Create a connected pair of sockets for testing locally
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.bind(('127.0.0.1', 0))
        self.server_sock.listen(1)
        self.port = self.server_sock.getsockname()[1]
        
        self.client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_sock.connect(('127.0.0.1', self.port))
        self.conn, _ = self.server_sock.accept()

    def tearDown(self):
        self.client_sock.close()
        self.conn.close()
        self.server_sock.close()

    def test_send_and_read_message(self):
        msg = {"type": "TEST", "value": 123}
        send_message(self.client_sock, msg)
        
        reader = MessageReader(self.conn)
        received = reader.read_message()
        self.assertEqual(received, msg)

    def test_partial_reads(self):
        # Simulate partial sends (e.g., packets being split)
        msg_str = '{"type": "PARTIAL", "data": "hello"}\n'
        self.client_sock.sendall(msg_str[:10].encode('utf-8'))
        
        reader = MessageReader(self.conn)
        
        # Send the rest after a tiny delay
        def send_rest():
            self.client_sock.sendall(msg_str[10:].encode('utf-8'))
            
        threading.Timer(0.1, send_rest).start()
        
        received = reader.read_message()
        self.assertEqual(received, {"type": "PARTIAL", "data": "hello"})

    def test_multiple_messages(self):
        # Simulates multiple messages arriving at once in the buffer
        msg1 = {"id": 1}
        msg2 = {"id": 2}
        
        send_message(self.client_sock, msg1)
        send_message(self.client_sock, msg2)
        
        reader = MessageReader(self.conn)
        self.assertEqual(reader.read_message(), msg1)
        self.assertEqual(reader.read_message(), msg2)

if __name__ == "__main__":
    unittest.main()
