"""
Communication protocol definition.
Defines message formats (e.g., JSON structures) used for TCP socket communication between master and workers.
"""
import json

class ProtocolException(Exception):
    pass

def send_message(sock, msg_dict):
    """
    Serializes a dictionary to JSON and sends it over the socket,
    delimited by a newline character.
    """
    try:
        serialized = json.dumps(msg_dict) + "\n"
        sock.sendall(serialized.encode('utf-8'))
    except Exception as e:
        raise ProtocolException(f"Failed to send message: {e}")

class MessageReader:
    """
    Reads newline-delimited JSON messages from a TCP socket.
    Handles partial reads and multiple messages in a stream.
    """
    def __init__(self, sock):
        self.sock = sock
        self.buffer = ""

    def read_message(self):
        """
        Reads from the socket until a newline is found.
        Returns the parsed JSON dictionary, or None if connection is closed.
        """
        while "\n" not in self.buffer:
            try:
                data = self.sock.recv(4096)
                if not data:
                    # Connection closed
                    return None
                self.buffer += data.decode('utf-8')
            except Exception as e:
                raise ProtocolException(f"Socket read error: {e}")
        
        line, self.buffer = self.buffer.split("\n", 1)
        try:
            return json.loads(line)
        except json.JSONDecodeError as e:
            raise ProtocolException(f"Invalid JSON received: {e}")
