from .messages import (
    BetMessage,
    BatchMessage,
    GetWinnersMessage,
    MESSAGE_TYPE_BET,
    MESSAGE_TYPE_BATCH,
    MESSAGE_TYPE_GET_WINNERS,
    MESSAGE_TYPE_RESPONSE,
)

BUFFER_SIZE = 4096


def read_packet_from(client_socket):
    """Read length-prefixed packet and parse message"""
    # Read length prefix (4 bytes)
    length_data = _read_exact(client_socket, 4)
    length = int.from_bytes(length_data, byteorder="big")
    # Read message data
    data = _read_exact(client_socket, length)

    # Parse based on message type
    if len(data) < 1:
        raise ValueError("Empty message")

    msg_type = data[0]

    if msg_type == MESSAGE_TYPE_BET:
        return BetMessage.from_data(data)
    elif msg_type == MESSAGE_TYPE_BATCH:
        return BatchMessage.from_data(data)
    elif msg_type == MESSAGE_TYPE_GET_WINNERS:
        return GetWinnersMessage.from_data(data)
    else:
        raise ValueError(f"Unknown message type: {msg_type}")


def send_response(client_socket, success, error=None, winners=None):
    """Send response message using custom protocol"""
    # Build response data
    data = bytearray()
    data.append(MESSAGE_TYPE_RESPONSE)

    # Add success flag
    data.extend(("1" if success else "0").encode("utf-8"))
    data.extend(b"|")

    # Add error if present
    if error:
        data.extend(error.encode("utf-8"))
    data.extend(b"|")

    # Add winners if present
    if winners:
        data.extend(str(len(winners)).encode("utf-8"))
        data.extend(b"|")
        for winner in winners:
            data.extend(winner.encode("utf-8"))
            data.extend(b"|")
    else:
        data.extend(b"0|")

    # Send length-prefixed message
    length = len(data)
    length_bytes = length.to_bytes(4, byteorder="big")

    _send_exact(client_socket, length_bytes)
    _send_exact(client_socket, data)


def _send_exact(sock, data):
    """Send exactly all bytes in data (prevents short write)"""
    total_sent = 0
    while total_sent < len(data):
        sent = sock.send(data[total_sent:])
        if sent == 0:
            raise RuntimeError("Socket connection broken")
        total_sent += sent


def _read_exact(sock, n):
    """Read exactly n bytes from socket"""
    chunks = []
    bytes_read = 0
    while bytes_read < n:
        chunk = sock.recv(n - bytes_read)
        if not chunk:  # Connection closed
            raise RuntimeError("Socket connection broken")
        chunks.append(chunk)
        bytes_read += len(chunk)
    return b"".join(chunks)
