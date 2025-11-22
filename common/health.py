"""
Health check TCP server for monitoring container health
"""
import socket
import logging
import threading

PING_MESSAGE = b"PING"
PONG_MESSAGE = b"PONG"


def handle_health_check(client_socket):
    """
    Handles a single health check connection
    Reads PING and responds with PONG
    """
    try:
        # Receive data from client
        data = client_socket.recv(len(PING_MESSAGE))
        
        if data == PING_MESSAGE:
            # Send PONG response
            client_socket.sendall(PONG_MESSAGE)
            logging.info("action: health_check | result: success | msg: responded to PING with PONG")
        else:
            logging.warning(
                "action: health_check | result: fail | msg: unexpected message: %s",
                data.decode("utf-8", errors="replace")
            )
    except Exception as e:
        logging.error("action: handle_health_check | result: fail | error: %s", e)
    finally:
        client_socket.close()


def run_health_server(port):
    """
    Starts a TCP health check server on the specified port
    Listens for PING messages and responds with PONG
    This function blocks indefinitely and should be run in a separate thread
    """
    host = "0.0.0.0"
    
    try:
        # Create socket
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Set socket options to allow address reuse (important for container restarts)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind to address
        server_socket.bind((host, port))
        
        # Listen for connections
        server_socket.listen(5)
        
        logging.info(
            "action: start_health_server | result: success | address: %s:%d",
            host,
            port
        )
        
        # Accept connections indefinitely
        while True:
            client_socket, client_address = server_socket.accept()
            
            # Handle each connection in a separate thread
            client_thread = threading.Thread(
                target=handle_health_check,
                args=(client_socket,),
                daemon=True
            )
            client_thread.start()
            
    except OSError as e:
        logging.error(
            "action: start_health_server | result: fail | error: failed to bind to %s:%d: %s",
            host,
            port,
            e
        )
        raise
    except Exception as e:
        logging.error("action: health_server | result: fail | error: %s", e)
        raise
    finally:
        if 'server_socket' in locals():
            server_socket.close()

