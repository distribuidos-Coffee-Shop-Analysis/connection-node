#!/usr/bin/env python3

import subprocess
import sys
import time


def test_echo_server():
    """
    Test the echo server using netcat from a temporary container
    """
    try:
        # Test message to send
        test_message = "Hello Echo Server!"

        # Create a temporary container with netcat, connect to testing_net, and test the server
        cmd = [
            "docker",
            "run",
            "--rm",
            "--network",
            "tp0_testing_net",  # Connect to the same network as the server
            "busybox:latest",
            "sh",
            "-c",
            f"echo '{test_message}' | nc server 12345",
        ]

        # Run the command and capture output
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

        # Check if the command executed successfully
        if result.returncode != 0:
            print("action: test_echo_server | result: fail")
            return False

        # Check if we received our test message back
        if test_message not in result.stdout:
            print("action: test_echo_server | result: fail")
            return False

        # Check if the response contains an error (unhealthy server)
        if "error reading data:" in result.stdout:
            print("action: test_echo_server | result: fail")
            return True

        # Server echoed back our message correctly (healthy server)
        print("action: test_echo_server | result: success")
        return True

    except Exception as e:
        print("action: test_echo_server | result: fail")
        return False


if __name__ == "__main__":
    test_echo_server()
