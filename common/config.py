#!/usr/bin/env python3

import os
from configparser import ConfigParser
from dataclasses import dataclass


@dataclass
class MiddlewareConfig:
    """Configuration for RabbitMQ middleware"""
    host: str
    port: int
    username: str
    password: str


@dataclass
class ServerConfig:
    """Configuration for the server"""
    port: int
    listen_backlog: int
    logging_level: str


def initialize_config():
    """Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file.
    If at least one of the config parameters is not found a KeyError exception
    is thrown. If a parameter could not be parsed, a ValueError is thrown.
    If parsing succeeded, the function returns ServerConfig and MiddlewareConfig objects
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    try:
        # Server configuration
        server_config = ServerConfig(
            port=int(os.getenv("SERVER_PORT", config["DEFAULT"]["SERVER_PORT"])),
            listen_backlog=int(
                os.getenv(
                    "SERVER_LISTEN_BACKLOG", config["DEFAULT"]["SERVER_LISTEN_BACKLOG"]
                )
            ),
            logging_level=os.getenv(
                "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
            )
        )

        # Middleware configuration
        middleware_config = MiddlewareConfig(
            host=os.getenv("RABBITMQ_HOST", config["DEFAULT"]["RABBITMQ_HOST"]),
            port=int(os.getenv("RABBITMQ_PORT", config["DEFAULT"]["RABBITMQ_PORT"])),
            username=os.getenv("RABBITMQ_USER", config["DEFAULT"]["RABBITMQ_USER"]),
            password=os.getenv("RABBITMQ_PASSWORD", config["DEFAULT"]["RABBITMQ_PASSWORD"])
        )

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e)
        )

    return server_config, middleware_config
