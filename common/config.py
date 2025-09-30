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
    users_joiners_count: int


@dataclass
class ServerConfig:
    """Configuration for the server"""

    port: int
    listen_backlog: int
    logging_level: str


def initialize_config():
    """Parse config file to find program config params

    Function that searches for program configuration parameters in the config.ini file.
    Environment variables take precedence over config file values.
    If at least one of the config parameters is not found a KeyError exception
    is thrown. If a parameter could not be parsed, a ValueError is thrown.
    If parsing succeeded, the function returns ServerConfig and MiddlewareConfig objects
    """

    config = ConfigParser()

    # Read config file - raise error if it doesn't exist or can't be read
    config_files_read = config.read("config.ini")
    if not config_files_read:
        raise KeyError("Configuration file 'config.ini' not found or could not be read")

    def _get_required_config(env_key, config_key):
        """Get configuration value from environment variable or config file, raise error if missing"""
        # Environment variables take precedence
        env_value = os.getenv(env_key)
        if env_value is not None:
            return env_value

        # Try to get from config file
        try:
            return config["DEFAULT"][config_key]
        except KeyError:
            raise KeyError(
                f"Required configuration parameter '{config_key}' not found in environment variable '{env_key}' or config file"
            )

    try:
        # Server configuration
        server_config = ServerConfig(
            port=int(_get_required_config("SERVER_PORT", "SERVER_PORT")),
            listen_backlog=int(
                _get_required_config("SERVER_LISTEN_BACKLOG", "SERVER_LISTEN_BACKLOG")
            ),
            logging_level=_get_required_config("LOGGING_LEVEL", "LOGGING_LEVEL"),
        )

        # Middleware configuration
        middleware_config = MiddlewareConfig(
            host=_get_required_config("RABBITMQ_HOST", "RABBITMQ_HOST"),
            port=int(_get_required_config("RABBITMQ_PORT", "RABBITMQ_PORT")),
            username=_get_required_config("RABBITMQ_USER", "RABBITMQ_USER"),
            password=_get_required_config("RABBITMQ_PASSWORD", "RABBITMQ_PASSWORD"),
            users_joiners_count=int(_get_required_config("USERS_JOINERS_COUNT", "USERS_JOINERS_COUNT")),
        )

    except KeyError as e:
        raise KeyError("Configuration error: {}. Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Configuration parsing error: {}. Aborting server".format(e))

    return server_config, middleware_config
