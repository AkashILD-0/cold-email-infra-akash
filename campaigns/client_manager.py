import logging
from db import create_client, get_client

logger = logging.getLogger(__name__)


def get_client_api_key(client_id: str) -> str:
    """Get Instantly API key for a client."""
    client = get_client(client_id)
    if not client:
        raise ValueError(f"Client {client_id} not found")
    return client["instantly_api_key"]


def get_client_domains(client_id: str) -> list:
    """Get sending domains for a client."""
    client = get_client(client_id)
    if not client:
        raise ValueError(f"Client {client_id} not found")
    return client.get("sending_domains", [])
