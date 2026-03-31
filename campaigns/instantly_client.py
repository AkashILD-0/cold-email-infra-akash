import logging
import requests
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)


class InstantlyClient:
    """Instantly.ai API client. Instantiated per-client with their API key."""

    BASE_URL = "https://api.instantly.ai/api/v1"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def _params(self, extra: dict = None) -> dict:
        params = {"api_key": self.api_key}
        if extra:
            params.update(extra)
        return params

    def create_campaign(self, name: str) -> dict:
        """Create a new Instantly campaign. Returns campaign dict."""
        try:
            resp = requests.post(
                f"{self.BASE_URL}/campaign/create",
                json={"api_key": self.api_key, "name": name},
                timeout=30
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Instantly create campaign error: {e}")
            return {}

    def add_leads_to_campaign(self, campaign_id: str, leads: list) -> dict:
        """Add leads to an Instantly campaign. leads = [{email, first_name, last_name, ...}]"""
        try:
            resp = requests.post(
                f"{self.BASE_URL}/lead/add",
                json={
                    "api_key": self.api_key,
                    "campaign_id": campaign_id,
                    "leads": leads,
                },
                timeout=60
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Instantly add leads error: {e}")
            return {}

    def set_campaign_schedule(self, campaign_id: str, schedule: dict) -> dict:
        """Set sending schedule for a campaign."""
        try:
            resp = requests.post(
                f"{self.BASE_URL}/campaign/set-schedule",
                json={
                    "api_key": self.api_key,
                    "campaign_id": campaign_id,
                    **schedule,
                },
                timeout=30
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Instantly schedule error: {e}")
            return {}

    def set_campaign_sequences(self, campaign_id: str, sequences: list) -> dict:
        """Set email sequences for a campaign. sequences = [{subject, body, delay}]"""
        try:
            resp = requests.post(
                f"{self.BASE_URL}/campaign/set-sequences",
                json={
                    "api_key": self.api_key,
                    "campaign_id": campaign_id,
                    "sequences": sequences,
                },
                timeout=30
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Instantly sequences error: {e}")
            return {}

    def activate_campaign(self, campaign_id: str) -> dict:
        """Activate (start sending) a campaign."""
        try:
            resp = requests.post(
                f"{self.BASE_URL}/campaign/activate",
                json={"api_key": self.api_key, "campaign_id": campaign_id},
                timeout=30
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Instantly activate error: {e}")
            return {}

    def pause_campaign(self, campaign_id: str) -> dict:
        """Pause a campaign."""
        try:
            resp = requests.post(
                f"{self.BASE_URL}/campaign/pause",
                json={"api_key": self.api_key, "campaign_id": campaign_id},
                timeout=30
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Instantly pause error: {e}")
            return {}

    def get_campaign_analytics(self, campaign_id: str) -> dict:
        """Get campaign analytics/metrics."""
        try:
            resp = requests.get(
                f"{self.BASE_URL}/analytics/campaign/summary",
                params=self._params({"campaign_id": campaign_id}),
                timeout=30
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Instantly analytics error: {e}")
            return {}
