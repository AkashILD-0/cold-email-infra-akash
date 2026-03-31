import logging
import requests
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)


class InstantlyClient:
    """Instantly.ai API client (v2). Instantiated per-client with their API key."""

    BASE_URL = "https://api.instantly.ai/api/v2"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

    def create_campaign(self, name: str) -> dict:
        """Create a new Instantly campaign. Returns campaign dict."""
        try:
            resp = requests.post(
                f"{self.BASE_URL}/campaigns",
                headers=self._headers(),
                json={
                    "name": name,
                    "campaign_schedule": {
                        "schedules": [{
                            "name": "Default",
                            "timing": {"from": "09:00", "to": "17:00"},
                            "days": {"1": True, "2": True, "3": True, "4": True, "5": True},
                            "timezone": "America/Detroit",
                        }]
                    },
                },
                timeout=30
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Instantly create campaign error: {e}")
            return {}

    def add_leads_to_campaign(self, campaign_id: str, leads: list) -> dict:
        """Add leads to an Instantly campaign one at a time (v2 API).
        leads = [{email, first_name, last_name, company_name, custom_variables}]
        Returns {added: int, errors: int}."""
        result = {"added": 0, "errors": 0}
        for lead in leads:
            try:
                payload = {
                    "email": lead["email"],
                    "first_name": lead.get("first_name", ""),
                    "last_name": lead.get("last_name", ""),
                    "company_name": lead.get("company_name", ""),
                    "campaign": campaign_id,
                }
                # Add custom variables if present
                for key, value in lead.get("custom_variables", {}).items():
                    payload[key] = value
                resp = requests.post(
                    f"{self.BASE_URL}/leads",
                    headers=self._headers(),
                    json=payload,
                    timeout=30
                )
                resp.raise_for_status()
                result["added"] += 1
            except Exception as e:
                logger.error(f"Instantly add lead error ({lead.get('email')}): {e}")
                result["errors"] += 1
        return result

    def set_campaign_schedule(self, campaign_id: str, schedule: dict) -> dict:
        """Set sending schedule for a campaign."""
        try:
            resp = requests.patch(
                f"{self.BASE_URL}/campaigns/{campaign_id}",
                headers=self._headers(),
                json={"campaign_schedule": schedule},
                timeout=30
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Instantly schedule error: {e}")
            return {}

    def set_campaign_sequences(self, campaign_id: str, sequences: list) -> dict:
        """Set email sequences for a campaign. sequences = [{steps: [...]}]"""
        try:
            resp = requests.patch(
                f"{self.BASE_URL}/campaigns/{campaign_id}",
                headers=self._headers(),
                json={"sequences": sequences},
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
                f"{self.BASE_URL}/campaigns/{campaign_id}/activate",
                headers=self._headers(),
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
                f"{self.BASE_URL}/campaigns/{campaign_id}/pause",
                headers=self._headers(),
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
                f"{self.BASE_URL}/campaigns/{campaign_id}/analytics",
                headers=self._headers(),
                timeout=30
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Instantly analytics error: {e}")
            return {}
