import logging
from validation.leadmagic_client import validate_email as lm_validate
from validation.million_verifier_client import validate_email as mv_validate
from db import update_lead_fields

logger = logging.getLogger(__name__)


def validate_lead_email(lead: dict, campaign_id: str) -> dict:
    """Two-pass email validation cascade. Returns {email_verdict, lm_status, lm_catchall, mv_result, mv_quality_score}."""

    lead_id = str(lead["lead_id"])
    email = lead.get("email")

    if not email:
        return {"email_verdict": None}

    result = {
        "email_verdict": None,
        "lm_status": None,
        "lm_catchall": None,
        "mv_result": None,
        "mv_quality_score": None,
    }

    # Pass 1: LeadMagic
    lm_result = lm_validate(email, campaign_id=campaign_id, lead_id=lead_id)
    result["lm_status"] = lm_result.get("status")
    result["lm_catchall"] = lm_result.get("is_catchall", False)

    if lm_result["status"] == "valid" and not lm_result.get("is_catchall"):
        result["email_verdict"] = "SEND"
    elif lm_result["status"] == "valid" and lm_result.get("is_catchall"):
        result["email_verdict"] = "RISKY"
    elif lm_result["status"] == "invalid":
        result["email_verdict"] = "DO NOT SEND"
    else:
        # UNVERIFIED — pass to Million Verifier
        mv_result = mv_validate(email, campaign_id=campaign_id, lead_id=lead_id)
        result["mv_result"] = mv_result.get("result")
        result["mv_quality_score"] = mv_result.get("quality_score")

        if mv_result["result"] == "ok":
            result["email_verdict"] = "SEND"
        elif mv_result["result"] in ("invalid", "disposable"):
            result["email_verdict"] = "DO NOT SEND"
        elif mv_result["result"] == "catch_all":
            result["email_verdict"] = "RISKY"
        else:
            result["email_verdict"] = "UNVERIFIED"

    # Update lead in DB
    update_lead_fields(lead_id, result)

    logger.info(f"Validation for {email}: {result['email_verdict']} (LM={result['lm_status']}, MV={result['mv_result']})")
    return result
