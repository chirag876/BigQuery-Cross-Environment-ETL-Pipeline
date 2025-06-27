import base64
import json
import traceback
from typing import Tuple

from core.models.billing_etl_model import CustomException, validate_payload
from core.services.billing_etl import process_etl_job
from core.utility import logger


def main(request) -> Tuple[str, int]:
    try:
        logger.info_log("[New HTTP request received!]")

        if request.headers.get("content-type") != "application/json":
            raise CustomException("Invalid request: Content-Type must be application/json", 415)

        body = request.get_data()
        envelope = json.loads(body)

        if "message" not in envelope:
            raise CustomException("Invalid JSON payload: 'message' field missing", 400)

        message = envelope["message"]
        if "data" not in message:
            raise CustomException("Invalid Pub/Sub message format: 'data' field missing", 400)

        try:
            decoded_bytes = base64.b64decode(message["data"])
            decoded_str = decoded_bytes.decode("utf-8")
            decoded_message = json.loads(decoded_str)
            logger.info_log(f"Decoded message: {decoded_message}")
        except Exception as e:
            raise CustomException(f"Invalid Base64 or JSON in 'data' field: {e}", 400)

        success, validated_message = validate_payload(decoded_message)
        if not success:
            raise CustomException(f"Payload validation failed => {validated_message}", 400)

        process_etl_job(pubsub_message=validated_message.model_dump())
        logger.info_log("[Success] Message processed successfully!")

        return "Success", 200

    except CustomException as e:
        logger.error_log(f"[Bad Request] => {e.message}")
        return str(e.message), e.status_code

    except Exception as e:
        traceback.print_exc()
        logger.error_log(f"[Unhandled Error] => {e}")
        return "Internal Server Error", 500
