from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ValidationError

class CustomException(Exception):
    def __init__(self, message, status_code=400):
        super().__init__(message)
        self.status_code = status_code

class BillingETLJob(BaseModel):
    """Model for internal ETL job processing within the service."""
    org_id: int
    project_id: str
    dataset_id: str
    table_id: str
    target_date: Optional[str] = None

class BillingETLMessage(BaseModel):
    org_id: int
    

def validate_payload(payload: dict):
    try:
        validated_payload = BillingETLMessage(**payload)
        return True, validated_payload
    except ValidationError as e:
        return False, e.json()
    
class BillingETLStatus(BaseModel):
    """Model for tracking the status of each billing ETL run."""
    org_id: int
    project_id: str
    status: str 
    end_date_time: Optional[datetime] = None