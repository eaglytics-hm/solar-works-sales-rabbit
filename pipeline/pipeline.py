from typing import Callable, Optional
from dataclasses import dataclass
import json

def safe_round(x: Optional[float]) -> Optional[float]:
    return round(x, 6) if x else None

@dataclass(frozen=True)
class Pipeline:
    table: str
    endpoint: str
    transform: Callable[[list[dict]], list[dict]]
    schema: list[dict]
    p_key: list[str]
    incre_key: str


leads = Pipeline(
    "Leads",
    "leads",
    lambda rows: [
        {
            "id": row.get("id"),
            "userName": row.get("userName"),
            "userId": row.get("userId"),
            "firstName": row.get("firstName"),
            "lastName": row.get("lastName"),
            "phonePrimary": row.get("phonePrimary"),
            "phoneAlternate": row.get("phoneAlternate"),
            "email": row.get("email"),
            "street1": row.get("street1"),
            "street2": row.get("street2"),
            "city": row.get("city"),
            "state": row.get("state"),
            "zip": row.get("zip"),
            "country": row.get("country"),
            "latitude": safe_round(row.get("latitude")),
            "longitude": safe_round(row.get("longitude")),
            "status": row.get("status"),
            "notes": row.get("notes"),
            "customFields": json.dumps(row.get("customFields")),
            "businessName": row.get("businessName"),
            "appointment": row.get("appointment"),
            "integrationData": json.dumps(row.get("integrationData")),
            "statusModified": row.get("statusModified"),
            "ownerModified": row.get("ownerModified"),
            "dateCreated": row.get("dateCreated"),
            "dateModified": row.get("dateModified"),
        }
        for row in [i for j in rows for i in j]
    ],
    [
        {"name": "id", "type": "NUMERIC"},
        {"name": "userName", "type": "STRING"},
        {"name": "userId", "type": "NUMERIC"},
        {"name": "firstName", "type": "STRING"},
        {"name": "lastName", "type": "STRING"},
        {"name": "phonePrimary", "type": "STRING"},
        {"name": "phoneAlternate", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {"name": "street1", "type": "STRING"},
        {"name": "street2", "type": "STRING"},
        {"name": "city", "type": "STRING"},
        {"name": "state", "type": "STRING"},
        {"name": "zip", "type": "STRING"},
        {"name": "country", "type": "STRING"},
        {"name": "latitude", "type": "NUMERIC"},
        {"name": "longitude", "type": "NUMERIC"},
        {"name": "status", "type": "STRING"},
        {"name": "notes", "type": "STRING"},
        {"name": "customFields", "type": "STRING"},
        {"name": "businessName", "type": "STRING"},
        {"name": "appointment", "type": "STRING"},
        {"name": "integrationData", "type": "STRING"},
        {"name": "statusModified", "type": "TIMESTAMP"},
        {"name": "ownerModified", "type": "TIMESTAMP"},
        {"name": "dateCreated", "type": "TIMESTAMP"},
        {"name": "dateModified", "type": "TIMESTAMP"},
    ],
    ["id"],
    "statusModified",
)

lead_status_histories = Pipeline(
    "LeadStatusHistories",
    "leadStatusHistories",
    lambda rows: [
        {
            **i,
            "lead_id": int(k),
        }
        for page in rows
        for k, v in page.items()
        for i in v
    ],
    [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "STRING"},
        {"name": "abbreviation", "type": "STRING"},
        {"name": "active", "type": "BOOLEAN"},
        {"name": "changedByUserId", "type": "STRING"},
        {"name": "statusUpdated", "type": "TIMESTAMP"},
        {"name": "leadCreated", "type": "TIMESTAMP"},
        {"name": "leadModified", "type": "TIMESTAMP"},
        {"name": "lead_id", "type": "INTEGER"},
    ],
    [
        "id",
        "lead_id",
    ],
    "statusUpdated",
)
