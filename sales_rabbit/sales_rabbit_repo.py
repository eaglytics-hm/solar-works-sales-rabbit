import os
from datetime import datetime, timezone

import requests

BASE_URL = "https://api.salesrabbit.com"
PER_PAGE = 2000


def auth_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {os.getenv('API_KEY')}",
            "Content-Type": "application/json",
        }
    )
    return session


def timestamp_format(ts: datetime) -> str:
    return ts.replace(tzinfo=timezone.utc).isoformat(timespec="seconds") + "+00:00"


def get(session: requests.Session, endpoint: str):
    def _get(start: datetime, page: int = 1) -> list[dict]:
        with session.get(
            f"{BASE_URL}/{endpoint}",
            params={
                "perPage": PER_PAGE,
                "page": page,
            },
            headers={
                "If-Modified-Since": timestamp_format(start),
            },
        ) as r:
            if r.status_code == 304:
                return []
            res = r.json()
        return (
            [res["data"]]
            if not res["meta"]["morePages"]
            else [res["data"]] + _get(start, page + 1)
        )

    return _get
