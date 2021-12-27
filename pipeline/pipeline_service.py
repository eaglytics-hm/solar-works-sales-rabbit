from typing import Optional
from datetime import datetime
from functools import reduce
import importlib

from pipeline import pipeline, pipeline_repo
from sales_rabbit import sales_rabbit_repo


def compose(*fn):
    def _compose(f, g):
        return lambda x: f(g(x))

    return reduce(_compose, fn, lambda x: x)


def factory(table: str) -> pipeline.Pipeline:
    try:
        return getattr(importlib.import_module(f"pipeline.pipeline"), table)
    except (ImportError, AttributeError):
        raise ValueError(table)


def run_service(pipeline: pipeline.Pipeline):
    def _pipeline(start: Optional[str]) -> dict:
        with sales_rabbit_repo.auth_session() as session:
            return compose(
                lambda x: {
                    "table": pipeline.table,
                    "output_rows": x,
                },
                pipeline_repo.load(
                    pipeline.table,
                    pipeline.schema,
                    pipeline.p_key,
                    pipeline.incre_key,
                ),
                pipeline.transform,
                sales_rabbit_repo.get(session, pipeline.endpoint),
            )(
                datetime.strptime(start, "%Y-%m-%d")
                if start
                else pipeline_repo.get_latest(pipeline.table, pipeline.incre_key)()
            )

    return _pipeline
