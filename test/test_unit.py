from unittest.mock import Mock

import pytest

from pipeline import pipeline, pipeline_service, pipeline_repo
from main import main


@pytest.fixture(
    params=[
        pipeline.Leads,
        pipeline.LeadStatusHistories,
    ],
    ids=[
        pipeline.Leads.table,
        pipeline.LeadStatusHistories.table,
    ],
)
def pl(request):
    return request.param


@pytest.fixture(
    params=[
        None,
        "2021-12-01",
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def start(request):
    return request.param


def test_get_latest(pl):
    res = pipeline_repo.get_latest(pl.table, pl.incre_key)()
    assert res


def test_pipeline_service(pl, start):
    res = pipeline_service.pipeline_service(pl)(start)
    assert res


def test_factory(pl):
    assert pipeline_service.factory(pl.table) == pl


def test_pipeline_controller(pl, start):
    data = {
        "table": pl.table,
        "start": start,
    }
    res = main(Mock(get_json=Mock(return_value=data), args=data))
    assert res
