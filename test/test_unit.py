import pytest

from pipeline import pipeline, pipeline_controller, pipeline_repo

pipelines = [
    pipeline.leads,
    pipeline.lead_status_histories,
]


@pytest.mark.parametrize(
    "pl",
    pipelines,
    ids=[i.table for i in pipelines],
)
def test_get_latest(pl):
    res = pipeline_repo.get_latest(pl.table, pl.incre_key)()
    assert res


@pytest.mark.parametrize(
    "pl",
    pipelines,
    ids=[i.table for i in pipelines],
)
@pytest.mark.parametrize(
    "start",
    [None, "2021-12-01"],
    ids=["auto", "manual"],
)
def test_pipeline(pl, start):
    res = pipeline_controller.pipeline_controller(pl)(start)
    assert res
