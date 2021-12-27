from pipeline import pipeline_service


def pipeline_controller(request_data: dict) -> dict:
    return pipeline_service.run_service(
        pipeline_service.factory(request_data["table"])
    )(request_data.get("start"))
