from pipeline import pipeline_controller


def main(request) -> dict:
    data: dict = request.get_json()
    print(data)

    if "table" in data:
        response = pipeline_controller.pipeline_controller(data)
        print(response)
        return response
    else:
        raise ValueError(data)
