from datetime import datetime

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()
DATASET = "SalesRabbit"


def get_latest(table: str, key: str):
    def _get() -> datetime:
        return [
            dict(row.items())
            for row in BQ_CLIENT.query(
                f"""SELECT MAX({key}) AS incre FROM {DATASET}.{table}"""
            ).result()
        ][0]["incre"]
    return _get


def load(table: str, schema: list[dict], p_key: list[str], incre_key: str):
    def _load(rows: list[dict]) -> int:
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{DATASET}.{table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=schema,
                ),
            )
            .result()
            .output_rows
        )
        _update(table, p_key, incre_key)
        return output_rows

    return _load


def _update(table: str, p_key: list[str], incre_key: str) -> None:
    BQ_CLIENT.query(
        f"""
    CREATE OR REPLACE TABLE {DATASET}.{table} AS
    SELECT * EXCEPT (row_num)
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY {','.join(p_key)}
                ORDER BY {incre_key} DESC
            ) AS row_num
        FROM {DATASET}.{table}
    ) WHERE row_num = 1"""
    ).result()
