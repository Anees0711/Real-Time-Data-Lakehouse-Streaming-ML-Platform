from __future__ import annotations

import logging

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import SparkSession

from transport_platform.core.config import SILVER_PATH

LOGGER = logging.getLogger(__name__)
SUITE_NAME = "silver_suite"


def run() -> None:
    spark = SparkSession.builder.appName("QualityCheck").master("local[*]").getOrCreate()

    try:
        context = gx.get_context()

        # Always create suite fresh (ephemeral context safe)
        context.add_expectation_suite(expectation_suite_name=SUITE_NAME)

        df = spark.read.parquet(SILVER_PATH)

        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="runtime_connector",
            data_asset_name="silver_data",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "run_1"},
        )

        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=SUITE_NAME,
        )

        validator.expect_column_to_exist("station")
        validator.expect_column_values_to_be_between("delay_minutes", 0, 60)

        results = validator.validate()
        LOGGER.info("Validation success: %s", results["success"])

    finally:
        spark.stop()
