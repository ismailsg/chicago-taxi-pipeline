from dagster import Definitions
from taxi_pipeline.assets import all_assets
from taxi_pipeline.resources import SparkResource

defs = Definitions(
    assets=all_assets,
    resources={
        "spark": SparkResource(app_name="taxi-pipeline"),
    },
)
