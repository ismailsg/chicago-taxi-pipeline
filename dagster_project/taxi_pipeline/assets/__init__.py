from dagster import load_assets_from_modules
from . import bronze, silver, gold

all_assets = load_assets_from_modules([bronze, silver, gold])
