from pathlib import Path

import pandas as pd
from loguru import logger
import typer
from sqlalchemy import text
from python_scripts.data_aquisition.gdata_sql_queries import get_sql_engine, get_all_tables, get_discharge_table, groundwater_query, discharge_query, get_ratings_from_sites,  get_field_observations_for_sites

from python_scripts.config import PROCESSED_DATA_DIR

app = typer.Typer()

@app.command()
def water_temp(
    site_id: str = typer.Argument(...),
    output_path: Path = PROCESSED_DATA_DIR / "dataset.csv",
):
    engine = get_sql_engine()
    logger.info(f"Getting water temp for site {site_id}...")
    df = get_water_temp(engine, site_id)
    logger.success(f"Query returned {len(df)} rows.")
    df.to_csv(output_path, index=False)
    logger.success(f"Saved to {output_path}")

@app.command()
def list_tables(output_path: Path = PROCESSED_DATA_DIR / "tables.csv",):
    engine = get_sql_engine()
    logger.info("Getting all tables...")
    df = get_all_tables(engine)
    df.to_csv(output_path, index=False)
    print(df.to_string())

@app.command()
def view_discharge_table(output_path: Path = PROCESSED_DATA_DIR / "view_discharge_table.csv",):
    engine = get_sql_engine()
    logger.info("Getting all tables...")
    df = get_discharge_table(engine)
    df.to_csv(output_path, index=False)
    print(df.to_string())

@app.command()
def view_groundwater_data(output_path: Path = PROCESSED_DATA_DIR / "groundwater_data.csv",):
    engine = get_sql_engine()
    logger.info("Getting all tables...")
    df = groundwater_query(engine)
    df.to_csv(output_path, index=False)
    print(df.to_string())

@app.command()
def view_discharge_data(output_path: Path = PROCESSED_DATA_DIR / "discharge_data.csv",):
    engine = get_sql_engine()
    logger.info("Getting all tables...")
    df = discharge_query(engine)
    df.to_csv(output_path, index=False)
    print(df.to_string())


@app.command()
def view_field_observations(output_path: Path = PROCESSED_DATA_DIR / "field_observations.csv",):
    engine = get_sql_engine()
    logger.info("Getting all tables...")
    df = get_field_observations_for_sites(engine)
    df.to_csv(output_path, index=False)
    print(df.to_string())


@app.command()
def view_ratings(output_path: Path = PROCESSED_DATA_DIR / "ratings.csv",):
    engine = get_sql_engine()
    logger.info("Getting all tables...")
    df = get_ratings_from_sites(engine)
    df.to_csv(output_path, index=False)
    print(df.to_string())


if __name__ == "__main__":
    app()