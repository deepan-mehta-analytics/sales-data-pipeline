# =============================================================================
# tests/unit/test_simulate_new_orders.py
# Unit tests for scripts/simulate_new_orders.py.
# =============================================================================

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import yaml

from scripts.simulate_new_orders import generate_new_orders

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # tests/unit → project root
SCHEMA_PATH = PROJECT_ROOT / "config" / "schema.yaml"


@pytest.fixture
def schema():
    with open(SCHEMA_PATH, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


class TestGenerateNewOrders:
    def test_returns_requested_count(self):
        df = generate_new_orders(count=10, as_of=date(2026, 8, 13), next_row_id=9995)

        assert len(df) == 10

    def test_row_ids_are_sequential_from_next_row_id(self):
        df = generate_new_orders(count=5, as_of=date(2026, 8, 13), next_row_id=9995)

        assert df["Row ID"].tolist() == [9995, 9996, 9997, 9998, 9999]

    def test_customer_id_has_sim_prefix(self):
        df = generate_new_orders(count=10, as_of=date(2026, 8, 13), next_row_id=9995)

        assert df["Customer ID"].str.startswith("SIM").all()

    def test_order_id_has_sim_prefix(self):
        df = generate_new_orders(count=10, as_of=date(2026, 8, 13), next_row_id=9995)

        assert df["Order ID"].str.startswith("SIM").all()

    def test_ship_date_on_or_after_order_date(self):
        df = generate_new_orders(count=20, as_of=date(2026, 8, 13), next_row_id=9995)

        order_dates = pd.to_datetime(df["Order Date"], format="%m/%d/%Y")
        ship_dates = pd.to_datetime(df["Ship Date"], format="%m/%d/%Y")

        assert (ship_dates >= order_dates).all()

    def test_order_date_is_as_of_date(self):
        df = generate_new_orders(count=5, as_of=date(2026, 8, 13), next_row_id=9995)

        assert (df["Order Date"] == "08/13/2026").all()

    def test_categorical_values_are_schema_allowed(self, schema):
        df = generate_new_orders(count=50, as_of=date(2026, 8, 13), next_row_id=9995)

        for col in ["Segment", "Region", "Category", "Ship Mode"]:
            allowed = set(schema["columns"][col]["allowed_values"])
            actual = set(df[col].unique())
            assert actual.issubset(allowed), f"{col} produced disallowed values: {actual - allowed}"

    def test_numeric_values_respect_schema_bounds(self, schema):
        df = generate_new_orders(count=50, as_of=date(2026, 8, 13), next_row_id=9995)

        assert (df["Sales"] >= schema["columns"]["Sales"]["min_value"]).all()
        assert (df["Quantity"] >= schema["columns"]["Quantity"]["min_value"]).all()
        assert (df["Discount"] >= schema["columns"]["Discount"]["min_value"]).all()
        assert (df["Discount"] <= schema["columns"]["Discount"]["max_value"]).all()

    def test_all_schema_columns_present(self, schema):
        df = generate_new_orders(count=5, as_of=date(2026, 8, 13), next_row_id=9995)

        expected_cols = set(schema["columns"].keys())
        assert expected_cols.issubset(set(df.columns))
