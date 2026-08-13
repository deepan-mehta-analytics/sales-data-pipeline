# =============================================================================
# scripts/simulate_new_orders.py
# Synthetic new-order generator — demo/testing tool for the incremental
# load feature (v2.0). NEVER imported by the production pipeline (src/) or
# run in CI. Triggered manually via `make simulate-new-orders`.
#
# Appends N schema-valid, realistic-looking synthetic orders to
# data/bronze/sales_data.csv, dated "today" by default, so a subsequent
# pipeline run has genuinely new rows for its watermark-based incremental
# extraction to pick up.
#
# Every generated Customer ID / Order ID / Product ID carries a "SIM"
# marker so synthetic rows are always visually distinguishable from the
# real historical Superstore data in any query or screenshot.
# =============================================================================

import argparse  # CLI argument parsing
import random  # Numeric field generation
from datetime import date, timedelta  # Order/ship date generation
from pathlib import Path  # Cross-platform path resolution

import pandas as pd  # DataFrame construction and CSV append
import yaml  # Reads config/config.yaml and config/schema.yaml
from faker import Faker  # Realistic name/address generation

PROJECT_ROOT = Path(__file__).resolve().parent.parent  # scripts -> project root
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
SCHEMA_PATH = PROJECT_ROOT / "config" / "schema.yaml"

fake = Faker()  # Module-level Faker instance (deterministic seeding not needed — this is a demo tool)

# Sub-Category has no allowed_values list in schema.yaml, so a small
# realistic mapping keeps generated products plausible per Category.
SUBCATEGORIES_BY_CATEGORY = {
    "Furniture": ["Bookcases", "Chairs", "Tables", "Furnishings"],
    "Office Supplies": ["Labels", "Binders", "Paper", "Storage", "Art", "Envelopes", "Fasteners", "Supplies"],
    "Technology": ["Phones", "Accessories", "Machines", "Copiers"],
}


def _load_yaml(path: Path) -> dict:
    """Read and return a YAML file as a dict."""
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _next_row_id(bronze_path: Path) -> int:
    """Return one past the current max Row ID in the bronze CSV."""
    max_row_id = pd.read_csv(bronze_path, usecols=["Row ID"], encoding="latin-1")[
        "Row ID"
    ].max()  # latin-1 matches the bronze CSV encoding
    return int(max_row_id) + 1


def generate_new_orders(count: int, as_of: date, next_row_id: int) -> pd.DataFrame:
    """
    Generate `count` synthetic order rows, schema-valid and dated `as_of`.

    Parameters
    ----------
    count       : int   Number of synthetic rows to generate.
    as_of       : date  Order Date to stamp every generated row with.
    next_row_id : int   Row ID of the first generated row (subsequent rows
                         increment sequentially, never colliding with real data).

    Returns
    -------
    pd.DataFrame
        count rows, columns matching config/schema.yaml exactly, string
        date columns formatted MM/DD/YYYY (matching the real bronze CSV).
    """
    schema = _load_yaml(SCHEMA_PATH)
    date_format = "%m/%d/%Y"  # Matches config.yaml's source.date_format

    segments = schema["columns"]["Segment"]["allowed_values"]
    regions = schema["columns"]["Region"]["allowed_values"]
    categories = schema["columns"]["Category"]["allowed_values"]
    ship_modes = schema["columns"]["Ship Mode"]["allowed_values"]

    rows = []
    for i in range(count):
        row_id = next_row_id + i
        category = random.choice(categories)
        sub_category = random.choice(SUBCATEGORIES_BY_CATEGORY[category])
        ship_offset = random.randint(0, 6)  # 0-6 days -> satisfies ship_after_order
        ship_date = as_of + timedelta(days=ship_offset)
        sales = round(random.uniform(10.0, 800.0), 2)
        quantity = random.randint(1, 8)
        discount = random.choice([0.0, 0.1, 0.15, 0.2, 0.3])
        profit = round(sales * random.uniform(-0.15, 0.35), 4)

        rows.append(
            {
                "Row ID": row_id,
                "Order ID": f"SIM-{as_of.year}-{fake.unique.random_int(100000, 999999)}",
                "Order Date": as_of.strftime(date_format),
                "Ship Date": ship_date.strftime(date_format),
                "Ship Mode": random.choice(ship_modes),
                "Customer ID": f"SIM-{fake.unique.random_int(10000, 99999)}",
                "Customer Name": fake.name(),
                "Segment": random.choice(segments),
                "Country": "United States",
                "City": fake.city(),
                "State": fake.state(),
                "Postal Code": fake.zipcode(),
                "Region": random.choice(regions),
                "Product ID": (
                    f"{category[:3].upper()}-{sub_category[:2].upper()}"
                    f"-SIM{fake.unique.random_int(1000000, 9999999)}"
                ),
                "Category": category,
                "Sub-Category": sub_category,
                "Product Name": (
                    f"{fake.word().capitalize()} "
                    f"{sub_category[:-1] if sub_category.endswith('s') else sub_category}"
                ),
                "Sales": sales,
                "Quantity": quantity,
                "Discount": discount,
                "Profit": profit,
            }
        )

    return pd.DataFrame(rows)


def append_to_bronze(df: pd.DataFrame, bronze_path: Path) -> None:
    """Append generated rows to the bronze CSV, matching its existing column order."""
    existing_columns = pd.read_csv(
        bronze_path, nrows=0, encoding="latin-1"
    ).columns.tolist()  # latin-1 matches the bronze CSV encoding
    df[existing_columns].to_csv(
        bronze_path, mode="a", header=False, index=False, encoding="latin-1"
    )  # Maintain encoding when appending


def main() -> None:
    """CLI entry point — see `make simulate-new-orders`."""
    parser = argparse.ArgumentParser(description="Generate synthetic new Superstore orders for incremental-load demos.")
    parser.add_argument("--count", type=int, default=25, help="Number of synthetic orders to generate (default: 25)")
    parser.add_argument(
        "--as-of", type=str, default=None, help="Order date for generated rows, YYYY-MM-DD (default: today)"
    )
    args = parser.parse_args()

    config = _load_yaml(CONFIG_PATH)
    bronze_path = PROJECT_ROOT / config["paths"]["bronze"]
    as_of = date.fromisoformat(args.as_of) if args.as_of else date.today()

    next_row_id = _next_row_id(bronze_path)
    new_orders = generate_new_orders(count=args.count, as_of=as_of, next_row_id=next_row_id)
    append_to_bronze(new_orders, bronze_path)

    print(
        f"Appended {len(new_orders)} synthetic orders (Row ID {next_row_id}-{next_row_id + args.count - 1}) "
        f"dated {as_of.isoformat()} to {bronze_path}"
    )


if __name__ == "__main__":
    main()
