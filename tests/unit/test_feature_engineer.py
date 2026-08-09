# =============================================================================
# tests/unit/test_feature_engineer.py
# Unit tests for src/transform/feature_engineer.py.
# =============================================================================

import pandas as pd  # DataFrame creation and assertions

from src.transform.feature_engineer import add_categorical_features  # Profit tier and shipping speed buckets
from src.transform.feature_engineer import add_financial_features  # Margin and per-unit columns
from src.transform.feature_engineer import add_time_features  # Temporal KPI columns
from src.transform.feature_engineer import engineer  # Public entry point


class TestAddTimeFeatures:
    """Tests for the add_time_features function."""

    def test_order_year_extracted(self, cleaned_df):
        """order_year must equal the calendar year of Order Date."""
        result = add_time_features(cleaned_df)

        # The first row has Order Date 11/8/2016 — year must be 2016.
        assert result["order_year"].iloc[0] == 2016, "order_year should be 2016"

    def test_order_month_extracted(self, cleaned_df):
        """order_month must equal the month number (1-12) of Order Date."""
        result = add_time_features(cleaned_df)

        # November is month 11.
        assert result["order_month"].iloc[0] == 11, "order_month should be 11 for November"

    def test_order_quarter_extracted(self, cleaned_df):
        """order_quarter must be between 1 and 4 inclusive."""
        result = add_time_features(cleaned_df)

        assert result["order_quarter"].between(1, 4).all(), "All order_quarter values must be between 1 and 4"

    def test_shipping_days_non_negative(self, cleaned_df):
        """shipping_days must be >= 0 for every row (Ship Date >= Order Date)."""
        result = add_time_features(cleaned_df)

        assert (result["shipping_days"] >= 0).all(), "shipping_days must be non-negative"

    def test_shipping_days_correct(self, cleaned_df):
        """
        The first row has Order Date 11/8/2016 and Ship Date 11/11/2016,
        so shipping_days must be 3.
        """
        result = add_time_features(cleaned_df)

        assert result["shipping_days"].iloc[0] == 3, "shipping_days should be 3 for first row"

    def test_input_not_mutated(self, cleaned_df):
        """add_time_features must return a copy and not mutate the input."""
        cols_before = set(cleaned_df.columns)
        add_time_features(cleaned_df)  # Call the function

        assert set(cleaned_df.columns) == cols_before, "add_time_features must not add columns to the input DataFrame"


class TestAddFinancialFeatures:
    """Tests for the add_financial_features function."""

    def test_profit_margin_pct_computed(self, cleaned_df):
        """profit_margin_pct must be (Profit / Sales) * 100."""
        result = add_financial_features(cleaned_df)

        # Row 0: Sales=261.96, Profit=41.91 → margin ≈ 16.00 %
        expected_margin = round((41.91 / 261.96) * 100, 2)
        actual_margin = result["profit_margin_pct"].iloc[0]

        assert (
            abs(actual_margin - expected_margin) < 0.1
        ), f"Expected profit_margin_pct ≈ {expected_margin}, got {actual_margin}"

    def test_zero_sales_gives_zero_margin(self):
        """A row with Sales=0 must produce profit_margin_pct=0 (no division by zero)."""
        df = pd.DataFrame(
            {
                "Sales": [0.0],  # Zero sales — tests division guard
                "Profit": [10.0],  # Non-zero profit
                "Quantity": [1],
                "Discount": [0.0],
            }
        )
        result = add_financial_features(df)

        assert result["profit_margin_pct"].iloc[0] == 0.0, "profit_margin_pct must be 0.0 when Sales is 0"

    def test_is_profitable_flag(self, cleaned_df):
        """is_profitable must be True for rows with positive Profit."""
        result = add_financial_features(cleaned_df)

        # All rows in cleaned_df have Profit > 0, so all should be True.
        assert result["is_profitable"].all(), "All rows with Profit > 0 must be is_profitable=True"

    def test_negative_profit_not_profitable(self):
        """is_profitable must be False for rows with negative Profit."""
        df = pd.DataFrame({"Sales": [100.0], "Profit": [-20.0], "Quantity": [1], "Discount": [0.5]})
        result = add_financial_features(df)

        # Using 'not' is idiomatic — avoids flake8 E712 boolean-equality warning.
        assert not result["is_profitable"].iloc[0], "is_profitable must be False when Profit < 0"

    def test_discount_amount_computed(self, cleaned_df):
        """discount_amount must equal Sales * Discount."""
        result = add_financial_features(cleaned_df)

        expected = (cleaned_df["Sales"] * cleaned_df["Discount"]).round(2)
        pd.testing.assert_series_equal(result["discount_amount"], expected, check_names=False, rtol=0.01)

    def test_per_unit_columns_with_nullable_int64_quantity(self):
        """
        Regression test: Quantity as pandas nullable 'Int64' (capital I) with a
        zero-quantity row must still produce float64 revenue_per_unit /
        profit_per_unit columns with correct values.

        This is the dtype path that broke under Airflow's pinned pandas 2.1.4 /
        numpy 1.26.4 (np.where returned an object-dtype array instead of
        float64, and .round(2) on an object array raised TypeError). The fix
        casts Quantity to float64 before dividing so the result is dtype-stable
        across pandas/numpy versions. This test runs under the project's normal
        pandas/numpy versions, which do not reproduce the original failure —
        it exists to lock in identical, correct output after the fix.
        """
        df = pd.DataFrame(
            {
                "Sales": [100.0, 50.0, 0.0],  # Revenue for 3 rows, including a zero-quantity row
                "Profit": [20.0, -10.0, 0.0],  # Profit for 3 rows, matching Sales rows
                "Quantity": pd.array([4, 0, 0], dtype="Int64"),  # Nullable Int64 dtype, including a zero
                "Discount": [0.1, 0.0, 0.0],  # Discount fraction per row
            }
        )
        result = add_financial_features(df)

        # Both per-unit columns must end up as plain float64, not object dtype.
        assert result["revenue_per_unit"].dtype == "float64", "revenue_per_unit must be float64"
        assert result["profit_per_unit"].dtype == "float64", "profit_per_unit must be float64"

        # Row 0: Quantity=4 (non-zero) → Sales/Quantity and Profit/Quantity.
        assert result["revenue_per_unit"].iloc[0] == 25.0, "100.0 / 4 must equal 25.0"
        assert result["profit_per_unit"].iloc[0] == 5.0, "20.0 / 4 must equal 5.0"

        # Rows 1 and 2: Quantity=0 → division guard must default to 0.0.
        assert result["revenue_per_unit"].iloc[1] == 0.0, "Zero quantity must default revenue_per_unit to 0.0"
        assert result["profit_per_unit"].iloc[1] == 0.0, "Zero quantity must default profit_per_unit to 0.0"
        assert result["revenue_per_unit"].iloc[2] == 0.0, "Zero quantity must default revenue_per_unit to 0.0"
        assert result["profit_per_unit"].iloc[2] == 0.0, "Zero quantity must default profit_per_unit to 0.0"


class TestAddCategoricalFeatures:
    """Tests for the add_categorical_features function."""

    def test_profit_tier_loss(self):
        """A row with negative profit_margin_pct must get profit_tier='Loss'."""
        df = pd.DataFrame(
            {
                "profit_margin_pct": [-15.0],  # Negative margin → Loss tier
                "shipping_days": [3],
            }
        )
        result = add_categorical_features(df)

        assert result["profit_tier"].iloc[0] == "Loss"

    def test_profit_tier_high(self):
        """A row with profit_margin_pct >= 20 must get profit_tier='High'."""
        df = pd.DataFrame(
            {
                "profit_margin_pct": [25.0],  # 25 % margin → High tier
                "shipping_days": [3],
            }
        )
        result = add_categorical_features(df)

        assert result["profit_tier"].iloc[0] == "High"

    def test_shipping_speed_express(self):
        """shipping_days of 2 must produce shipping_speed='Express'."""
        df = pd.DataFrame(
            {
                "profit_margin_pct": [10.0],
                "shipping_days": [2],  # 2-day shipping → Express
            }
        )
        result = add_categorical_features(df)

        assert result["shipping_speed"].iloc[0] == "Express"

    def test_shipping_speed_same_day(self):
        """shipping_days of 0 or 1 must produce shipping_speed='Same Day'."""
        df = pd.DataFrame(
            {
                "profit_margin_pct": [10.0],
                "shipping_days": [0],  # Same-day delivery
            }
        )
        result = add_categorical_features(df)

        assert result["shipping_speed"].iloc[0] == "Same Day"

    def test_shipping_speed_with_nullable_int64_shipping_days(self):
        """
        Regression test: shipping_days as pandas nullable 'Int64' (capital I)
        must still produce correct shipping_speed labels at every boundary.

        This is the dtype path that broke under Airflow's pinned pandas 2.1.4 /
        numpy 1.26.4: comparisons on a nullable Int64 column produce pandas'
        nullable 'boolean' extension dtype, and np.select's condlist raised
        "TypeError: invalid entry 0 in condlist: should be boolean ndarray" on
        those older versions. The fix casts shipping_days to float64 before
        building the comparisons so np.select always receives native numpy
        bool arrays. This test runs under the project's normal pandas/numpy
        versions, which do not reproduce the original failure — it exists to
        lock in identical, correct output after the fix.
        """
        df = pd.DataFrame(
            {
                "profit_margin_pct": [10.0, 10.0, 10.0, 10.0],  # Constant margin, irrelevant to this test
                "shipping_days": pd.array([1, 2, 3, 7], dtype="Int64"),  # Nullable Int64, boundary values
            }
        )
        result = add_categorical_features(df)

        # shipping_days dtype must remain Int64 — the cast is local to the comparison only.
        assert result["shipping_days"].dtype == "Int64", "shipping_days column dtype must stay Int64"

        # Boundary values must map to the expected labels.
        expected = ["Same Day", "Express", "Express", "Slow"]  # 1→Same Day, 2/3→Express, 7→Slow
        assert list(result["shipping_speed"]) == expected, f"Expected {expected}, got {list(result['shipping_speed'])}"


class TestEngineer:
    """Tests for the public engineer() entry point."""

    def test_returns_dataframe_and_dict(self, cleaned_df):
        """engineer() must return a (pd.DataFrame, dict) tuple."""
        result = engineer(cleaned_df)

        assert isinstance(result, tuple) and len(result) == 2
        assert isinstance(result[0], pd.DataFrame)
        assert isinstance(result[1], dict)

    def test_new_columns_added(self, cleaned_df):
        """engineer() must add at least the core derived columns."""
        enriched, meta = engineer(cleaned_df)

        expected_new_cols = {
            "order_year",
            "order_month",
            "shipping_days",
            "profit_margin_pct",
            "is_profitable",
            "profit_tier",
            "shipping_speed",
        }
        actual_new_cols = set(enriched.columns) - set(cleaned_df.columns)

        missing = expected_new_cols - actual_new_cols
        assert not missing, f"engineer() is missing expected columns: {missing}"

    def test_row_count_unchanged(self, cleaned_df):
        """Feature engineering must not add or remove rows."""
        enriched, _ = engineer(cleaned_df)

        assert len(enriched) == len(cleaned_df), "engineer() must not change the number of rows"

    def test_metadata_new_features_list(self, cleaned_df):
        """The metadata dict must list the names of newly added columns."""
        _, meta = engineer(cleaned_df)

        assert "new_features" in meta, "Metadata must contain 'new_features' key"
        assert len(meta["new_features"]) > 0, "new_features must not be empty"
