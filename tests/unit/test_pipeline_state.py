# =============================================================================
# tests/unit/test_pipeline_state.py
# Unit tests for src/utils/pipeline_state.py.
# =============================================================================

from src.utils.pipeline_state import get_watermark, set_watermark


class TestGetWatermark:
    def test_missing_file_returns_none(self, tmp_duckdb):
        """A database file that doesn't exist yet means 'first run' — no watermark."""
        assert get_watermark(tmp_duckdb) is None

    def test_no_pipeline_state_table_returns_none(self, tmp_duckdb):
        """An existing DB file with no pipeline_state table yet also means 'first run'."""
        import duckdb

        con = duckdb.connect(str(tmp_duckdb))
        con.execute("CREATE TABLE some_other_table (x INTEGER)")
        con.close()

        assert get_watermark(tmp_duckdb) is None

    def test_returns_stored_watermark(self, tmp_duckdb):
        set_watermark(tmp_duckdb, "2017-06-15", run_id="test-run-1")

        assert get_watermark(tmp_duckdb) == "2017-06-15"


class TestSetWatermark:
    def test_creates_table_and_row(self, tmp_duckdb):
        set_watermark(tmp_duckdb, "2017-01-01", run_id="run-a")

        assert get_watermark(tmp_duckdb) == "2017-01-01"

    def test_overwrites_previous_watermark(self, tmp_duckdb):
        set_watermark(tmp_duckdb, "2017-01-01", run_id="run-a")
        set_watermark(tmp_duckdb, "2017-06-15", run_id="run-b")

        assert get_watermark(tmp_duckdb) == "2017-06-15"

    def test_single_row_only(self, tmp_duckdb):
        """Setting the watermark twice must not accumulate rows."""
        import duckdb

        set_watermark(tmp_duckdb, "2017-01-01", run_id="run-a")
        set_watermark(tmp_duckdb, "2017-06-15", run_id="run-b")

        con = duckdb.connect(str(tmp_duckdb), read_only=True)
        try:
            count = con.execute("SELECT COUNT(*) FROM pipeline_state").fetchone()[0]
        finally:
            con.close()

        assert count == 1
