"""Import and interchange checks do not replace the Docker/JVM spatial checks."""

from importlib import metadata
import io

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from packaging.specifiers import SpecifierSet

pytestmark = [pytest.mark.feature("F012")]


def test_declared_processor_runtime_interchanges_nullable_numeric_data():
    # Exercise the actual binary extension combination used around analytical table inputs.
    frame = pd.DataFrame({"delay_seconds": pd.Series([0, None, 120], dtype="Int64")})
    arrow = pa.Table.from_pandas(frame, preserve_index=False)
    restored = arrow.to_pandas()
    assert restored["delay_seconds"].tolist() == [0, pd.NA, 120]
    assert np.asarray(restored["delay_seconds"].dropna(), dtype=np.float64).tolist() == [0.0, 120.0]
    dependencies = metadata.requires("data-horizon-processor") or []
    assert "numpy==1.26.4" in dependencies and "pandas==2.2.3" in dependencies
    supported = SpecifierSet(metadata.metadata("data-horizon-processor")["Requires-Python"])
    assert "3.11" in supported and "3.12" in supported and "3.13" not in supported


def test_sedona_public_import_and_spark_sql_have_explicit_dependencies():
    pytest.importorskip("pyspark", reason="Install the processor spark extra for its import regression")
    sedona = pytest.importorskip("sedona.spark", reason="Install the processor spark extra for Sedona imports")
    from pyspark.sql import SparkSession

    assert callable(sedona.SedonaContext.create)
    assert callable(SparkSession.builder.getOrCreate)
    assert int(np.__version__.split(".")[0]) < 2
    assert pd.__version__ == "2.2.3"


@pytest.mark.parametrize("codec", ["snappy", "zstd"])
def test_pinned_arrow_pandas_parquet_interchange_keeps_nulls_and_values(codec):
    frame = pd.DataFrame({"delay_seconds": pd.Series([0, None, 120], dtype="Int64")})
    output = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), output, compression=codec)
    output.seek(0)
    table = pq.read_table(output)
    assert table.to_pylist() == [{"delay_seconds": 0}, {"delay_seconds": None}, {"delay_seconds": 120}]
    assert table.to_pandas()["delay_seconds"].isna().tolist() == [False, True, False]
