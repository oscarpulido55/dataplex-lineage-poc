import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def create_parquet(filename, data):
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename)
    print(f"Created {filename}")

if __name__ == "__main__":
    # Dataset 1: Direct GCS Read (BQ External)
    data_1 = [
        {"id": 1, "name": "Alice", "source_system": "external_bq_tables"},
        {"id": 2, "name": "Bob", "source_system": "external_bq_tables"}
    ]
    create_parquet("external_bq_tables_source.parquet", data_1)

    # Dummy Dataset for Destination Table (to avoid Terraform error)
    data_dummy = [
        {
            "id": 0,
            "name": "dummy",
            "source_system": "dummy",
            "processed_timestamp": pd.to_datetime("2026-01-01T00:00:00Z"),
            "pipeline_id": "dummy"
        }
    ]
    create_parquet("dummy_dest.parquet", data_dummy)

    # Dataset 3: Native Dataplex
    data2 = [
        {"id": 101, "name": "Dataplex Charlie", "source_system": "custom_catalog_entries"},
        {"id": 102, "name": "Dataplex Dave", "source_system": "custom_catalog_entries"}
    ]
    create_parquet("custom_catalog_entries_source.parquet", data2)

    # Dataset 4: Custom API Managed Lineage
    data3 = [
        {"id": 201, "name": "Api Eve", "source_system": "api_managed_lineage"},
        {"id": 202, "name": "Api Frank", "source_system": "api_managed_lineage"}
    ]
    create_parquet("api_managed_lineage_source.parquet", data3)

    # Dataset 2: Native BQ Lineage
    data4 = [
        {"id": 301, "name": "Native BQ Grace", "source_system": "native_bq"},
        {"id": 302, "name": "Native BQ Heidi", "source_system": "native_bq"}
    ]
    create_parquet("native_bq_source.parquet", data4)
