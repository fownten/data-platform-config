# pipeline/ingest.py
from pyspark.sql import SparkSession
import sys


def main():
    spark = (
        SparkSession.builder.appName("Minio_to_Iceberg_Batch")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://minio.storage-layer.svc.cluster.local:9000",
        )
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.nessie.uri",
            "http://nessie.data-layer.svc.cluster.local:19120/api/v1",
        )
        .config("spark.sql.catalog.nessie.ref", "main")
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )

    LANDING_ZONE = "s3a://landing/"  # <-- Update to your Go app's output path
    print("Reading new data from Landing Zone...")
    try:
        df = spark.read.json(LANDING_ZONE)  # Change to .parquet() if needed
        if df.isEmpty():
            print("No new data found.")
            sys.exit(0)
        print(f"Found {df.count()} records. Appending to Iceberg...")
        df.write.format("iceberg").mode("append").save("nessie.gold.testing_table")
        # Clean up landing zone
        URI = spark._jvm.java.net.URI
        Path = spark._jvm.org.apache.hadoop.fs.Path
        FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
        fs = FileSystem.get(URI(LANDING_ZONE), spark._jsc.hadoopConfiguration())
        fs.delete(Path(LANDING_ZONE), True)
        print("Success! Landing zone cleared.")
    except Exception as e:
        print(f"Pipeline error: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
