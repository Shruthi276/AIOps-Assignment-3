
import os, json, time, argparse
from functools import reduce
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

BASE = os.environ.get("A3_BASE", "/workspace")
RAW = f"{BASE}/data/raw"
LOOKUP = f"{BASE}/data/lookup/taxi_zone_lookup.csv"
OUT = f"{BASE}/output/spark"
BENCH = f"{BASE}/benchmarks/spark_results.jsonl"

CANON = {
    "VendorID": "long", "tpep_pickup_datetime": "timestamp", "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "long", "trip_distance": "double", "PULocationID": "long",
    "DOLocationID": "long", "payment_type": "long", "fare_amount": "double",
    "tip_amount": "double", "tolls_amount": "double", "total_amount": "double",
}
KEEP = list(CANON)
KEY_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
            "trip_distance", "PULocationID", "DOLocationID", "fare_amount", "total_amount"]


def avg_speed_mph(distance, hours):         
    if distance is None or hours is None or hours <= 0:
        return None
    return round(distance / hours, 4)


speed_udf = F.udf(avg_speed_mph, T.DoubleType())


def read_one(spark, path):
    df = spark.read.parquet(path)
    return df.select([F.col(c).cast(CANON[c]).alias(c) for c in KEEP])


def build_pre_udf(spark, n_files):
    files = sorted(f for f in os.listdir(RAW) if f.endswith(".parquet"))[:n_files]
    df = reduce(DataFrame.unionByName, [read_one(spark, f"{RAW}/{f}") for f in files])

    df = df.dropna(subset=KEY_COLS)                                               # rule 2
    df = df.filter((F.col("tpep_pickup_datetime") >= F.lit("2022-01-01 00:00:00").cast("timestamp")) &
                   (F.col("tpep_pickup_datetime") < F.lit("2025-01-01 00:00:00").cast("timestamp")))  # rule 4
    df = df.withColumn("trip_duration_hours",
                       (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600.0)
    df = df.filter((F.col("trip_duration_hours") > 0) & (F.col("trip_duration_hours") <= 24) &
                   (F.col("trip_distance") > 0) & (F.col("trip_distance") <= 100))   # rule 5
    df = df.dropDuplicates(KEEP)          # rule 3: dedup AFTER filters (same result, far fewer rows shuffled)
    df = (df.withColumn("pickup_date", F.date_format("tpep_pickup_datetime", "yyyy-MM-dd"))
            .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")))

    zones = (spark.read.option("header", True).csv(LOOKUP)
             .select(F.col("LocationID").cast("long").alias("LocationID"), "Borough", "Zone"))
    pu = zones.select(F.col("LocationID").alias("PULocationID"),
                      F.col("Borough").alias("pickup_borough"), F.col("Zone").alias("pickup_zone"))
    do = zones.select(F.col("LocationID").alias("DOLocationID"),
                      F.col("Borough").alias("dropoff_borough"), F.col("Zone").alias("dropoff_zone"))
    return df.join(F.broadcast(pu), "PULocationID", "left").join(F.broadcast(do), "DOLocationID", "left")  # rule 6


def add_speed(df):                                                                  # rule 7
    return df.withColumn("avg_speed_mph", speed_udf("trip_distance", "trip_duration_hours")) \
             .filter(F.col("avg_speed_mph") <= 100)


def udf_bench(spark):
    """Isolated UDF cost: Python UDF vs the same maths as a native Spark expression.

    Reads the already-cleaned parquet written by the main run instead of rebuilding the
    pipeline (no second read of raw files, no second dedup shuffle, no cache needed).
    Only the two input columns are read. Note: the saved output already had rows with
    speed > 100 mph removed (rule 7), so the row count differs from the pre-UDF table by
    a negligible amount; per-row UDF cost is unaffected.
    """
    pre = spark.read.parquet(OUT).select("trip_distance", "trip_duration_hours")
    pre.count()                                       # warm the file cache so both timings read the same way
    t = time.perf_counter()
    pre.withColumn("s", speed_udf("trip_distance", "trip_duration_hours")).agg(F.sum("s")).collect()
    udf_s = time.perf_counter() - t
    t = time.perf_counter()
    pre.withColumn("s", F.round(F.col("trip_distance") / F.col("trip_duration_hours"), 4)).agg(F.sum("s")).collect()
    native_s = time.perf_counter() - t
    return udf_s, native_s


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=36, help="use only the first N files (for testing)")
    ap.add_argument("--run", default="run1")
    ap.add_argument("--skip-udf-bench", action="store_true")
    ap.add_argument("--hold", action="store_true", help="wait at the end so you can screenshot :4040")
    ap.add_argument("--exec-mem", default="3g")
    ap.add_argument("--exec-overhead", default="768m", help="off-heap room for Python UDF workers")
    ap.add_argument("--exec-cores", default="2")
    ap.add_argument("--driver-mem", default="1g")
    ap.add_argument("--partitions", default="64")
    ap.add_argument("--master", default="spark://spark-master:7077")
    a = ap.parse_args()

    b = (SparkSession.builder.appName("spark_clean").master(a.master)
         .config("spark.driver.memory", a.driver_mem).config("spark.executor.memory", a.exec_mem)
         .config("spark.executor.memoryOverhead", a.exec_overhead)
         .config("spark.executor.cores", a.exec_cores).config("spark.sql.shuffle.partitions", a.partitions)
         .config("spark.sql.session.timeZone", "UTC"))
    if a.master.startswith("spark://"):
        b = b.config("spark.driver.host", "spark-master").config("spark.driver.bindAddress", "0.0.0.0")
    spark = b.getOrCreate()

    t0 = time.perf_counter()                          # ingestion -> export
    add_speed(build_pre_udf(spark, a.files)).write.mode("overwrite").parquet(OUT)
    total = time.perf_counter() - t0

    rows = spark.read.parquet(OUT).count()
    result = {"framework": "spark", "run": a.run, "files": a.files, "total_seconds": round(total, 2), "rows": rows}
    if not a.skip_udf_bench:
        u, n = udf_bench(spark)
        result.update(udf_seconds=round(u, 2), native_seconds=round(n, 2), udf_overhead_seconds=round(u - n, 2))
    print(json.dumps(result, indent=2))
    os.makedirs(os.path.dirname(BENCH), exist_ok=True)
    with open(BENCH, "a") as f:
        f.write(json.dumps(result) + "\n")
    if a.hold:
        input("Take your :4040 screenshots, then press Enter to exit...")
    spark.stop()
