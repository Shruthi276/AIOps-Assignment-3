
import os, json, time, shutil, argparse
import numpy as np
import pandas as pd
import pyarrow as pa
import ray

BASE = os.environ.get("A3_BASE", "/workspace")
RAW = f"{BASE}/data/raw"
LOOKUP = f"{BASE}/data/lookup/taxi_zone_lookup.csv"
OUT = f"{BASE}/output/ray"
BENCH = f"{BASE}/benchmarks/ray_results.jsonl"

CANON = pa.schema([
    ("VendorID", pa.int64()), ("tpep_pickup_datetime", pa.timestamp("us")),
    ("tpep_dropoff_datetime", pa.timestamp("us")), ("passenger_count", pa.int64()),
    ("trip_distance", pa.float64()), ("PULocationID", pa.int64()), ("DOLocationID", pa.int64()),
    ("payment_type", pa.int64()), ("fare_amount", pa.float64()), ("tip_amount", pa.float64()),
    ("tolls_amount", pa.float64()), ("total_amount", pa.float64()),
])
KEEP = CANON.names
KEY_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
            "trip_distance", "PULocationID", "DOLocationID", "fare_amount", "total_amount"]
INT_COLS = ["VendorID", "passenger_count", "PULocationID", "DOLocationID", "payment_type"]
START, END = pd.Timestamp("2022-01-01"), pd.Timestamp("2025-01-01")
DEDUPE_BUCKETS = 256

OUT_COLS = ["DOLocationID", "PULocationID", "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "payment_type", "fare_amount", "tip_amount",
            "tolls_amount", "total_amount", "trip_duration_hours", "pickup_date", "pickup_hour",
            "pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone", "avg_speed_mph"]


def avg_speed_mph(distance, hours):        
    if distance is None or hours is None or hours <= 0:
        return None
    return round(distance / hours, 4)


def load_zones():
    z = pd.read_csv(LOOKUP, keep_default_na=False, na_values=[""])
    z["LocationID"] = z["LocationID"].astype("int64")
    pu = z[["LocationID", "Borough", "Zone"]].rename(columns={
        "LocationID": "PULocationID", "Borough": "pickup_borough", "Zone": "pickup_zone"})
    do = z[["LocationID", "Borough", "Zone"]].rename(columns={
        "LocationID": "DOLocationID", "Borough": "dropoff_borough", "Zone": "dropoff_zone"})
    return pu, do


def to_canonical(table: pa.Table) -> pa.Table:
    
    return table.select(KEEP).cast(CANON, safe=False)


def _secs(ts: pd.Series) -> np.ndarray:
    """Whole epoch seconds, truncated like Spark's unix_timestamp()."""
    return ts.astype("datetime64[s]").astype("int64").to_numpy()


def clean_filter_bucket(df: pd.DataFrame) -> pd.DataFrame:
    """Rules 2, 4, 5 in one pass, then tag each row with a dedup bucket."""
    df = df.dropna(subset=KEY_COLS)                                                  
    pick = df["tpep_pickup_datetime"]
    df = df[(pick >= START) & (pick < END)].copy()                                    
    ps, ds = _secs(df["tpep_pickup_datetime"]), _secs(df["tpep_dropoff_datetime"])
    dur = (ds - ps) / 3600.0
    df["trip_duration_hours"] = dur
    df = df[(dur > 0) & (dur <= 24) & (df["trip_distance"] > 0) & (df["trip_distance"] <= 100)].copy()  
    for c in ["passenger_count", "PULocationID", "DOLocationID"]:
        df[c] = df[c].astype("int64")                     
    for c in ["VendorID", "payment_type"]:
        df[c] = df[c].astype("Int64")                       

    
    h = (_secs(df["tpep_pickup_datetime"]) * 2654435761 + _secs(df["tpep_dropoff_datetime"]) * 40503
         + df["PULocationID"].to_numpy() * 131 + df["DOLocationID"].to_numpy() * 7)
    df["_bucket"] = (h % DEDUPE_BUCKETS).astype("int64")
    return df.reset_index(drop=True)


def dedupe_bucket(df: pd.DataFrame) -> pd.DataFrame:                                  


def make_finish(pu, do):
    def finish(df: pd.DataFrame) -> pd.DataFrame:
        """Derived date/hour, rule 6 (zone join, broadcast-style), rule 7 (Python UDF + filter)."""
        ts = df["tpep_pickup_datetime"]
        df = df.copy()
        df["pickup_date"] = ts.to_numpy().astype("datetime64[D]").astype(str).astype(object)
        df["pickup_hour"] = ts.dt.hour.astype("int32")
        df = df.merge(pu, on="PULocationID", how="left").merge(do, on="DOLocationID", how="left")
       
        speeds = [avg_speed_mph(d, h) for d, h in
                  zip(df["trip_distance"].tolist(), df["trip_duration_hours"].tolist())]
        df["avg_speed_mph"] = pd.Series(speeds, dtype="float64", index=df.index)
        df = df[df["avg_speed_mph"] <= 100]
        return df[OUT_COLS].reset_index(drop=True)
    return finish


def build_dataset(n_files):
    files = sorted(f for f in os.listdir(RAW) if f.endswith(".parquet"))[:n_files]
    pu, do = load_zones()
    ds = ray.data.read_parquet([f"{RAW}/{f}" for f in files], columns=KEEP, partitioning=None)  # ingestion
    ds = ds.map_batches(to_canonical, batch_format="pyarrow", batch_size=None)       # rule 1
    ds = ds.map_batches(clean_filter_bucket, batch_format="pandas", batch_size=None) # rules 2,4,5
    # rule 3: Ray Data 2.40 has no drop_duplicates; bucket then dedupe exactly inside each bucket
    ds = ds.groupby("_bucket").map_groups(dedupe_bucket, batch_format="pandas")
    ds = ds.map_batches(make_finish(pu, do), batch_format="pandas", batch_size=None)
    return ds, len(files)


def py_udf_batch(df: pd.DataFrame) -> pd.DataFrame:
    d, h = df["trip_distance"].tolist(), df["trip_duration_hours"].tolist()
    return pd.DataFrame({"s": [avg_speed_mph(x, y) for x, y in zip(d, h)]})


def native_batch(df: pd.DataFrame) -> pd.DataFrame:     
    return pd.DataFrame({"s": np.round(df["trip_distance"].to_numpy() / df["trip_duration_hours"].to_numpy(), 4)})


def udf_bench():
    """Isolated UDF cost on the already-cleaned output (no pipeline rebuild, nothing held in memory)."""
    ds = ray.data.read_parquet(OUT, columns=["trip_distance", "trip_duration_hours"], partitioning=None)
    ds.sum("trip_distance")                             
    t = time.perf_counter()
    ds.map_batches(py_udf_batch, batch_format="pandas", batch_size=None).sum("s")
    udf_s = time.perf_counter() - t
    t = time.perf_counter()
    ds.map_batches(native_batch, batch_format="pandas", batch_size=None).sum("s")
    native_s = time.perf_counter() - t
    return udf_s, native_s


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=36, help="use only the first N files (for testing)")
    ap.add_argument("--run", default="run1")
    ap.add_argument("--skip-udf-bench", action="store_true")
    ap.add_argument("--hold", action="store_true", help="wait at the end so you can screenshot the dashboard")
    ap.add_argument("--address", default="auto", help="'auto' for the Docker cluster, 'local' to test on one machine")
    ap.add_argument("--local-cpus", type=int, default=2)
    a = ap.parse_args()

    if a.address == "local":
        ray.init(num_cpus=a.local_cpus, include_dashboard=False)
    else:
        ray.init(address=a.address)
    print("Ray cluster resources:", ray.cluster_resources())

    shutil.rmtree(OUT, ignore_errors=True)            
    os.makedirs(OUT, exist_ok=True)

    t0 = time.perf_counter()                         
    ds, n_used = build_dataset(a.files)
    ds.write_parquet(OUT)
    total = time.perf_counter() - t0

    rows = ray.data.read_parquet(OUT, partitioning=None).count()
    result = {"framework": "ray", "run": a.run, "files": n_used, "total_seconds": round(total, 2), "rows": rows}
    if not a.skip_udf_bench:
        u, n = udf_bench()
        result.update(udf_seconds=round(u, 2), native_seconds=round(n, 2), udf_overhead_seconds=round(u - n, 2))
    print(json.dumps(result, indent=2))
    os.makedirs(os.path.dirname(BENCH), exist_ok=True)
    with open(BENCH, "a") as f:
        f.write(json.dumps(result) + "\n")
    if a.hold:
        input("Take your dashboard screenshots, then press Enter to exit...")
    ray.shutdown()
