# DA3408 A3: Spark vs. Ray, the Data Engineering Duel

An identical data-cleaning pipeline implemented in **Apache Spark** (`spark_clean.py`) and **Ray Data**
(`ray_clean.py`), run on a two-worker cluster for each framework and benchmarked on NYC Yellow Taxi
trip data (2022-2024, 36 monthly Parquet files, about 1.9 GB).


---

## 1. Repository contents

| File | Purpose |
|---|---|
| `spark_clean.py` | The Spark pipeline (PySpark, standalone cluster, Python UDF) |
| `ray_clean.py` | The Ray Data pipeline (same rules, same Python function for the UDF) |
| `README.md` | This guide. The Dockerfile and both cluster definitions are included below. |
| `Report.pdf` |
|`Screenshots'|

---

## 2. Environment used

- Ubuntu inside VirtualBox, **12 GB RAM, 4 vCPUs**
- Docker (two worker containers per framework on one VM, each worker acting as one node)
- Python 3.11 in the containers, PySpark 3.5.3, Ray 2.40.0, Java 21

Install Docker on the VM:

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-v2
sudo usermod -aG docker $USER && newgrp docker
docker run hello-world
```

---

## 3. Project layout and data

Run everything from the repo root (the folder holding `spark_clean.py`). Create this layout:

```
.
├── spark_clean.py
├── ray_clean.py
├── README.md
├── Dockerfile                    (create from section 4)
├── docker-compose.spark.yml      (create from section 5)
├── docker-compose.ray.yml        (create from section 6)
├── data/
│   ├── raw/                      yellow_tripdata_2022-01.parquet ... yellow_tripdata_2024-12.parquet
│   └── lookup/taxi_zone_lookup.csv
├── output/                       (created by the pipelines: output/spark, output/ray)
└── benchmarks/                   (created by the pipelines: *_results.jsonl, plus stats logs)
```

Download the 36 monthly Yellow Taxi Parquet files from the NYC TLC Trip Record Data page into `data/raw/`
(files are named `yellow_tripdata_YYYY-MM.parquet`). Then get the zone lookup table used for the join:

```bash
mkdir -p data/lookup benchmarks output
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv -P data/lookup/
```

`data/`, `output/` and `benchmarks/` are large or generated, so they are not committed (add them to `.gitignore`).

The whole repo folder is mounted at `/workspace` in every container, so all nodes read the same shared
directory.

---

## 4. Build the image

All containers use one image, which guarantees identical Python, Java, Spark and Ray versions on every node.
Create `Dockerfile`:

```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    "ray[default,data]==2.40.0" \
    "pandas==2.2.3" \
    pyarrow \
    psutil

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH=$PATH:/usr/local/lib/python3.11/site-packages/pyspark/bin
ENV PYSPARK_PYTHON=python3

WORKDIR /workspace
```

Notes: `pandas` is pinned because pandas 3 is incompatible with Ray 2.40; `ray[default]` is required for the
dashboard; on an ARM machine change `amd64` to `arm64` in `JAVA_HOME`.

```bash
docker build -t dueling-cluster .
```

---

## 5. Spark cluster (1 master + 2 workers)

Create `docker-compose.spark.yml`:

```yaml
services:
  spark-master:
    image: dueling-cluster
    container_name: spark-master
    hostname: spark-master
    command: spark-class org.apache.spark.deploy.master.Master --host spark-master
    environment:
      - SPARK_DAEMON_MEMORY=256m
      - SPARK_MASTER_OPTS=-Dspark.worker.timeout=300
    ports: ["8080:8080", "7077:7077", "4040:4040"]
    volumes: [".:/workspace"]

  spark-worker-1:
    image: dueling-cluster
    container_name: spark-worker-1
    hostname: spark-worker-1
    depends_on: [spark-master]
    command: spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077 --cores 2 --memory 2g
    environment:
      - SPARK_DAEMON_MEMORY=256m
    volumes: [".:/workspace"]

  spark-worker-2:
    image: dueling-cluster
    container_name: spark-worker-2
    hostname: spark-worker-2
    depends_on: [spark-master]
    command: spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077 --cores 2 --memory 2g
    environment:
      - SPARK_DAEMON_MEMORY=256m
    volumes: [".:/workspace"]
```

```bash
docker compose -f docker-compose.spark.yml up -d
docker ps                       # spark-master, spark-worker-1, spark-worker-2 all "Up"
```

Open **http://localhost:8080**: the Spark Master UI must show **2 workers, state ALIVE**.
The job UI is at **http://localhost:4040** while a job runs. The driver runs inside the `spark-master`
container, so scripts are launched with `docker exec`.

Stop it with `docker compose -f docker-compose.spark.yml down`.

---

## 6. Ray cluster (1 head + 2 workers)

Create `docker-compose.ray.yml`:

```yaml
services:
  ray-head:
    image: dueling-cluster
    container_name: ray-head
    hostname: ray-head
    shm_size: 2gb
    command: ray start --head --port=6379 --dashboard-host=0.0.0.0 --num-cpus=0 --block
    ports: ["8265:8265"]
    volumes: [".:/workspace"]

  ray-worker-1:
    image: dueling-cluster
    container_name: ray-worker-1
    hostname: ray-worker-1
    shm_size: 2gb
    depends_on: [ray-head]
    command: ray start --address=ray-head:6379 --num-cpus=2 --block
    volumes: [".:/workspace"]

  ray-worker-2:
    image: dueling-cluster
    container_name: ray-worker-2
    hostname: ray-worker-2
    shm_size: 2gb
    depends_on: [ray-head]
    command: ray start --address=ray-head:6379 --num-cpus=2 --block
    volumes: [".:/workspace"]
```

```bash
docker compose -f docker-compose.ray.yml up -d
docker exec -it ray-head python -c "import ray; ray.init(address='auto'); print(ray.cluster_resources()); print(len(ray.nodes()), 'nodes')"
```

Expect `CPU: 4.0` and 3 nodes. Open **http://localhost:8265**: the Cluster tab shows the head and 2 workers.
The head runs with `--num-cpus=0`, so all work executes on the two workers (mirroring Spark). `shm_size`
is required for Ray's object store.

Stop it with `docker compose -f docker-compose.ray.yml down`.

**Run only one framework at a time.** Running both together would distort every measurement.

---

## 7. The pipeline (identical in both frameworks)

| # | Step | Rule |
|---|---|---|
| 1 | Ingestion | Read all `yellow_tripdata_*.parquet` files. The 2022 and 2023-2024 files have different column types (int32/int64/double, `airport_fee` vs `Airport_fee`), so every file is cast to one canonical schema. Only these columns are kept: `VendorID`, pickup/dropoff timestamps, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `payment_type`, `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`. |
| 2 | Null handling | Drop rows with a null in: both timestamps, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `fare_amount`, `total_amount`. |
| 3 | Duplicates | Remove exact duplicate rows (all kept columns). |
| 4 | Timestamps | Keep pickups from 2022-01-01 up to (not including) 2025-01-01; add `pickup_date` (`yyyy-MM-dd`) and `pickup_hour`. |
| 5 | Sanity filters | Trip duration over 0 and at most 24 h; distance over 0 and at most 100 miles. Adds `trip_duration_hours`. |
| 6 | Heavy join | Left join with `taxi_zone_lookup.csv` twice (pickup and dropoff), adding borough and zone for each. |
| 7 | UDF | A plain Python function `avg_speed_mph(distance, hours)` (rounded to 4 decimals); rows over 100 mph are dropped. The same function is used by both frameworks. |
| 8 | Export | Write Parquet to `output/spark/` or `output/ray/`. |

Design notes:

- **Spark** uses a normal Python UDF (not a built-in expression), so the JVM-to-Python serialization overhead is
  what gets measured.
- **Ray** applies the same Python function row by row inside `map_batches`, so it runs natively in Python.
- **Join:** Ray Data 2.40 has no distributed join, so both pipelines use a broadcast-style join (the lookup has
  only 265 rows). Spark uses `F.broadcast`; Ray merges the small table inside each batch.
- **Deduplication:** Ray Data 2.40 has no `drop_duplicates`. `ray_clean.py` hashes each row into 256 buckets and
  removes exact duplicates inside each bucket (identical rows always share a bucket, so the result is exact).
  Spark uses `dropDuplicates()`.

---

## 8. Running the benchmark

Both scripts take the same flags:

| Flag | Meaning |
|---|---|
| `--files N` | Use only the first N files (default: all 36). **Use the same N for Spark and Ray.** |
| `--run NAME` | Label written to the results file (e.g. `run1`) |
| `--skip-udf-bench` | Skip the separate UDF measurement (use this for full-size runs) |
| `--hold` | Wait at the end so UI screenshots can be taken. Press Enter to exit. |

Spark only: `--partitions` (shuffle partitions, default 64), `--exec-mem`, `--exec-cores`, `--driver-mem`.

### Execution time and resource usage

Each run prints a JSON result and appends it to `benchmarks/spark_results.jsonl` or `benchmarks/ray_results.jsonl`.
`total_seconds` is the wall-clock time from ingestion to the end of the export.

Open a second terminal and log CPU and memory per container while the run is going. **Spark:**

```bash
# terminal 2: CPU/memory logger (stop with Ctrl+C when the run ends)
while true; do docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" | sed "s/^/$(date +%T),/" >> benchmarks/spark_stats.csv; sleep 2; done

# terminal 1: the run (--hold keeps the process open for the :4040 screenshots)
docker exec -it spark-master python /workspace/spark_clean.py --run run1 --skip-udf-bench --hold
```

**Ray** (same logger, writing to `benchmarks/ray_stats.csv`):

```bash
while true; do docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" | sed "s/^/$(date +%T),/" >> benchmarks/ray_stats.csv; sleep 2; done

docker exec -it ray-head python /workspace/ray_clean.py --run run1 --skip-udf-bench --hold
```

Peak CPU and memory: from each stats CSV take the maximum of the CPU % and memory columns per container, and
the maximum of the per-timestamp sum across containers for the cluster total.

Repeat each framework 3 times (`run1`, `run2`, `run3`) and restart the cluster
(`docker compose ... down` then `up -d`) before each timed run. Report the mean.


---

## 9. Verifying that the results match

After running both frameworks on the same N, compare the two output folders: same columns and types, same row
count, same per-column null counts and sums, and the same set of rows. The comparison output is shown in the
report.

---




---
## 10. AI Disclosure (Fair Use of AI)


### 1. Tools used
- **Claude (Anthropic)**: the main AI assistant.


### 2. How they were used
- **Logic translation:** Claude helped translate the cleaning logic between PySpark (`spark_clean.py`) and Ray Data (`ray_clean.py`), so both scripts apply the same rules in the same order.
- **Boilerplate:** Claude helped draft the `Dockerfile` and the `docker-compose` files. I built the image, started both clusters, checked the Spark Master UI (2 workers) and the Ray Dashboard (3 nodes), and fixed the setup on my own VM. The cluster configuration and network setup were done manually.
- **Debugging build and runtime errors:** I pasted error logs and asked for the cause and a fix. Examples are the Java package error in the build, the Ray dashboard install problem, the pandas version problem, and lost Spark executors.
- **Debugging the Ray out-of-memory (OOM) errors:** My Ray runs showed `Workers killed due to memory pressure (OOM)` in the raylet log, plus heavy spilling to disk. I pasted these logs into Claude to understand the cause. It explained that the dedupe step is a full shuffle and that the cluster has only about 7.6 GB of memory. It suggested ways to lower memory use:
  - smaller `batch_size` instead of `batch_size=None`,
  - smaller block size,
  - a higher `num_cpus` per task to lower parallelism,
.

### 3. Impact
AI saved time on translation, setup boilerplate, and understanding errors.

### What AI was NOT used for
- **No performance numbers were generated by AI.** All timings (`total_seconds`, UDF time), and all CPU and memory figures come from my own runs. They can be verified in the `benchmarks/*.jsonl` and `*_stats.csv` files and in my Spark UI and Ray Dashboard screenshots.
- No benchmark output, log or dashboard was invented or edited.


