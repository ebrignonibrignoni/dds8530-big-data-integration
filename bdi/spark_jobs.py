"""
PySpark processing with both APIs, plus a performance comparison.
  RDD job        attribute frequencies with flatMap / reduceByKey
  DataFrame job  joins products with the entity ground truth and checks whether
                 listings of the same monitor agree on screen size
  Benchmark      the same aggregation (count and mean value length per attribute) in pandas, Dask, Spark RDD and Spark DataFrame
                 at 1x, 4x and 10x the data volume
"""
import time

import dask.dataframe as dd
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from bdi.config import GT_DIR, KAFKA_BOOTSTRAP, RESULTS, STAGE, get_logger

log = get_logger("spark")


SPARK_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def session() -> SparkSession:
    """One Spark configuration for the whole run. Spark starts its Java process once per Python
    process, so the Kafka connector must be requested here, before the first session starts."""
    b = (SparkSession.builder.master("local[*]").appName("alaska-integration")
         .config("spark.sql.shuffle.partitions", "8").config("spark.ui.enabled", "false"))
    if KAFKA_BOOTSTRAP:
        b = b.config("spark.jars.packages", SPARK_KAFKA)
    return b.getOrCreate()


def rdd_job(spark) -> dict:
    df = spark.read.parquet(str(STAGE / "attributes_clean.parquet"))
    rdd = df.select("source", "attribute").rdd
    freq = (rdd.map(lambda r: (r.attribute, 1)).reduceByKey(lambda a, b: a + b)
            .sortBy(lambda kv: -kv[1]))
    top = freq.take(10)
    per_source = rdd.map(lambda r: (r.source, r.attribute)).distinct().countByKey()
    once = freq.filter(lambda kv: kv[1] == 1).count()
    res = {"top_attributes": top, "attributes_used_by_one_listing": once,
           "distinct_attributes_per_source": dict(per_source)}
    log.info("RDD job: top attributes %s; %d attribute names appear only once", top[:5], once)
    return res


def dataframe_job(spark) -> dict:
    prod = spark.read.parquet(str(STAGE / "products.parquet"))
    er = spark.read.csv(str(GT_DIR / "monitor_entity_resolution_gt.csv"), header=True).dropDuplicates()
    joined = er.join(prod, "spec_id", "inner")
    per_entity = (joined.groupBy("entity_id")
                  .agg(F.count("*").alias("listings"), F.countDistinct("source").alias("websites"),
                       F.countDistinct("brand").alias("brands"),
                       F.min("screen_in").alias("min_in"), F.max("screen_in").alias("max_in")))
    per_entity = per_entity.withColumn("size_spread", F.col("max_in") - F.col("min_in"))
    p = per_entity.toPandas()
    res = {"entities": len(p), "median_listings_per_entity": float(p.listings.median()),
           "median_websites_per_entity": float(p.websites.median()),
           "entities_with_conflicting_size_gt_1in": int((p.size_spread > 1).sum()),
           "entities_with_conflicting_brand": int((p.brands > 1).sum())}
    p.to_parquet(STAGE / "entity_consistency.parquet", index=False)
    log.info("DataFrame job: %s", res)
    return res


def _replicate(df, k):
    return pd.concat([df.assign(spec_id=df.spec_id + f"#{i}") for i in range(k)], ignore_index=True)


def benchmark(spark, scales=(1, 4, 10)) -> pd.DataFrame:
    base = pd.read_parquet(STAGE / "attributes_clean.parquet")[["spec_id", "source", "attribute", "value"]]
    rows = []
    for k in scales:
        df = _replicate(base, k)
        df["vlen"] = df["value"].str.len()
        path = STAGE / f"bench_{k}x.parquet"
        df.to_parquet(path, index=False)
        n = len(df)

        t = time.perf_counter()
        pd.read_parquet(path).groupby("attribute").agg(n=("spec_id", "size"), vlen=("vlen", "mean"))
        rows.append(("pandas", k, n, time.perf_counter() - t))

        t = time.perf_counter()
        dd.read_parquet(path).groupby("attribute").agg({"spec_id": "count", "vlen": "mean"}).compute()
        rows.append(("Dask", k, n, time.perf_counter() - t))

        t = time.perf_counter()
        (spark.read.parquet(str(path)).groupBy("attribute")
         .agg(F.count("*"), F.avg("vlen")).collect())
        rows.append(("Spark DataFrame", k, n, time.perf_counter() - t))

        t = time.perf_counter()
        (spark.read.parquet(str(path)).select("attribute", "vlen").rdd
         .map(lambda r: (r.attribute, (1, r.vlen or 0)))
         .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).collect())
        rows.append(("Spark RDD", k, n, time.perf_counter() - t))
        path.unlink()
        log.info("Benchmark %dx (%d rows) done", k, n)
    out = pd.DataFrame(rows, columns=["engine", "scale", "rows", "seconds"])
    out["seconds"] = out["seconds"].round(3)
    out.to_csv(RESULTS / "benchmark.csv", index=False)
    log.info("Benchmark:\n%s", out.to_string(index=False))
    return out


def run() -> dict:
    spark = session()
    try:
        res = {"rdd": rdd_job(spark), "dataframe": dataframe_job(spark)}
        bench = benchmark(spark)
        res["benchmark"] = bench.to_dict(orient="records")
    finally:
        spark.stop()
    return res
