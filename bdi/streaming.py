"""
REAL-TIME INGESTION: a producer replays product listings into a Kafka topic as a
live feed; Spark Structured Streaming consumes the topic, parses each JSON message,
applies the same brand and screen-size normalization in Spark SQL, and aggregates
counts per brand in micro-batches. Runs when KAFKA_BOOTSTRAP is set (GitHub Actions
provides a Kafka broker as a service container).
"""
import json
import time

import pandas as pd

from bdi.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, RESULTS, STAGE, get_logger

log = get_logger("streaming")


def produce(n: int = 3000) -> int:
    from kafka import KafkaProducer
    prod = pd.read_parquet(STAGE / "titles.parquet").sample(n, random_state=1)
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    for r in prod.itertuples(index=False):
        producer.send(KAFKA_TOPIC, {"spec_id": r.spec_id, "source": r.source, "page_title": r.page_title,
                                    "event_time": time.time()})
    producer.flush()
    log.info("Produced %d messages to topic %s", n, KAFKA_TOPIC)
    return n


def consume() -> dict:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    from bdi.spark_jobs import session
    spark = session()
    schema = StructType([StructField("spec_id", StringType()), StructField("source", StringType()),
                         StructField("page_title", StringType()), StructField("event_time", DoubleType())])
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", KAFKA_TOPIC).option("startingOffsets", "earliest").load())
    msgs = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("m")).select("m.*")
    low = F.lower(F.col("page_title"))
    brands = ["samsung", "dell", "asus", "lg", "hp", "acer", "philips", "benq", "aoc", "viewsonic", "nec", "iiyama"]
    brand_col = F.coalesce(*[F.when(low.rlike(rf"\b{b}\b"), F.lit(b)) for b in brands], F.lit("other"))
    size = F.regexp_extract(F.col("page_title"), r'(\d{2}(?:\.\d)?)\s*(?:"|in\b|inch)', 1).cast("double")
    enriched = msgs.withColumn("brand", brand_col).withColumn("screen_in", size)
    out_dir = STAGE / "stream_out"
    query = (enriched.writeStream.format("parquet").option("path", str(out_dir))
             .option("checkpointLocation", str(STAGE / "stream_ckpt")).trigger(availableNow=True).start())
    t = time.perf_counter()
    query.awaitTermination()
    elapsed = time.perf_counter() - t
    progress = [p for p in query.recentProgress if p.get("numInputRows")]
    result = spark.read.parquet(str(out_dir))
    by_brand = result.groupBy("brand").count().orderBy(F.desc("count")).toPandas()
    spark.stop()
    rows = int(by_brand["count"].sum())
    res = {"messages_processed": rows, "micro_batches": len(progress), "seconds": round(elapsed, 2),
           "rows_per_second": round(rows / elapsed, 1) if elapsed else None,
           "top_brands": by_brand.head(8).to_dict(orient="records")}
    by_brand.to_csv(RESULTS / "stream_by_brand.csv", index=False)
    log.info("Streaming: %s", res)
    return res


def run() -> dict:
    if not KAFKA_BOOTSTRAP:
        log.warning("KAFKA_BOOTSTRAP not set; streaming step skipped (runs in GitHub Actions)")
        return {"skipped": True}
    produce()
    return consume()
