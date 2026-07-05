import os
import matplotlib

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += r';C:\hadoop\bin'
matplotlib.use('Agg')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pymongo import MongoClient, UpdateOne


# -----------------------------------------------------------------------------
# Configuration globale
# -----------------------------------------------------------------------------
# Le simulateur dépose ici des fichiers CSV bruts. Spark Structured Streaming
# surveille ce dossier et traite chaque nouveau fichier dès son apparition.
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(ROOT_DIR, "dossier_flux")
CHECKPOINT_DIR = os.path.join(ROOT_DIR, "checkpoints_streaming_v3")

# If in an image:
# MONGO_URI = "mongodb://mongo_pharmacovigilance:27017/pharmacovigilance"
# If not in an image (local dev):
# MONGO_URI = "mongodb://127.0.0.1:27017/pharmacovigilance"
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017/pharmacovigilance")
MONGO_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.13:10.5.0"

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)


# -----------------------------------------------------------------------------
# Schéma strict du flux brut
# -----------------------------------------------------------------------------
# Le schéma est volontairement explicite pour éviter les ambiguïtés de typage
# et garantir un comportement stable en streaming.
RAW_SCHEMA = T.StructType(
    [
        T.StructField("report_id", T.StringType(), True),
        T.StructField("report_date", T.StringType(), True),
        T.StructField("drug_name", T.StringType(), True),
        T.StructField("drug_class", T.StringType(), True),
        T.StructField("adverse_event", T.StringType(), True),
        T.StructField("severity", T.StringType(), True),
        T.StructField("outcome", T.StringType(), True),
        T.StructField("age_group", T.StringType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("manufacturer", T.StringType(), True),
        T.StructField("source_type", T.StringType(), True),
        T.StructField("seriousness_score", T.IntegerType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    """Construit la session Spark configurée pour MongoDB 10.5.0."""
    session_active = SparkSession.getActiveSession()
    if session_active:
        session_active.stop()

    return (
        SparkSession.builder.appName("DrugSafetyAndPharmacovigilanceStreaming")
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.jars.packages", MONGO_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def normalize_stream(df):
    """Normalise le flux brut et ajoute les colonnes dérivées utiles aux agrégations."""
    return (
        df.withColumn("report_date", F.to_date(F.col("report_date"), "yyyy-MM-dd"))
        .withColumn("report_month", F.date_format(F.col("report_date"), "yyyy-MM"))
    )


def _bulk_upsert_count_collection(collection, items, key_fields, count_field):
    operations = []
    for item in items:
        filter_doc = {field: item[field] for field in key_fields}
        update_doc = {
            "$setOnInsert": filter_doc,
            "$inc": {count_field: int(item[count_field])},
        }
        operations.append(UpdateOne(filter_doc, update_doc, upsert=True))

    if operations:
        collection.bulk_write(operations, ordered=False)


def _upsert_average_collection(collection, items, key_fields, count_field, sum_field, avg_field):
    for item in items:
        filter_doc = {field: item[field] for field in key_fields}
        existing = collection.find_one(filter_doc) or {}
        current_count = int(existing.get(count_field, 0)) + int(item[count_field])
        current_sum = float(existing.get(sum_field, 0)) + float(item[sum_field])
        current_avg = current_sum / current_count if current_count else 0.0

        document = {
            **filter_doc,
            count_field: current_count,
            sum_field: current_sum,
            avg_field: current_avg,
        }
        if avg_field == "custom_risk_score":
            document[avg_field] = current_count * current_avg

        collection.replace_one(filter_doc, document, upsert=True)


def process_microbatch(batch_df, batch_id):
    """Calcule les indicateurs sur un micro-lot et cumule l'état dans MongoDB."""
    batch = normalize_stream(batch_df)
    severe_labels = ["Severe", "High", "Grave"]
    hospitalization_condition = F.lower(F.col("outcome")).contains("hospital")

    mongo_client = MongoClient(MONGO_URI)
    default_db = mongo_client.get_default_database()
    db = default_db if default_db is not None else mongo_client["pharmacovigilance"]

    try:
        drug_counts = batch.groupBy("drug_name").agg(F.count("report_id").alias("total_reports")).collect()
        severe_drugs = batch.filter(F.col("severity").isin(severe_labels)) \
            .groupBy("drug_name").agg(F.count("report_id").alias("severe_reports")).collect()
        frequent_ae = batch.groupBy("adverse_event").agg(F.count("report_id").alias("event_count")).collect()
        severe_ae = batch.groupBy("adverse_event") \
            .agg(F.count("report_id").alias("report_count"), F.sum("seriousness_score").alias("seriousness_sum")).collect()
        temporal = batch.groupBy("report_month").agg(
            F.count("report_id").alias("total_reports"),
            F.sum(F.when(F.col("severity").isin(severe_labels), F.lit(1)).otherwise(F.lit(0))).alias("severe_reports"),
        ).collect()
        risk_scores = batch.groupBy("drug_name").agg(
            F.count("report_id").alias("total_reports"),
            F.sum("seriousness_score").alias("seriousness_sum"),
        ).collect()
        hospitalization_signals = batch.filter(hospitalization_condition).groupBy("drug_name", "adverse_event") \
            .agg(F.count("report_id").alias("hospitalization_count")).collect()

        _bulk_upsert_count_collection(db["top_reported_drugs"], drug_counts, ["drug_name"], "total_reports")
        _bulk_upsert_count_collection(db["severe_reported_drugs"], severe_drugs, ["drug_name"], "severe_reports")
        _bulk_upsert_count_collection(db["frequent_adverse_events"], frequent_ae, ["adverse_event"], "event_count")
        _bulk_upsert_count_collection(db["temporal_analysis"], temporal, ["report_month"], "total_reports")

        for item in temporal:
            db["temporal_analysis"].update_one(
                {"report_month": item["report_month"]},
                {"$inc": {"severe_reports": int(item["severe_reports"])}},
                upsert=True,
            )

        for item in risk_scores:
            key = {"drug_name": item["drug_name"]}
            existing = db["drug_risk_scores"].find_one(key) or {}
            total_reports = int(existing.get("total_reports", 0)) + int(item["total_reports"])
            seriousness_sum = float(existing.get("seriousness_sum", 0)) + float(item["seriousness_sum"])
            avg_seriousness = seriousness_sum / total_reports if total_reports else 0.0
            db["drug_risk_scores"].replace_one(
                key,
                {
                    "drug_name": item["drug_name"],
                    "total_reports": total_reports,
                    "seriousness_sum": seriousness_sum,
                    "avg_seriousness": avg_seriousness,
                    "custom_risk_score": total_reports * avg_seriousness,
                },
                upsert=True,
            )

        for item in severe_ae:
            key = {"adverse_event": item["adverse_event"]}
            existing = db["severe_adverse_events"].find_one(key) or {}
            report_count = int(existing.get("report_count", 0)) + int(item["report_count"])
            seriousness_sum = float(existing.get("seriousness_sum", 0)) + float(item["seriousness_sum"])
            avg_seriousness_score = seriousness_sum / report_count if report_count else 0.0
            db["severe_adverse_events"].replace_one(
                key,
                {
                    "adverse_event": item["adverse_event"],
                    "report_count": report_count,
                    "seriousness_sum": seriousness_sum,
                    "avg_seriousness_score": avg_seriousness_score,
                },
                upsert=True,
            )

        _bulk_upsert_count_collection(
            db["hospitalization_signals"],
            hospitalization_signals,
            ["drug_name", "adverse_event"],
            "hospitalization_count",
        )

        print(f"Micro-lot {batch_id} traité et cumulé dans MongoDB.")
    finally:
        mongo_client.close()


def main():
    """Point d'entrée principal du traitement temps réel."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    print("Spark Structured Streaming initialisé.")
    print(f"Dossier d'écoute : {INPUT_DIR}")
    print("Le simulateur doit déposer de nouveaux fichiers CSV dans ce dossier.")

    raw_stream = (
        spark.readStream.schema(RAW_SCHEMA)
        .option("header", True)
        .option("maxFilesPerTrigger", 1)
        .csv(INPUT_DIR)
    )

    query = (
        raw_stream.writeStream
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .foreachBatch(process_microbatch)
        .trigger(processingTime="3 seconds")
        .start()
    )

    print("\nFlux actifs. Appuyez sur Ctrl+C pour arrêter proprement.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")
    finally:
        if query.isActive:
            query.stop()
        spark.stop()
        print("Session Spark arrêtée.")


if __name__ == "__main__":
    main()