import datetime

import os
import sys
import pyspark.sql.functions as F
import matplotlib


# Configuration of env variables for Hadoop and Spark
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += r';C:\hadoop\bin'
matplotlib.use('Agg')


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, year, date_format, count, avg, desc, when
import matplotlib.pyplot as plt


# Cleaning possible ghost session
session_active = SparkSession.getActiveSession()
if session_active:
    session_active.stop()


# Init spark session with MongoDB connector
spark = SparkSession.builder \
    .appName("DrugSafetyAndPharmacovigilanceAnalytics") \
    .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1:27017/pharmacovigilance") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.5.0") \
    .getOrCreate()

print("Spark session initialized with MongoDB connector.")


# paths init
root_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(root_dir, "..", "data", "dataset_project.csv")
figures_dir = os.path.join(root_dir, "..", "figures")
os.makedirs(figures_dir, exist_ok=True)


# Loading dataset
df = spark.read.csv(csv_path, header=True, inferSchema=True)


print("=== DONNÉES CHARGÉES AVEC SUCCÈS ===")
# Conversion dates
df = df.withColumn("report_date", to_date(col("report_date"))) \
       .withColumn("report_month", date_format(col("report_date"), "yyyy-MM"))

print("=== APERÇU DES DONNÉES CHARGÉES ===")
df.show(5, truncate=False)




# ==========================================
# PART 1 : Drug Analysis
# ==========================================
print("\n" + "="*40 + "\n[drug analysis]\n" + "="*40)

# TOP 10 drugs by total reports (global volume)
drug_counts = df.groupBy("drug_name") \
                .agg(count("report_id").alias("total_reports")) \
                .orderBy(desc("total_reports"))

print("1. Top drugs by total reports (global volume) :")
drug_counts.show(10)


def save_horizontal_bar_chart(rows, x_key, y_key, title, filename, color="#2c3e50"):
    if not rows:
        return

    labels = [row[x_key] for row in rows]
    values = [row[y_key] for row in rows]

    plt.figure(figsize=(10, 6))
    plt.barh(labels[::-1], values[::-1], color=color)
    plt.title(title)
    plt.xlabel(y_key.replace("_", " ").title())
    plt.tight_layout()
    plt.savefig(os.path.join(figures_dir, filename), dpi=150)
    plt.close()


def save_line_chart(rows, x_key, y_keys, title, filename):
    if not rows:
        return

    x_values = [row[x_key] for row in rows]
    plt.figure(figsize=(10, 6))

    for y_key, color in y_keys:
        y_values = [row[y_key] for row in rows]
        plt.plot(x_values, y_values, marker="o", linewidth=2, label=y_key.replace("_", " ").title(), color=color)

    plt.title(title)
    plt.xlabel(x_key.replace("_", " ").title())
    plt.ylabel("Number of Reports")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(figures_dir, filename), dpi=150)
    plt.close()


top_drugs_rows = drug_counts.limit(10).collect()
save_horizontal_bar_chart(top_drugs_rows, "drug_name", "total_reports", "Top 10 drugs by total reports", "top_reported_drugs.png")

# Number of severe reports by drug
# (We filter here on the labels 'Severe' or 'High' in the severity column)
severe_drugs = df.filter(col("severity").isin("Severe", "High", "Grave")) \
                 .groupBy("drug_name") \
                 .agg(count("report_id").alias("severe_reports")) \
                 .orderBy(desc("severe_reports"))

print("2. Number of 'Severe' reports by drug :")
severe_drugs.show(10)
save_horizontal_bar_chart(severe_drugs.limit(10).collect(), "drug_name", "severe_reports", "Top 10 drugs with severe reports", "severe_reported_drugs.png", color="#c0392b")




# ==========================================
# PART 2 : Adverse Event Analysis
# ==========================================
print("\n" + "="*40 + "\n[Adverse Event Analysis]\n" + "="*40)

# Most frequent adverse events
frequent_ae = df.groupBy("adverse_event") \
                .agg(count("report_id").alias("event_count")) \
                .orderBy(desc("event_count"))

print("1. Most frequent adverse events :")
frequent_ae.show(10)
save_horizontal_bar_chart(frequent_ae.limit(10).collect(), "adverse_event", "event_count", "Top 10 most frequent adverse events", "frequent_adverse_events.png", color="#16a085")

# Most severe adverse events (based on the average of the seriousness_score)
severe_ae = df.groupBy("adverse_event") \
              .agg(avg("seriousness_score").alias("avg_seriousness_score")) \
              .orderBy(desc("avg_seriousness_score"))

print("2. Most severe adverse events with the highest average severity score :")
severe_ae.show(10)




print("\n" + "="*40 + "\n[Temporal Analysis]\n" + "="*40)

# Monthly evolution of total reports and severe cases
temporal_analysis = df.groupBy("report_month").agg(
    F.count("report_id").alias("total_reports"),
    F.sum(F.when(F.col("severity").isin("Severe", "High", "Grave"), 1).otherwise(0)).alias("severe_reports")
).orderBy("report_month")

print("Monthly evolution of reports (Totals vs Severe) :")
temporal_analysis.show(12)
save_line_chart(temporal_analysis.collect(), "report_month", [("total_reports", "#3498db"), ("severe_reports", "#9b59b6")], "Monthly evolution of reports", "temporal_analysis.png")




# ==========================================
# PART 4: Custom Risk Score Calculation
# ==========================================
print("\n" + "="*40 + "\n[Custom Risk Score Calculation and Ranking]\n" + "="*40)

# Volume of reports and average seriousness score by drug
risk_score_df = df.groupBy("drug_name").agg(
    count("report_id").alias("total_reports"),
    avg("seriousness_score").alias("avg_seriousness")
)

# Application of the custom risk score formula: custom_risk_score = total_reports * avg_seriousness
risk_score_df = risk_score_df.withColumn("custom_risk_score", col("total_reports") * col("avg_seriousness")) \
                             .orderBy(desc("custom_risk_score"))

print("Ranked drugs by custom risk score :")
risk_score_df.show(10)
save_horizontal_bar_chart(risk_score_df.limit(10).collect(), "drug_name", "custom_risk_score", "Top 10 des scores de risque calculés", "drug_risk_scores.png", color="#8e44ad")




# ==========================================
# PART 5 : Signal Detection
# ==========================================
print("\n" + "="*40 + "\n[Signal Detection]\n" + "="*40)

# Example asked: Identify the pairs (drug, adverse event) frequently associated with hospitalization
hospitalization_signals = df.filter(col("outcome").like("%Hospital%") | col("outcome").like("%hospital%")) \
                            .groupBy("drug_name", "adverse_event") \
                            .agg(count("report_id").alias("hospitalization_count")) \
                            .orderBy(desc("hospitalization_count"))

print("Top couples of drugs and adverse events leading to hospitalization :")
hospitalization_signals.show(15, truncate=False)
save_horizontal_bar_chart(hospitalization_signals.limit(15).collect(), "drug_name", "hospitalization_count", "Top of hospitalization signals", "hospitalization_signals.png", color="#d35400")


# ==========================================
# 2. export to MongoDB
# ==========================================
def save_df_to_mongodb(df, collection_name):
    """
    Écrit un DataFrame PySpark dans une collection MongoDB.
    On utilise le mode 'overwrite' pour écraser les données à chaque exécution du batch.
    """
    print(f"Saving collection '{collection_name}' to MongoDB...")
    df.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "pharmacovigilance") \
        .option("collection", collection_name) \
        .save()
    print("Succès !")

print("\n" + "="*40 + "\n[Export to MongoDB]\n" + "="*40)

# ==========================================
# 3. save all DataFrames to MongoDB
# ==========================================
try:
    save_df_to_mongodb(drug_counts, "top_reported_drugs")
    save_df_to_mongodb(severe_drugs, "severe_reported_drugs")
    save_df_to_mongodb(frequent_ae, "frequent_adverse_events")
    save_df_to_mongodb(severe_ae, "severe_adverse_events")
    save_df_to_mongodb(temporal_analysis, "temporal_analysis")
    save_df_to_mongodb(risk_score_df, "drug_risk_scores")
    save_df_to_mongodb(hospitalization_signals, "hospitalization_signals")
    
    print("\nAll data has been successfully exported to MongoDB.")
except Exception as e:
    print(f"\nError during export to MongoDB : {e}")

# Arrêt propre de la session
try:
    spark.stop()
    print("Spark stopped successfully.")
except Exception as e:
    print(f"Error occurred while stopping the Spark session : {e}")