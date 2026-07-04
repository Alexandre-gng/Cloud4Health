import csv
import random
import time
import uuid
from datetime import date, timedelta
from pathlib import Path



BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
FLUX_DIR = ROOT_DIR / "dossier_flux"
FLUX_DIR.mkdir(parents=True, exist_ok=True)

CSV_HEADER = [
    "report_id",
    "report_date",
    "drug_name",
    "drug_class",
    "adverse_event",
    "severity",
    "outcome",
    "age_group",
    "country",
    "manufacturer",
    "source_type",
    "seriousness_score",
]

DRUGS = [
    ("Metformin", "Antidiabetic", "PharmaA"),
    ("Ibuprofen", "NSAID", "PharmaB"),
    ("Amoxicillin", "Antibiotic", "PharmaC"),
    ("Atorvastatin", "Lipid-lowering", "PharmaD"),
    ("Paracetamol", "Analgesic", "PharmaE"),
    ("Amlodipine", "Antihypertensive", "PharmaF"),
]

ADVERSE_EVENTS = [
    "Nausea",
    "Diarrhea",
    "Headache",
    "Rash",
    "Allergic Reaction",
    "Dizziness",
    "Fatigue",
    "Gastrointestinal Bleeding",
]

SEVERITIES = ["Mild", "Moderate", "Severe", "High", "Grave"]
OUTCOMES = ["Recovered", "Hospitalized", "Improved", "Under Treatment", "Fatal"]
AGE_GROUPS = ["Child", "Adult", "Senior"]
COUNTRIES = ["France", "Germany", "UK", "Spain", "Italy", "Canada"]
SOURCE_TYPES = ["Hospital", "Physician", "Pharmacy", "Patient Report"]


def datetime_stamp():
    """Crée un horodatage court pour les identifiants et noms de fichiers."""
    return time.strftime("%Y%m%d%H%M%S")


def generate_report_rows(batch_size: int):
    """Génère un lot de rapports médicaux bruts compatibles avec le schéma Spark."""
    rows = []
    today = date.today()

    for _ in range(batch_size):
        drug_name, drug_class, manufacturer = random.choice(DRUGS)
        severity = random.choices(SEVERITIES, weights=[45, 28, 14, 8, 5], k=1)[0]
        outcome = random.choices(OUTCOMES, weights=[48, 22, 14, 10, 6], k=1)[0]
        seriousness_score = {
            "Mild": random.randint(1, 3),
            "Moderate": random.randint(3, 5),
            "Severe": random.randint(7, 9),
            "High": random.randint(8, 10),
            "Grave": random.randint(9, 10),
        }[severity]

        report_date = today - timedelta(days=random.randint(0, 180))

        rows.append(
            {
                "report_id": f"R{datetime_stamp()}_{uuid.uuid4().hex[:8]}",
                "report_date": report_date.strftime("%Y-%m-%d"),
                "drug_name": drug_name,
                "drug_class": drug_class,
                "adverse_event": random.choice(ADVERSE_EVENTS),
                "severity": severity,
                "outcome": outcome,
                "age_group": random.choice(AGE_GROUPS),
                "country": random.choice(COUNTRIES),
                "manufacturer": manufacturer,
                "source_type": random.choice(SOURCE_TYPES),
                "seriousness_score": seriousness_score,
            }
        )

    return rows


def write_batch_to_csv(rows):
    """Écrit un lot dans un fichier temporaire puis le rend visible d'un seul coup.

    Cette stratégie évite que Spark lise un fichier encore en cours d'écriture.
    """
    file_stem = f"batch_{datetime_stamp()}_{uuid.uuid4().hex[:6]}"
    temp_path = FLUX_DIR / f"{file_stem}.tmp"
    final_path = FLUX_DIR / f"{file_stem}.csv"

    with temp_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)

    temp_path.replace(final_path)
    print(f"[FLUX] {len(rows)} nouveau(x) rapport(s) écrit(s) dans {final_path.name}")


def main():
    print("Lancement du simulateur de flux brut vers dossier_flux/...")

    try:
        while True:
            batch_size = random.randint(2, 5)
            rows = generate_report_rows(batch_size)
            write_batch_to_csv(rows)
            time.sleep(7)
    except KeyboardInterrupt:
        print("\nSimulateur arrêté proprement.")


if __name__ == "__main__":
    main()