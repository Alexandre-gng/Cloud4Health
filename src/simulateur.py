import time
import random
from pymongo import MongoClient

print("🚀 Lancement du simulateur de flux pour la soutenance...")
client = MongoClient("mongodb://127.0.0.1:27017/")
db = client["pharmacovigilance"]

try:
    while True:
        # 1. On prend un médicament au hasard et on augmente son score de risque
        drugs = list(db["drug_risk_scores"].find())
        if drugs:
            target_drug = random.choice(drugs)
            increment = round(random.uniform(4.0, 12.0), 2)
            db["drug_risk_scores"].update_one(
                {"drug_name": target_drug["drug_name"]},
                {"$inc": {"custom_risk_score": increment}}
            )
            print(f"[FLUX] Alerte reçue pour {target_drug['drug_name']} -> Risque +{increment}")

        # 2. On ajoute quelques cas au mois le plus récent de l'analyse temporelle
        months = list(db["temporal_analysis"].find().sort("report_month", -1).limit(1))
        if months:
            latest_month = months[0]["report_month"]
            new_totals = random.randint(5, 25)
            new_severe = random.randint(1, 8)
            db["temporal_analysis"].update_one(
                {"report_month": latest_month},
                {"$inc": {"total_reports": new_totals, "severe_reports": new_severe}}
            )

        # Attente de 3 secondes avant la prochaine mise à jour
        time.sleep(3)

except KeyboardInterrupt:
    print("\n🛑 Simulateur arrêté.")