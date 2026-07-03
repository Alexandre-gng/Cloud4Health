from flask import Flask, jsonify, render_template, request
from pymongo import MongoClient, DESCENDING, ASCENDING

app = Flask(__name__)

# Connexion à MongoDB
client = MongoClient("mongodb://127.0.0.1:27017/")
db = client["pharmacovigilance"]

# --- ROUTE POUR SERVIR LE FRONTEND ---

@app.route('/')
def index():
    """Affiche la page principale du Dashboard"""
    # Flask va chercher automatiquement ce fichier dans le dossier 'templates/'
    return render_template('index.html')


# --- ROUTES DE L'API (JSON) ---

@app.route('/api/risk-scores', methods=['GET'])
def get_risk_scores():
    """Récupère le Top 10 des médicaments les plus à risque"""
    # On exclut l'ID interne de Mongo (_id: 0) pour éviter les erreurs de sérialisation JSON
    cursor = db["drug_risk_scores"].find({}, {"_id": 0}).sort("custom_risk_score", DESCENDING).limit(10)
    return jsonify(list(cursor))

@app.route('/api/temporal-data', methods=['GET'])
def get_temporal_data():
    """Récupère l'évolution temporelle triée chronologiquement par mois"""
    cursor = db["temporal_analysis"].find({}, {"_id": 0}).sort("report_month", ASCENDING)
    return jsonify(list(cursor))


@app.route('/api/push-data', methods=['POST'])
def push_data():
    """Ajoute ou met à jour une donnée dans MongoDB en temps réel"""
    payload = request.get_json(silent=True) or {}
    data_type = payload.get("type")

    if data_type == "risk":
        drug_name = (payload.get("drug_name") or "").strip()
        custom_risk_score = payload.get("custom_risk_score")

        if not drug_name or custom_risk_score is None:
            return jsonify({"error": "Les champs drug_name et custom_risk_score sont requis."}), 400

        try:
            custom_risk_score = float(custom_risk_score)
        except (TypeError, ValueError):
            return jsonify({"error": "custom_risk_score doit être un nombre."}), 400

        db["drug_risk_scores"].update_one(
            {"drug_name": drug_name},
            {"$set": {"drug_name": drug_name, "custom_risk_score": custom_risk_score}},
            upsert=True,
        )
        return jsonify({"message": "Score de risque enregistré."}), 201

    if data_type == "temporal":
        report_month = (payload.get("report_month") or "").strip()
        total_reports = payload.get("total_reports")
        severe_reports = payload.get("severe_reports")

        if not report_month or total_reports is None or severe_reports is None:
            return jsonify({"error": "Les champs report_month, total_reports et severe_reports sont requis."}), 400

        try:
            total_reports = int(total_reports)
            severe_reports = int(severe_reports)
        except (TypeError, ValueError):
            return jsonify({"error": "total_reports et severe_reports doivent être des nombres entiers."}), 400

        db["temporal_analysis"].update_one(
            {"report_month": report_month},
            {
                "$set": {
                    "report_month": report_month,
                    "total_reports": total_reports,
                    "severe_reports": severe_reports,
                }
            },
            upsert=True,
        )
        return jsonify({"message": "Donnée temporelle enregistrée."}), 201

    return jsonify({"error": "Type de donnée invalide."}), 400


if __name__ == '__main__':
    # Lancement du serveur Flask sur le port 8000
    app.run(debug=True, host='127.0.0.1', port=8000)