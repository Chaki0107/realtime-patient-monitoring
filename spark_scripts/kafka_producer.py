"""
kafka_producer.py
Simule une smartwatch qui envoie des mesures en temps réel vers Kafka.
Lit le dataset de nouvelles observations ligne par ligne avec un délai simulé.

Usage : python3 /spark_scripts/kafka_producer.py
"""

import json
import time
import csv
import random
from kafka import KafkaProducer

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
KAFKA_BROKER  = 'kafka:9092'
TOPIC         = 'mesures-patients'
DATASET_PATH  = '/tmp/nouvelles_observations.csv'
DELAI_SECONDES = 2   # délai entre chaque mesure envoyée

# ─────────────────────────────────────────
# INITIALISATION DU PRODUCER
# ─────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

print(f"[PRODUCER] Connecté à Kafka : {KAFKA_BROKER}")
print(f"[PRODUCER] Topic cible      : {TOPIC}")
print(f"[PRODUCER] Dataset source   : {DATASET_PATH}")
print(f"[PRODUCER] Délai par mesure : {DELAI_SECONDES}s")
print("─" * 50)

# ─────────────────────────────────────────
# LECTURE ET ENVOI DU DATASET
# ─────────────────────────────────────────
def envoyer_mesures():
    while True:
        with open(DATASET_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            nb_envoyes = 0

            for row in reader:
                # Construire le message JSON avec les mêmes colonnes que signauxvitaux
                message = {
                    "mesure_id"                  : row.get("mesure_id", f"RT_{nb_envoyes:05d}"),
                    "patient_id"                 : row.get("patient_id", ""),
                    "timestamp"                  : row.get("timestamp", ""),
                    "date"                       : row.get("date", ""),
                    "heure"                      : row.get("heure", ""),
                    "temperature_C"              : float(row.get("temperature_C", 0)),
                    "tension_systolique_mmHg"    : int(row.get("tension_systolique_mmHg", 0)),
                    "tension_diastolique_mmHg"   : int(row.get("tension_diastolique_mmHg", 0)),
                    "frequence_cardiaque_bpm"    : int(row.get("frequence_cardiaque_bpm", 0)),
                    "spo2_pct"                   : float(row.get("spo2_pct", 0)),
                    "frequence_respiratoire_rpm" : int(row.get("frequence_respiratoire_rpm", 0)),
                    "glycemie_g_L"               : float(row.get("glycemie_g_L", 0)),
                    "poids_kg"                   : float(row.get("poids_kg", 0)),
                    "score_douleur_0_10"         : int(row.get("score_douleur_0_10", 0)),
                    "capteur_id"                 : row.get("capteur_id", ""),
                    "source"                     : "smartwatch"
                }

                # Clé = patient_id pour que Kafka partitionne par patient
                producer.send(
                    TOPIC,
                    key=message["patient_id"],
                    value=message
                )
                producer.flush()

                nb_envoyes += 1
                print(f"[PRODUCER] Envoyé #{nb_envoyes} | "
                    f"Patient: {message['patient_id']} | "
                    f"FC: {message['frequence_cardiaque_bpm']} bpm | "
                    f"SpO2: {message['spo2_pct']}% | "
                    f"Tension: {message['tension_systolique_mmHg']}/{message['tension_diastolique_mmHg']}")

                time.sleep(DELAI_SECONDES)

        print(f"\n[PRODUCER] Terminé. {nb_envoyes} mesures envoyées.")
     

if __name__ == "__main__":
    envoyer_mesures()
