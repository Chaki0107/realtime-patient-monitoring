"""
spark_alertes.py - VERSION FINALE
Spark Structured Streaming :
  1. Charge les seuils depuis Hive
  2. Charge les infos patients depuis Hive
  3. Consomme Kafka "mesures-patients"
  4. Compare aux seuils + enrichit avec profil patient
  5. Envoie alertes à Flask Socket.IO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType
)
import socketio
import threading

KAFKA_BROKER   = 'kafka:9092'
TOPIC          = 'mesures-patients'
FLASK_URL      = 'http://dashboard-alertes:5000'
HIVE_DB        = 'surveillance'
BATCH_INTERVAL = '5 seconds'

# ── Socket.IO client ──
sio = socketio.Client()

def connecter_socket():
    try:
        sio.connect(FLASK_URL)
        print(f"[SPARK] Connecté à Flask : {FLASK_URL}")
    except Exception as e:
        print(f"[SPARK] Erreur connexion Flask : {e}")

t = threading.Thread(target=connecter_socket)
t.start()
t.join(timeout=10)

# ── Session Spark ──
spark = SparkSession.builder \
    .appName("SurveillancePatientsStreaming") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("[SPARK] Session démarrée.")

# ── Chargement seuils ──
print("[SPARK] Chargement des seuils depuis Hive...")
seuils_df = spark.sql(f"""
    SELECT signal, seuil_warning_min, seuil_warning_max,
           seuil_critique_min, seuil_critique_max, unite
    FROM {HIVE_DB}.seuils_alerte
""")
seuils = {}
for row in seuils_df.collect():
    seuils[row['signal']] = {
        'warn_min': row['seuil_warning_min'],
        'warn_max': row['seuil_warning_max'],
        'crit_min': row['seuil_critique_min'],
        'crit_max': row['seuil_critique_max'],
        'unite':    row['unite']
    }
print(f"[SPARK] {len(seuils)} seuils chargés : {list(seuils.keys())}")

# ── Chargement profils patients ──
print("[SPARK] Chargement des infos patients depuis Hive...")
profils_patients = {}
try:
    patients_df = spark.sql(f"""
        SELECT patient_id, nom, age, sexe, ville,
               maladie_chronique, medecin_referent, numero_urgence, date_inscription
        FROM {HIVE_DB}.table_patients
    """)
    profils_patients = {}
    for row in patients_df.collect():
        profils_patients[row['patient_id']] = {
            'nom':         row['nom'],
            'age':         row['age'],
            'sexe':        row['sexe'],
            'ville':       row['ville'],
            'maladie':     row['maladie_chronique'],
            'medecin':     row['medecin_referent'],
            'urgence':     row['numero_urgence'],
            'inscription': row['date_inscription'],
        }
    print(f"[SPARK] {len(profils_patients)} patients chargés. ")
except Exception as e:
    print(f"[SPARK] Erreur : {e}")



# ── Schéma Kafka ──
schema_mesure = StructType([
    StructField("mesure_id",                  StringType(),  True),
    StructField("patient_id",                 StringType(),  True),
    StructField("timestamp",                  StringType(),  True),
    StructField("date",                       StringType(),  True),
    StructField("heure",                      StringType(),  True),
    StructField("temperature_C",              FloatType(),   True),
    StructField("tension_systolique_mmHg",    IntegerType(), True),
    StructField("tension_diastolique_mmHg",   IntegerType(), True),
    StructField("frequence_cardiaque_bpm",    IntegerType(), True),
    StructField("spo2_pct",                   FloatType(),   True),
    StructField("frequence_respiratoire_rpm", IntegerType(), True),
    StructField("glycemie_g_L",               FloatType(),   True),
    StructField("poids_kg",                   FloatType(),   True),
    StructField("score_douleur_0_10",         IntegerType(), True),
    StructField("capteur_id",                 StringType(),  True),
    StructField("source",                     StringType(),  True),
])

# ── Lecture Kafka ──
stream_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

mesures_stream = stream_kafka \
    .select(from_json(col("value").cast("string"), schema_mesure).alias("m")) \
    .select("m.*")

# ── Évaluation seuils ──
def evaluer_niveau(signal, valeur):
    if signal not in seuils or valeur is None:
        return "NORMAL"
    s = seuils[signal]
    if valeur < s['crit_min'] or valeur > s['crit_max']:
        return "CRITIQUE"
    if valeur < s['warn_min'] or valeur > s['warn_max']:
        return "WARNING"
    return "NORMAL"

# ── Traitement batch ──
def traiter_batch(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    print(f"\n[SPARK] Batch #{batch_id} — {len(rows)} mesure(s)")

    for row in rows:
        pid_kafka = row['patient_id']  # ex: "P003" ou "PAT0003"
        # Étape de normalisation ultra-robuste :
        # Si c'est P003, on veut PAT0003. 
        # On enlève le 'P', on garde le reste, et on met 'PAT' devant.
        if pid_kafka and pid_kafka.startswith('P') and not pid_kafka.startswith('PAT'):
            chiffres = pid_kafka[1:] # prend tout après le 'P'
            # On s'assure d'avoir 4 chiffres (0003) pour correspondre à Hive
            pid = f"PAT{chiffres.zfill(4)}" 
        else:
            pid = pid_kafka

        print(f"[DEBUG] Recherche profil pour : {pid}") # Ajoute ça pour voir dans les logs
        profil = profils_patients.get(pid, {})

        signaux = {
            'frequence_cardiaque':    row['frequence_cardiaque_bpm'],
            'spo2':                   row['spo2_pct'],
            'temperature_C':          row['temperature_C'],
            'tension_systolique':     row['tension_systolique_mmHg'],
            'tension_diastolique':    row['tension_diastolique_mmHg'],
            'glycemie':               row['glycemie_g_L'],
            'frequence_respiratoire': row['frequence_respiratoire_rpm'],
        }

        alertes_patient = []
        for signal, valeur in signaux.items():
            if valeur is None:
                continue
            niveau = evaluer_niveau(signal, valeur)
            if niveau in ("CRITIQUE", "WARNING"):
                s = seuils.get(signal, {})
                alerte = {
                    "alerte_id"      : f"{row['mesure_id']}_{signal}",
                    "patient_id"     : pid,
                    "mesure_id"      : row['mesure_id'],
                    "timestamp"      : row['timestamp'],
                    "date"           : row['date'],
                    "heure"          : row['heure'],
                    "signal"         : signal,
                    "valeur_mesuree" : round(float(valeur), 2),
                    "unite"          : s.get('unite', ''),
                    "seuil_warn_min" : s.get('warn_min'),
                    "seuil_warn_max" : s.get('warn_max'),
                    "niveau_alerte"  : niveau,
                    "source"         : row['source'] if row['source'] else 'smartwatch'
                }
                alertes_patient.append(alerte)
                print(f"  [{niveau}] {pid} | {signal} = {valeur} {s.get('unite','')}")
                try:
                    sio.emit("alerte", alerte)
                except Exception as e:
                    print(f"  [ERREUR socket] {e}")


        # ── BLOC CENTRE PROCHE — juste avant le payload ──
        centre_proche = None
        ville_patient = profil.get('ville', '').lower()
        for gouv, centres in centres_sante.items():
            if gouv.lower() in ville_patient or ville_patient in gouv.lower():
                centre_proche = centres[0]
                break
        if not centre_proche and 'Tunis' in centres_sante:
            centre_proche = centres_sante['Tunis'][0]

        # Payload enrichi avec profil patient
        payload = {
            "patient_id"  : pid,
            "nom"         : profil.get('nom', pid),
            "age"         : profil.get('age', '?'),
            "sexe"        : profil.get('sexe', '?'),
            "ville"       : profil.get('ville', 'Inconnue'),
            "maladie"     : profil.get('maladie', '?'),
            "medecin"     : profil.get('medecin', '?'),
            "urgence"     : profil.get('urgence', '?'),
            "inscription" : profil.get('inscription', '?'),
            "timestamp"   : row['timestamp'],
            "fc"          : row['frequence_cardiaque_bpm'],
            "spo2"        : row['spo2_pct'],
            "tension"     : f"{row['tension_systolique_mmHg']}/{row['tension_diastolique_mmHg']}",
            "glycemie"    : row['glycemie_g_L'],
            "temperature" : row['temperature_C'],
            "nb_alertes"  : len(alertes_patient),
            "niveau_max"  : "CRITIQUE" if any(a["niveau_alerte"]=="CRITIQUE"
                                               for a in alertes_patient)
                            else "WARNING" if alertes_patient else "NORMAL",
            "alertes"     : alertes_patient,
        }
        try:
            sio.emit("patient_update", payload)
        except Exception as e:
            print(f"  [ERREUR patient_update] {e}")

# ── Chargement centres de santé ──
centres_sante = {}
try:
    centres_df = spark.sql("""
        SELECT centre_id, nom_centre, ville, gouvernorat,
               type_centre, telephone
        FROM surveillance.centres_sante
    """)
    for row in centres_df.collect():
        gouv = row['gouvernorat']
        if gouv not in centres_sante:
            centres_sante[gouv] = []
        centres_sante[gouv].append({
            'nom':  row['nom_centre'],
            'ville': row['ville'],
            'type':  row['type_centre'],
            'tel':   row['telephone'],
        })
    print(f"[SPARK] {sum(len(v) for v in centres_sante.values())} centres chargés")
except Exception as e:
    print(f"[SPARK] Centres non disponibles : {e}")    

# ── Lancement streaming ──
print(f"[SPARK] Streaming démarré (batch/{BATCH_INTERVAL})...")
print("[SPARK] En attente de mesures Kafka...\n")

query = mesures_stream.writeStream \
    .foreachBatch(traiter_batch) \
    .trigger(processingTime=BATCH_INTERVAL) \
    .option("checkpointLocation", "/tmp/spark_checkpoint_alertes") \
    .start()

query.awaitTermination()
