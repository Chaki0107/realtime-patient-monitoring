# Données d'exemple

Échantillons synthétiques pour tester le pipeline. Aucune donnée réelle de patient.

- Table_Patients.csv : patient_id, nom, age, sexe, ville, maladie_chronique, medecin_referent, numero_urgence, date_inscription (séparateur ;)
- Table_SignauxVitaux.csv : mesures de constantes vitales par patient et par capteur (séparateur ;)
- Table_Capteurs.csv : capteur_id, type_capteur, fabricant, date_installation, batterie_pct, statut_capteur, frequence_mesure_min (séparateur ;)
- nouvelles_observations.csv : mesures envoyées à Kafka pour simuler une smartwatch (séparateur ,)
