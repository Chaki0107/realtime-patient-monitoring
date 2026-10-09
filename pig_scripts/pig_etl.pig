-- ============================================================
-- SCRIPT PIG : ETL & Préparation des colonnes
-- Projet : Surveillance patients âgés à domicile
-- ============================================================
-- Usage : pig /pig_scripts/pig_etl.pig
-- Output : /user/hadoop/output/pig_output
-- ============================================================

-- ─────────────────────────────────────────
-- ÉTAPE 1 : CHARGEMENT DES 3 DATASETS
-- ─────────────────────────────────────────

patients = LOAD '/user/hadoop/input/Table_Patients.csv'
    USING PigStorage(';')
    AS (
        patient_id:chararray,
        nom:chararray,
        age:int,
        sexe:chararray,
        ville:chararray,
        maladie_chronique:chararray,
        medecin_referent:chararray,
        numero_urgence:chararray,
        date_inscription:chararray
    );


signaux = LOAD '/user/hadoop/input/Table_SignauxVitaux.csv'
    USING PigStorage(';')
    AS (
        mesure_id:chararray,
        patient_id:chararray,
        timestamp:chararray,
        date:chararray,
        heure:chararray,
        temperature_C:float,
        tension_systolique_mmHg:int,
        tension_diastolique_mmHg:int,
        frequence_cardiaque_bpm:int,
        spo2_pct:float,
        frequence_respiratoire_rpm:int,
        glycemie_g_L:float,
        poids_kg:float,
        imc:float,
        activite_physique_steps:int,
        duree_sommeil_h:float,
        score_douleur_0_10:int,
        capteur_id:chararray,
        qualite_signal:chararray
    );



capteurs = LOAD '/user/hadoop/input/Table_Capteurs.csv'
    USING PigStorage(';')
    AS (
        capteur_id:chararray,
        type_capteur:chararray,
        fabricant:chararray,
        date_installation:chararray,
        batterie_pct:int,
        statut_capteur:chararray,
        frequence_mesure_min:int
    );



-- ─────────────────────────────────────────
-- ÉTAPE 2 : SUPPRIMER LES EN-TÊTES CSV
-- ─────────────────────────────────────────

patients_clean = FILTER patients BY patient_id != 'patient_id';
signaux_clean  = FILTER signaux  BY mesure_id  != 'mesure_id';
capteurs_clean = FILTER capteurs BY capteur_id != 'capteur_id';

-- ─────────────────────────────────────────
-- ÉTAPE 3 : FILTRAGE QUALITÉ CAPTEURS
-- Garder uniquement les capteurs actifs
-- avec batterie suffisante (>= 20%)
-- ─────────────────────────────────────────

capteurs_ok = FILTER capteurs_clean BY statut_capteur == 'Actif' AND batterie_pct >= 20;


-- ─────────────────────────────────────────
-- ÉTAPE 4 : FILTRAGE QUALITÉ SIGNAUX
-- Garder uniquement les mesures avec
-- signal acceptable (bon ou moyen)
-- ─────────────────────────────────────────

signaux_ok = FILTER signaux_clean BY
    qualite_signal != 'mauvais'
    AND frequence_cardiaque_bpm IS NOT NULL
    AND spo2_pct IS NOT NULL
    AND temperature_C IS NOT NULL
    AND tension_systolique_mmHg IS NOT NULL
    AND tension_diastolique_mmHg IS NOT NULL
    AND glycemie_g_L IS NOT NULL
    AND frequence_respiratoire_rpm IS NOT NULL;



-- ─────────────────────────────────────────
-- ÉTAPE 5 : JOINTURE signaux + capteurs
-- Enrichir chaque mesure avec les infos
-- du capteur correspondant
-- ─────────────────────────────────────────

signaux_enrichis = JOIN signaux_ok BY capteur_id,
                        capteurs_ok BY capteur_id;

-- Projeter les colonnes utiles après join
signaux_proj = FOREACH signaux_enrichis GENERATE
    signaux_ok::mesure_id          AS mesure_id,
    signaux_ok::patient_id         AS patient_id,
    signaux_ok::date               AS date,
    signaux_ok::heure              AS heure,
    signaux_ok::temperature_C      AS temperature_C,
    signaux_ok::tension_systolique_mmHg   AS tension_systolique_mmHg,
    signaux_ok::tension_diastolique_mmHg  AS tension_diastolique_mmHg,
    signaux_ok::frequence_cardiaque_bpm   AS frequence_cardiaque_bpm,
    signaux_ok::spo2_pct           AS spo2_pct,
    signaux_ok::frequence_respiratoire_rpm AS frequence_respiratoire_rpm,
    signaux_ok::glycemie_g_L       AS glycemie_g_L,
    signaux_ok::poids_kg           AS poids_kg,
    signaux_ok::imc                AS imc,
    signaux_ok::activite_physique_steps   AS activite_physique_steps,
    signaux_ok::duree_sommeil_h    AS duree_sommeil_h,
    signaux_ok::score_douleur_0_10 AS score_douleur_0_10,
    signaux_ok::qualite_signal     AS qualite_signal,
    capteurs_ok::type_capteur      AS type_capteur,
    capteurs_ok::batterie_pct      AS batterie_pct,
    capteurs_ok::statut_capteur    AS statut_capteur;

-- ─────────────────────────────────────────
-- ÉTAPE 6 : JOINTURE avec patients
-- Ajouter les infos démographiques
-- ─────────────────────────────────────────

data_complet = JOIN signaux_proj BY patient_id, patients_clean BY patient_id;


-- ─────────────────────────────────────────
-- ÉTAPE 7 : GÉNÉRATION DES COLONNES DÉRIVÉES
-- Nouvelles colonnes pour préparer les KPIs
-- ─────────────────────────────────────────

data_final = FOREACH data_complet GENERATE 
    signaux_proj::mesure_id AS mesure_id,
    signaux_proj::patient_id AS patient_id,
    patients_clean::nom AS nom,
    patients_clean::age AS age,
    patients_clean::sexe AS sexe,
    patients_clean::ville AS ville,
    patients_clean::maladie_chronique AS maladie_chronique,
    signaux_proj::date AS date,
    signaux_proj::heure AS heure, 
    signaux_proj::temperature_C AS temperature_C,
    signaux_proj::tension_systolique_mmHg AS tension_systolique,
    signaux_proj::tension_diastolique_mmHg AS tension_diastolique,
    signaux_proj::frequence_cardiaque_bpm AS frequence_cardiaque,
    signaux_proj::spo2_pct AS spo2,
    signaux_proj::frequence_respiratoire_rpm AS frequence_respiratoire,
    signaux_proj::glycemie_g_L AS glycemie,
    signaux_proj::poids_kg AS poids,
    signaux_proj::imc AS imc,
    signaux_proj::activite_physique_steps AS steps,
    signaux_proj::duree_sommeil_h AS duree_sommeil,
    signaux_proj::score_douleur_0_10 AS score_douleur,
    signaux_proj::batterie_pct AS batterie_pct,
    signaux_proj::qualite_signal AS qualite_signal,
    signaux_proj::type_capteur AS type_capteur,
    (signaux_proj::tension_systolique_mmHg - signaux_proj::tension_diastolique_mmHg) AS pression_pulsee,
    ((signaux_proj::tension_systolique_mmHg >= 140 OR signaux_proj::tension_diastolique_mmHg >= 90) ? 1 : 0) AS flag_hypertension,
    ((signaux_proj::frequence_cardiaque_bpm > 100) ? 1 : 0) AS flag_tachycardie,
    ((signaux_proj::frequence_cardiaque_bpm < 60) ? 1 : 0) AS flag_bradycardie,
    ((signaux_proj::spo2_pct < 94.0) ? 1 : 0) AS flag_hypoxie,
    ((signaux_proj::spo2_pct < 90.0) ? 1 : 0) AS flag_hypoxie_critique,
    ((signaux_proj::temperature_C > 37.8) ? 1 : 0) AS flag_fievre,
    ((signaux_proj::glycemie_g_L < 0.70) ? 1 : 0) AS flag_hypoglycemie,
    ((signaux_proj::glycemie_g_L > 1.10) ? 1 : 0) AS flag_hyperglycemie,
    (float)((signaux_proj::spo2_pct - 90.0) / 10.0) AS indice_oxygenation,
    (float)( (signaux_proj::frequence_cardiaque_bpm - 75.0 >= 0 ? signaux_proj::frequence_cardiaque_bpm - 75.0 : 75.0 - signaux_proj::frequence_cardiaque_bpm) / 75.0) AS ecart_fc_normale,
    ((signaux_proj::batterie_pct < 20) ? 1 : 0) AS flag_batterie_critique;


STORE data_final INTO '/user/hadoop/output/data_final_patients' USING PigStorage(';');

