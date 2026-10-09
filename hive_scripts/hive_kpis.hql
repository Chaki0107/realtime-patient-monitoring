-- ============================================================
-- SCRIPT HIVE : Tables, KPIs et Seuils
-- Projet : Surveillance patients âgés à domicile
-- ============================================================
-- Usage : hive -f /hive_scripts/hive_kpis.hql
-- ============================================================


-- ─────────────────────────────────────────
-- ÉTAPE 1 : CRÉER LA BASE DE DONNÉES
-- ─────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS surveillance;
USE surveillance;


-- ─────────────────────────────────────────
-- ÉTAPE 2 : TABLE PRINCIPALE
-- Charge l'output de Pig (toutes colonnes)
-- ─────────────────────────────────────────

DROP TABLE IF EXISTS mesures_patients;

CREATE EXTERNAL TABLE mesures_patients (
    mesure_id               STRING,
    patient_id              STRING,
    nom                     STRING,
    age                     INT,
    sexe                    STRING,
    ville                   STRING,
    maladie_chronique       STRING,
    date_mesure             STRING,
    heure_mesure            STRING,
    temperature_C           FLOAT,
    tension_systolique      INT,
    tension_diastolique     INT,
    frequence_cardiaque     INT,
    spo2                    FLOAT,
    frequence_respiratoire  INT,
    glycemie                FLOAT,
    poids                   FLOAT,
    imc                     FLOAT,
    steps                   INT,
    duree_sommeil           FLOAT,
    score_douleur           INT,
    batterie_pct            INT,
    qualite_signal          STRING,
    type_capteur            STRING,
    pression_pulsee         INT,
    flag_hypertension       INT,
    flag_tachycardie        INT,
    flag_bradycardie        INT,
    flag_hypoxie            INT,
    flag_hypoxie_critique   INT,
    flag_fievre             INT,
    flag_hypoglycemie       INT,
    flag_hyperglycemie      INT,
    indice_oxygenation      FLOAT,
    ecart_fc_normale        FLOAT,
    flag_batterie_critique  INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/hadoop/output/data_final_patients';

show tables;

-- ─────────────────────────────────────────
-- ÉTAPE 3 : TABLE DES SEUILS D'ALERTE
-- Seuils médicaux de référence par signal
-- ─────────────────────────────────────────

DROP TABLE IF EXISTS seuils_alerte;

CREATE TABLE seuils_alerte (
    signal              STRING,
    seuil_warning_min   FLOAT,
    seuil_warning_max   FLOAT,
    seuil_critique_min  FLOAT,
    seuil_critique_max  FLOAT,
    unite               STRING,
    description         STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO seuils_alerte VALUES
    ('frequence_cardiaque',  60,  100,   40,  130, 'bpm',  'Fréquence cardiaque'),
    ('spo2',                 94,  100,   90,  100, '%',    'Saturation oxygène'),
    ('temperature_C',        36,   38,   35, 39.5, '°C',   'Température corporelle'),
    ('tension_systolique',   90,  140,   80,  180, 'mmHg', 'Tension systolique'),
    ('tension_diastolique',  60,   90,   50,  110, 'mmHg', 'Tension diastolique'),
    ('glycemie',           0.70, 1.10, 0.50, 2.50, 'g/L',  'Glycémie'),
    ('frequence_respiratoire', 12, 20,   8,   30, 'rpm',  'Fréquence respiratoire');


-- ─────────────────────────────────────────
-- ÉTAPE 4 : KPI 1
-- Moyennes journalières par patient
-- FC, Fréquence Respiratoire, Glycémie
-- ─────────────────────────────────────────

DROP TABLE IF EXISTS kpi1_moyennes_vitaux;

CREATE TABLE kpi1_moyennes_vitaux AS
SELECT
    patient_id,
    nom,
    age,
    maladie_chronique,
    date_mesure,
    COUNT(*)                                AS nb_mesures,
    ROUND(AVG(frequence_cardiaque), 2)      AS moy_fc_bpm,
    ROUND(MIN(frequence_cardiaque), 2)      AS min_fc_bpm,
    ROUND(MAX(frequence_cardiaque), 2)      AS max_fc_bpm,
    ROUND(AVG(frequence_respiratoire), 2)   AS moy_fr_rpm,
    ROUND(MIN(frequence_respiratoire), 2)   AS min_fr_rpm,
    ROUND(MAX(frequence_respiratoire), 2)   AS max_fr_rpm,
    ROUND(AVG(glycemie), 3)                 AS moy_glycemie_gL,
    ROUND(MIN(glycemie), 3)                 AS min_glycemie_gL,
    ROUND(MAX(glycemie), 3)                 AS max_glycemie_gL,
    ROUND(AVG(temperature_C), 2)            AS moy_temperature_C,
    ROUND(AVG(spo2), 2)                     AS moy_spo2_pct,
    ROUND(AVG(score_douleur), 2)            AS moy_score_douleur
FROM mesures_patients
GROUP BY patient_id, nom, age, maladie_chronique, date_mesure
ORDER BY patient_id, date_mesure;


-- ─────────────────────────────────────────
-- ÉTAPE 5 : KPI 2
-- Taux d'hypertension par patient
-- (systolique >= 140 OU diastolique >= 90)
-- ─────────────────────────────────────────

DROP TABLE IF EXISTS kpi2_hypertension;

CREATE TABLE kpi2_hypertension AS
SELECT
    patient_id,
    nom,
    age,
    maladie_chronique,
    date_mesure,
    COUNT(*)                                            AS nb_mesures,
    ROUND(AVG(tension_systolique), 2)                  AS moy_systolique_mmHg,
    ROUND(AVG(tension_diastolique), 2)                 AS moy_diastolique_mmHg,
    ROUND(MAX(tension_systolique), 2)                  AS max_systolique_mmHg,
    ROUND(MAX(tension_diastolique), 2)                 AS max_diastolique_mmHg,
    SUM(flag_hypertension)                             AS nb_mesures_hta,
    ROUND(
        SUM(flag_hypertension) * 100.0 / COUNT(*), 2
    )                                                  AS taux_hypertension_pct,
    -- Classification OMS
    CASE
        WHEN AVG(tension_systolique) >= 180
          OR AVG(tension_diastolique) >= 110
        THEN 'HTA grade 3 (sévère)'
        WHEN AVG(tension_systolique) >= 160
          OR AVG(tension_diastolique) >= 100
        THEN 'HTA grade 2 (modérée)'
        WHEN AVG(tension_systolique) >= 140
          OR AVG(tension_diastolique) >= 90
        THEN 'HTA grade 1 (légère)'
        WHEN AVG(tension_systolique) >= 130
          OR AVG(tension_diastolique) >= 85
        THEN 'Pré-hypertension'
        ELSE 'Normale'
    END                                                AS classification_oms
FROM mesures_patients
GROUP BY patient_id, nom, age, maladie_chronique, date_mesure
ORDER BY taux_hypertension_pct DESC;


-- ─────────────────────────────────────────
-- ÉTAPE 6 : KPI 3
-- Score de stabilité cardiaque par patient
-- Basé sur : écart FC normale + flags
-- Score entre 0 (instable) et 100 (stable)
-- ─────────────────────────────────────────

DROP TABLE IF EXISTS kpi3_stabilite_cardiaque;

CREATE TABLE kpi3_stabilite_cardiaque AS
SELECT
    patient_id,
    nom,
    age,
    maladie_chronique,
    date_mesure,
    COUNT(*)                                         AS nb_mesures,
    ROUND(AVG(frequence_cardiaque), 2)               AS moy_fc,
    ROUND(STDDEV_POP(frequence_cardiaque), 2)        AS stddev_fc,
    SUM(flag_tachycardie)                            AS nb_tachycardie,
    SUM(flag_bradycardie)                            AS nb_bradycardie,
    ROUND(AVG(ecart_fc_normale), 4)                  AS moy_ecart_fc,
    -- Score stabilité : 100 - pénalités
    -- Pénalité tachycardie : -5 par épisode
    -- Pénalité bradycardie : -5 par épisode
    -- Pénalité écart moyen : -20 si écart > 0.3
    ROUND(
        GREATEST(0,
            100
            - (SUM(flag_tachycardie) * 5.0 / COUNT(*)) * 100
            - (SUM(flag_bradycardie) * 5.0 / COUNT(*)) * 100
            - CASE WHEN AVG(ecart_fc_normale) > 0.3 THEN 20 ELSE 0 END
        ), 2
    )                                                AS score_stabilite_cardiaque,
    CASE
        WHEN (SUM(flag_tachycardie) + SUM(flag_bradycardie)) * 1.0 / COUNT(*) > 0.5
        THEN 'Instable'
        WHEN (SUM(flag_tachycardie) + SUM(flag_bradycardie)) * 1.0 / COUNT(*) > 0.2
        THEN 'Attention'
        ELSE 'Stable'
    END                                              AS statut_cardiaque
FROM mesures_patients
GROUP BY patient_id, nom, age, maladie_chronique, date_mesure
ORDER BY score_stabilite_cardiaque ASC;


-- ─────────────────────────────────────────
-- ÉTAPE 7 : KPI 4
-- Indice d'oxygénation critique
-- Basé sur SpO2 + fréquence respiratoire
-- ─────────────────────────────────────────

DROP TABLE IF EXISTS kpi4_oxygenation;

CREATE TABLE kpi4_oxygenation AS
SELECT
    patient_id,
    nom,
    age,
    maladie_chronique,
    date_mesure,
    COUNT(*)                                         AS nb_mesures,
    ROUND(AVG(spo2), 2)                              AS moy_spo2_pct,
    ROUND(MIN(spo2), 2)                              AS min_spo2_pct,
    ROUND(AVG(frequence_respiratoire), 2)            AS moy_fr_rpm,
    SUM(flag_hypoxie)                                AS nb_episodes_hypoxie,
    SUM(flag_hypoxie_critique)                       AS nb_episodes_hypoxie_critique,
    ROUND(
        SUM(flag_hypoxie) * 100.0 / COUNT(*), 2
    )                                                AS taux_hypoxie_pct,
    -- Indice combiné SpO2 + FR
    -- SpO2 < 94 ET FR > 20 = détresse respiratoire probable
    SUM(CASE WHEN flag_hypoxie = 1
              AND frequence_respiratoire > 20
             THEN 1 ELSE 0 END)                      AS nb_detresse_respiratoire,
    -- Indice global : moyenne de indice_oxygenation
    -- 1.0 = parfait, 0.0 = seuil critique SpO2=90
    ROUND(AVG(indice_oxygenation), 4)                AS indice_oxygenation_moy,
    CASE
        WHEN AVG(spo2) < 90 OR SUM(flag_hypoxie_critique) > 0
        THEN 'CRITIQUE'
        WHEN AVG(spo2) < 94 OR SUM(flag_hypoxie) * 1.0 / COUNT(*) > 0.3
        THEN 'ATTENTION'
        ELSE 'NORMAL'
    END                                              AS niveau_oxygenation
FROM mesures_patients
GROUP BY patient_id, nom, age, maladie_chronique, date_mesure
ORDER BY indice_oxygenation_moy ASC;




-- ─────────────────────────────────────────
-- ÉTAPE 8 : VUE GLOBALE PATIENT / JOUR
-- Fusion de tous les KPIs en une seule vue
-- ─────────────────────────────────────────

DROP VIEW IF EXISTS vue_dashboard_patient;

CREATE VIEW vue_dashboard_patient AS
SELECT
    k1.patient_id,
    k1.nom,
    k1.age,
    k1.maladie_chronique,
    k1.date_mesure,
    k1.nb_mesures,
    -- KPI 1
    k1.moy_fc_bpm,
    k1.moy_fr_rpm,
    k1.moy_glycemie_gL,
    k1.moy_temperature_C,
    k1.moy_spo2_pct,
    -- KPI 2
    k2.moy_systolique_mmHg,
    k2.moy_diastolique_mmHg,
    k2.taux_hypertension_pct,
    k2.classification_oms,
    -- KPI 3
    k3.score_stabilite_cardiaque,
    k3.statut_cardiaque,
    k3.nb_tachycardie,
    k3.nb_bradycardie,
    -- KPI 4
    k4.indice_oxygenation_moy,
    k4.niveau_oxygenation,
    k4.nb_episodes_hypoxie,
    k4.nb_detresse_respiratoire
FROM kpi1_moyennes_vitaux k1
JOIN kpi2_hypertension     k2 ON k1.patient_id = k2.patient_id AND k1.date_mesure = k2.date_mesure
JOIN kpi3_stabilite_cardiaque k3 ON k1.patient_id = k3.patient_id AND k1.date_mesure = k3.date_mesure
JOIN kpi4_oxygenation      k4 ON k1.patient_id = k4.patient_id AND k1.date_mesure = k4.date_mesure;



DROP TABLE IF EXISTS table_patients;

CREATE EXTERNAL TABLE table_patients (
    patient_id STRING,
    nom STRING,
    age INT,
    sexe STRING,
    ville STRING,
    maladie_chronique STRING,
    medecin_referent STRING,
    numero_urgence STRING,
    date_inscription STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';' 
STORED AS TEXTFILE
LOCATION '/user/hadoop/input/patients' 
TBLPROPERTIES ("skip.header.line.count"="1"); 



DROP TABLE IF EXISTS centres_sante;

CREATE TABLE centres_sante (
    centre_id       STRING,
    nom_centre      STRING,
    ville           STRING,
    gouvernorat     STRING,
    type_centre     STRING,
    telephone       STRING,
    latitude        FLOAT,
    longitude       FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO centres_sante VALUES
    ('C001', 'Hôpital Charles Nicolle',        'Tunis',        'Tunis',        'CHU',          '71-578-000', 36.818, 10.181),
    ('C002', 'Hôpital La Rabta',               'Tunis',        'Tunis',        'CHU',          '71-562-600', 36.829, 10.171),
    ('C003', 'Clinique Taoufik',               'Tunis',        'Tunis',        'Clinique',     '71-890-100', 36.841, 10.195),
    ('C004', 'Hôpital Régional Ariana',        'Ariana',       'Ariana',       'Hôpital',      '71-717-000', 36.862, 10.194),
    ('C005', 'Clinique Hannibal',              'Ariana',       'Ariana',       'Clinique',     '71-780-500', 36.875, 10.202),
    ('C006', 'CHU Farhat Hached',              'Sousse',       'Sousse',       'CHU',          '73-241-311', 35.825, 10.638),
    ('C007', 'Clinique Les Oliviers',          'Sousse',       'Sousse',       'Clinique',     '73-226-500', 35.831, 10.645),
    ('C008', 'CHU Fattouma Bourguiba',         'Monastir',     'Monastir',     'CHU',          '73-460-144', 35.764, 10.812),
    ('C009', 'Hôpital Régional Nabeul',        'Nabeul',       'Nabeul',       'Hôpital',      '72-285-633', 36.456, 10.735),
    ('C010', 'Hôpital Régional Bizerte',       'Bizerte',      'Bizerte',      'Hôpital',      '72-431-422', 37.274, 9.873),
    ('C011', 'CHU Hédi Chaker',                'Sfax',         'Sfax',         'CHU',          '74-241-833', 34.740, 10.760),
    ('C012', 'Clinique La Rose Sfax',          'Sfax',         'Sfax',         'Clinique',     '74-297-000', 34.748, 10.771),
    ('C013', 'Hôpital Régional Kairouan',      'Kairouan',     'Kairouan',     'Hôpital',      '77-231-422', 35.678, 10.096),
    ('C014', 'Hôpital Régional Gabès',         'Gabès',        'Gabès',        'Hôpital',      '75-270-233', 33.881, 10.097),
    ('C015', 'Hôpital Régional Gafsa',         'Gafsa',        'Gafsa',        'Hôpital',      '76-221-600', 34.425, 8.784),
    ('C016', 'Hôpital Régional Médenine',      'Médenine',     'Médenine',     'Hôpital',      '75-640-311', 33.354, 10.505),
    ('C017', 'Hôpital Régional Jendouba',      'Jendouba',     'Jendouba',     'Hôpital',      '78-630-444', 36.501, 8.778),
    ('C018', 'Hôpital Régional Le Kef',        'Le Kef',       'Le Kef',       'Hôpital',      '78-202-533', 36.182, 8.714),
    ('C019', 'Hôpital Régional Siliana',       'Siliana',      'Siliana',      'Hôpital',      '78-870-311', 36.085, 9.370),
    ('C020', 'Hôpital Régional Zaghouan',      'Zaghouan',     'Zaghouan',     'Hôpital',      '72-676-022', 36.402, 10.143),
    ('C021', 'Hôpital Régional Ben Arous',     'Ben Arous',    'Ben Arous',    'Hôpital',      '71-380-711', 36.753, 10.228),
    ('C022', 'Clinique Ennasr',                'Ben Arous',    'Ben Arous',    'Clinique',     '71-382-500', 36.748, 10.235),
    ('C023', 'Hôpital Régional Manouba',       'Manouba',      'Manouba',      'Hôpital',      '71-603-444', 36.810, 10.097),
    ('C024', 'Hôpital Régional Kasserine',     'Kasserine',    'Kasserine',    'Hôpital',      '77-471-022', 35.167, 8.836),
    ('C025', 'Hôpital Régional Sidi Bouzid',   'Sidi Bouzid',  'Sidi Bouzid',  'Hôpital',      '76-630-711', 35.038, 9.485),
    ('C026', 'Hôpital Régional Tozeur',        'Tozeur',       'Tozeur',       'Hôpital',      '76-452-600', 33.919, 8.134),
    ('C027', 'Hôpital Régional Kébili',        'Kébili',       'Kébili',       'Hôpital',      '75-491-311', 33.705, 8.965),
    ('C028', 'Hôpital Régional Tataouine',     'Tataouine',    'Tataouine',    'Hôpital',      '75-860-244', 32.921, 10.451);

-- ─────────────────────────────────────────
-- VÉRIFICATIONS
-- ─────────────────────────────────────────

SELECT 'mesures_patients'       AS table_name, COUNT(*) AS nb_lignes FROM mesures_patients
UNION ALL
SELECT 'kpi1_moyennes_vitaux'   AS table_name, COUNT(*) AS nb_lignes FROM kpi1_moyennes_vitaux
UNION ALL
SELECT 'kpi2_hypertension'      AS table_name, COUNT(*) AS nb_lignes FROM kpi2_hypertension
UNION ALL
SELECT 'kpi3_stabilite_cardiaque' AS table_name, COUNT(*) AS nb_lignes FROM kpi3_stabilite_cardiaque
UNION ALL
SELECT 'kpi4_oxygenation'       AS table_name, COUNT(*) AS nb_lignes FROM kpi4_oxygenation;
