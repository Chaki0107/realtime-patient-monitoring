-- ============================================================
-- SCRIPT HIVE : Table alertes
-- Projet : Surveillance patients âgés à domicile
-- ============================================================
-- Cette table reçoit toutes les alertes générées par Spark.
-- Elle sert d'historique persistant (même après redémarrage).
-- Spark peut y écrire via foreachBatch en plus d'émettre
-- vers Flask/WebSocket.
-- ============================================================

USE surveillance;

-- ─────────────────────────────────────────
-- TABLE ALERTES
-- ─────────────────────────────────────────

DROP TABLE IF EXISTS alertes;

CREATE TABLE alertes (
    alerte_id           STRING,       -- ex : RT_00042_frequence_cardiaque
    patient_id          STRING,       -- identifiant du patient
    mesure_id           STRING,       -- identifiant de la mesure source
    timestamp_alerte    STRING,       -- horodatage de la mesure déclenchante
    date_alerte         STRING,       -- date (YYYY-MM-DD)
    heure_alerte        STRING,       -- heure (HH:MM:SS)
    signal              STRING,       -- signal concerné (ex: frequence_cardiaque)
    valeur_mesuree      FLOAT,        -- valeur reçue de la smartwatch
    unite               STRING,       -- unité du signal (bpm, %, mmHg, g/L…)
    seuil_warning_min   FLOAT,        -- seuil bas niveau warning
    seuil_warning_max   FLOAT,        -- seuil haut niveau warning
    niveau_alerte       STRING,       -- CRITIQUE ou WARNING
    source              STRING        -- smartwatch, capteur, simulation…
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/output/alertes';

-- ─────────────────────────────────────────
-- VUE : Résumé alertes par patient / jour
-- ─────────────────────────────────────────

DROP VIEW IF EXISTS vue_alertes_resume;

CREATE VIEW vue_alertes_resume AS
SELECT
    patient_id,
    date_alerte,
    COUNT(*)                                        AS total_alertes,
    SUM(CASE WHEN niveau_alerte = 'CRITIQUE' THEN 1 ELSE 0 END) AS nb_critiques,
    SUM(CASE WHEN niveau_alerte = 'WARNING'  THEN 1 ELSE 0 END) AS nb_warnings,
    -- Signal le plus souvent en alerte
    -- (utilise une sous-requête pour avoir le mode)
    COUNT(DISTINCT signal)                          AS nb_signaux_alertes,
    MIN(heure_alerte)                               AS premiere_alerte,
    MAX(heure_alerte)                               AS derniere_alerte,
    -- Niveau global du patient ce jour-là
    CASE
        WHEN SUM(CASE WHEN niveau_alerte = 'CRITIQUE' THEN 1 ELSE 0 END) > 0
        THEN 'CRITIQUE'
        WHEN SUM(CASE WHEN niveau_alerte = 'WARNING'  THEN 1 ELSE 0 END) > 0
        THEN 'WARNING'
        ELSE 'NORMAL'
    END                                             AS niveau_global
FROM alertes
GROUP BY patient_id, date_alerte
ORDER BY date_alerte DESC, nb_critiques DESC;

-- ─────────────────────────────────────────
-- VUE : Alertes par signal (pour stats)
-- ─────────────────────────────────────────

DROP VIEW IF EXISTS vue_alertes_par_signal;

CREATE VIEW vue_alertes_par_signal AS
SELECT
    signal,
    niveau_alerte,
    COUNT(*)                    AS nb_occurrences,
    COUNT(DISTINCT patient_id)  AS nb_patients_touches,
    ROUND(AVG(valeur_mesuree), 2) AS valeur_moyenne,
    ROUND(MIN(valeur_mesuree), 2) AS valeur_min,
    ROUND(MAX(valeur_mesuree), 2) AS valeur_max,
    unite
FROM alertes
GROUP BY signal, niveau_alerte, unite
ORDER BY nb_occurrences DESC;

-- ─────────────────────────────────────────
-- VÉRIFICATION
-- ─────────────────────────────────────────

DESCRIBE alertes;

SELECT 'Table alertes créée avec succès' AS statut;
