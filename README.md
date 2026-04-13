# Marketplace Data Platform — Groupe 4

## Membres
- Omomene IWELOMEN — Data Engineer (Airflow / DWH / orchestration)
- Farah Mehannek — Backend Developer (Flask API / génération de données)
- Abdelhamid MAAROUFI — Data Warehouse Engineer (SQL / modélisation)
- Abraham MVOGO — DevOps / Infrastructure (Docker / Airflow / MinIO / PostgreSQL)
- Hajar BEROUAG — BI / Analytics Engineer (Streamlit / KPI / visualisation)


---

## Objectif du projet

Ce projet simule une architecture moderne de data engineering basée sur un pipeline complet :

- Génération de données via une API Flask
- Ingestion dans un data lake (MinIO)
- Construction d’un Data Warehouse en PostgreSQL
- Calcul de KPI et détection d’anomalies
- Visualisation via Streamlit

L’objectif est de reproduire une architecture **Lakehouse simplifiée** avec orchestration Airflow.

---

## Stack technique

- Apache Airflow 3.2.0 (orchestration des pipelines)
- PostgreSQL 18 (Data Warehouse + Analytics)
- MinIO (Data Lake S3-compatible)
- Flask (API de génération de données)
- Streamlit (dashboard analytique)
- Python 3.11
- Docker & Docker Compose
- boto3 (interaction MinIO)
- psycopg2 (connexion PostgreSQL)

---

## Architecture globale

![alt text](<captures/mermaid-diagram .png>)

flowchart LR
```

API[Flask API - Orders] --> DAG1[Airflow DAG Ingestion]
DAG1 --> MINIO[(MinIO Bronze Layer)]

MINIO --> DAG2[Airflow DAG DWH Build]
DAG2 --> DWH[(PostgreSQL DWH)]

DWH --> DAG3[Analytics DAG KPI]
DAG3 --> ANALYTICS[(Analytics Schema)]

DWH --> DAG4[Anomaly Detection DAG]
DAG4 --> ANALYTICS

ANALYTICS --> STREAMLIT[Streamlit Dashboard]
````

---

## Structure du projet

```
.
├── api/
│   ├── app.py                 # API Flask de génération de commandes
│   └── Dockerfile
│
├── dags/
│   ├── marketplace_orders_ingest_daily.py   # API → MinIO
│   ├── marketplace_dwh_build_daily.py       # MinIO → DWH
│   ├── marketplace_analytics_aggregate_daily.py
│   ├── marketplace_anomaly_detect_daily.py
│   └── utils/
│       └── minio_client.py
│
├── init-db/
│   ├── dwh_tables.sql
│   ├── analytics_tables.sql
│   └── schema.sql
│
├── init-minio/
│   └── seed-csv.sh
│
├── streamlit/
│   ├── app.py
│   └── requirements.txt
│
├── tests/
│   ├── test_api_hook.py
│   ├── test_db.py
│   └── test_anomaly.py
│
├── docker-compose.yaml
├── .env
└── README.md
```

---

## DAGs livrés

| DAG                                   | Rôle                                    | Schedule |
| ------------------------------------- | --------------------------------------- | -------- |
| marketplace_orders_ingest_daily       | Ingestion API → MinIO (bronze layer)    | @daily   |
| marketplace_dwh_build_daily           | Construction du DWH (dimensions + fact) | @daily   |
| marketplace_analytics_aggregate_daily | Calcul des KPI journaliers              | @daily   |
| marketplace_anomaly_detect_daily      | Détection d’anomalies sur revenus       | @daily   |

---

## Lancement du projet

### 1. Lancer l’environnement

```bash
docker compose up -d
```

### 2. Attendre le démarrage complet

Environ 60 secondes.

### 3. Relance Airflow

```bash
docker compose restart airflow-scheduler airflow-worker airflow-apiserver airflow-dag-processor
```

### 4. Accéder aux interfaces

| Service             | URL                                            |
| ------------------- | ---------------------------------------------- |
| Airflow UI          | [http://localhost:8080](http://localhost:8080) |
| MinIO UI            | [http://localhost:9001](http://localhost:9001) |
| Streamlit Dashboard | [http://localhost:8501](http://localhost:8501) |

---

## Identifiants par défaut

### Airflow

* user: admin
* password: admin

### MinIO

* user: minioadmin
* password: minioadmin

---

## Tests

Les tests couvrent :

* connexion API / hook MinIO
* intégrité des données en base
* logique de détection d’anomalies

### Exécution des tests

```bash
docker compose exec airflow-worker pytest tests/ -v
```

---

## Choix techniques

### 1. Architecture Lake → Warehouse → Analytics

Nous avons implémenté une architecture en couches :

* Bronze : MinIO (données brutes)
* Silver/Gold : PostgreSQL DWH
* Analytics : tables agrégées KPI

---

### 2. Airflow comme orchestrateur central

Airflow permet :

* la gestion des dépendances entre pipelines
* la planification quotidienne
* la traçabilité des exécutions

---

### 3. Modélisation en étoile (Star Schema)

Le DWH repose sur :

* fact_orders
* dim_seller
* dim_customer
* dim_product

---

### 4. Idempotence des pipelines

Chaque DAG est conçu pour être relançable :

* constraints à la création des tables
* insertion contrôlée
* ON CONFLICT dans les dimensions
* suppression par date
---

### 5. Simulation de données temporelles

Les données sont générées avec variation temporelle contrôlée afin de simuler plusieurs jours sans activer Airflow catchup.

---

## Limitations connues

* Données entièrement synthétiques
* Donnéees sur une périod d'un seul jour
* Modèle d’anomalie basé sur une seule date
* Monitoring absent (Prometheus / Grafana non implémentés)

---

## Améliorations possibles

* Migration vers dbt pour transformations
* Introduction de Spark pour scalabilité
* Ajout de dates prolongés pour détection d’anomalies 
* Mise en place de CI/CD GitHub Actions
* Monitoring Airflow + Prometheus + Grafana

---

## Résultat final

![alt text](<captures/2026-04-13 06_46_56-.png>)
![alt text](<captures/2026-04-13 12_51_41-Greenshot.png>) ![alt text](<captures/2026-04-13 12_58_16-.png>) ![alt text](<captures/2026-04-13 12_58_43-.png>) ![alt text](<captures/2026-04-13 12_59_40-Greenshot.png>) ![alt text](<captures/2026-04-13 12_59_56-Greenshot.png>) ![alt text](<captures/2026-04-13 13_00_42-.png>) ![alt text](<captures/2026-04-13 13_01_00-.png>) ![alt text](<captures/2026-04-13 13_01_40-.png>) ![alt text](<captures/2026-04-13 13_01_52-Greenshot.png>) ![alt text](<captures/2026-04-13 13_02_30-.png>) ![alt text](<captures/2026-04-13 13_03_42-.png>) ![alt text](<captures/2026-04-13 13_07_15-Greenshot.png>) ![alt text](<captures/2026-04-13 13_07_36-Greenshot.png>) ![alt text](<captures/2026-04-13 13_15_11-Greenshot.png>) ![alt text](<captures/2026-04-13 13_18_30-Greenshot.png>) ![alt text](<captures/2026-04-13 13_19_00-Greenshot.png>) ![alt text](<captures/2026-04-13 14_35_26-SUJET _ Explorateur de fichiers.png>) ![alt text](<captures/2026-04-13 14_58_13-SUJET _ Explorateur de fichiers.png>)

Le projet permet de :

* simuler un pipeline de données complet
* construire un Data Warehouse fonctionnel
* calculer des KPI journaliers
* détecter des anomalies business
* visualiser les données via dashboard interactif





