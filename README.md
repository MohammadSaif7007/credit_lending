# Credit Lending ETL Project

This project implements a **config-driven ETL pipeline** for ingesting, validating, transforming, and curating banking data, including clients, credits, collaterals, and market data. 
The pipeline supports multiple layers: **parsed → refined → curated** and is orchestrated using **Airflow**, with transformations implemented in **PySpark**.

---

## **Project Structure**
```
credit_lending_platform/
│
├── framework/                          # Generic reusable ETL framework
│   ├── config_loader.py
│   ├── spark_runner.py
│   ├── dq_runner.py
│   ├── metadata_logger.py
│   ├── base_transforms.py
│   └── utils.py
│
├── data/                               # Medallion storage
│   ├── source/
│   ├── parsed/
│   ├── refined/
│   └── curated/
│
├── pipelines/
│   └── credit_lending/
│       ├── parsed/
│       │   ├── python_helper.py
│       │   ├── transactions/
│       │   │   ├── config.yaml
│       │   │   └── validation.sql.j2
│       │   ├── customers/
│       │   │   ├── config.yaml
│       │   │   └── validation.sql.j2
│       │   └── tests/
│       │       └── test_parsed_helpers.py
│       │
│       ├── refined/
│       │   ├── python_helper.py
│       │   ├── transactions/
│       │   │   ├── config.yaml
│       │   │   └── validation.sql.j2
│       │   └── tests/
│       │       └── test_refined_helpers.py
│       │
│       └── curated/
│           ├── python_helper.py
│           ├── credit_portfolio/
│           │   ├── transformation.py
│           │   ├── config.yaml
│           │   └── validation.sql.j2
│           └── tests/
│               └── test_credit_portfolio.py
│
├── dags/
│   └── credit_lending_dag/
│       └── credit_lending_dag.py
│
├── docker/
│   ├── spark/Dockerfile
│   └── airflow/Dockerfile
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

**WorkFlow**
```
                +-------------------+
                |     Airflow       |
                |  (Orchestration)  |
                +---------+---------+
                          |
                          v
               +----------+----------+
               |   Spark Container   |
               | (Transform Engine)  |
               +----------+----------+
                          |
                          v
      SOURCE → PARSED → REFINED → CURATED
                                   |
                                   v
                                 SQL DQ 
                                 Validation(In All 3 layers)
                                   |
                                   v
                                Loading
```
---

## **Project Objective**

- Build a **config-driven ETL pipeline** for banking data (clients, credits, collaterals, market).  
- Ensure **data quality and validation** across layers.  
- Enable **curated datasets** for dashboards, risk assessment, and ML models.  
- Support **scalable, reusable, and automated workflows** for analytics and business insights.  

---

## **Solution Outcomes**

**Outcome:** A robust, **config-driven ETL solution** leveraging PySpark and Airflow to automate ingestion, transformation, validation, and curation of multi-source banking data.  

**Benefits:** Provides a **scalable and reusable pipeline**, ensures **data quality and consistency**, enables unified datasets for dashboards and analytics, and supports **advanced business insights** such as client risk assessment, portfolio monitoring, and ML-driven recommendations.  

---

## **Data Collection and Ingestion**

- Ingest client, credit, collateral, and market data from multiple internal and external sources.  
- Implement a **config-driven ingestion pipeline** using PySpark and Airflow for automated data loading.  
- Ensure reliable and scalable ingestion with **layered storage paths** for parsed, refined, and curated data.  

---

## **Data Transformation and Cleansing**

- Apply **config-driven transformations** on parsed data to cast types, map values, and add ingestion timestamps.  
- Implement **data cleansing functions** to remove nulls, unmapped values, and invalid entries across all tables.  
- Generate **refined and standardized datasets** ready for curated-level aggregation and downstream analytics.  

---

## **Processing and Enrichment**

- Perform **curated-level transformations** to aggregate, join, and enrich client, credit, collateral, and market data.  
- Calculate **business metrics** such as total loan amounts, average payments, credit scores, and default flags.  
- Generate **ready-to-use curated datasets** for downstream analytics, dashboards, and ML models.  

---

## **Analysis & Reporting**

- Provide **dashboards and reports** on client payments, delayed loans, and portfolio performance.  
- Enable **risk assessment** and insights using aggregated metrics from curated datasets.  
- Support **advanced analytics and ML models** for client segmentation, default prediction, and product recommendations.  

---

## **Spin Up Locally Using Docker**

1. **Clone the repository**
git clone <repository_url>
cd credit-lending-etl

**Build the Docker image** 
docker build -t credit_lending_etl:latest .

**Run Docker container** 
docker-compose up -d

**Start Airflow locally** 
# Initialize Airflow DB
airflow db init

# Start scheduler
airflow scheduler &

# Start webserver
airflow webserver -p 8080

# Access DAGs
Open browser: http://localhost:8080 → Trigger credit_lending_dag.


# Adding New Transformations
## **Parsed Layer**

- Add transformation function in pipelines/parsed/python_helper.py.
- Update the respective config.yaml to include the new step.
- Add validations if needed in config.yaml.

## **Refined Layer**

- Add function in pipelines/refined/python_helper.py.
- Update the config.yaml of the refined table with the new step.

## **Curated Layer**

- Create a new transformation.py in the curated table folder.
- Update the table's config.yaml with input, output, retain_columns, and validations.
- Update dependencies.json if the curated table depends on other tables.

## **Airflow DAG Updates**

- Add your new table to layers_tables in airflow/dags/credit_lending_dag.py.
- Define its dependencies in dependencies.json.
- DAG automatically creates TaskGroups and sets up upstream/downstream dependencies.

## **Unit Testing**

- Use pytest and Spark fixture defined in framework/conftest.py.

## **Run all tests:**
- pytest pipelines/parsed/tests
- pytest pipelines/refined/tests
- pytest pipelines/curated/tests

# Summary

Automated ingestion from multiple sources
Layered transformations and validations (parsed → refined → curated)
Reusable transformations via configuration files
Scalable Airflow DAG orchestration for dependencies and validations
Clean, unified datasets for analytics and ML models

# Data Flow
![alt text](assets/image.png)

# Orcestration Flow
![alt text](assets/image-1.png)
