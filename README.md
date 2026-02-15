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


-- WorkFlow
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
                   SQL DQ Validation
                          |
                          v
                    Metadata Logging