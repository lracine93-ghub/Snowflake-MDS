🏗️ Project Architecture: Secure Sales Analytics ELT

This document outlines the design patterns and security controls implemented in the Sales Analytics pipeline. The project is a dual-discipline implementation of Cloud Data Engineering (ELT) and Infrastructure Cybersecurity.

# 1. Data Pipeline Architecture (ELT)
The project follows a modern ELT (Extract, Load, Transform) pattern to maximize the compute power of the Snowflake cloud data warehouse.

* **Extraction:** Python-based ingestion scripts extract data from source files.Loading: Data is loaded into a STAGING layer in its rawest form to ensure a "Source of Truth" is preserved.
* **Transformation:** All business logic (Revenue Growth, Ranking) is handled via SQL Views in the ANALYTICS layer, utilizing Snowflake's query optimizer for performance.
* **Availability:** Implemented "Graceful Degradation" logic; the frontend (Streamlit) automatically detects warehouse connectivity and fails over to a local CSV cache to ensure 100% uptime.

# 2. Cybersecurity & Governance Framework
Security is not an add-on; it is baked into the authentication and authorization layers of the pipeline. 

🔐 Identity & Access Management (IAM)

**Asymmetric Key-Pair Authentication:** Replaced standard password-based auth with RSA 2048-bit keys. This eliminates the risk of "password spraying" or credential leakage in logs.

**JWT Handshake:** The Python driver uses the private key to sign a JSON Web Token for Snowflake authentication, providing a secure, non-interactive login.
    
# 3. Performance & Cost OptimizationTo ensure the trial resources were used efficiently, the following Snowflake-native features were utilized:
* **Partition Pruning:** Optimized query patterns to ensure the TableScan operation ignores irrelevant micro-partitions, reducing IO costs.
* **Result Caching:** Leveraged Snowflake's 24-hour result cache to prevent redundant compute spend for repeat dashboard views.
* **Warehouse Monitoring:** Built an audit dashboard to monitor QUERY_HISTORY, tracking the credit-burn and execution time of the automated pipeline.

🏗️ Infrastructure as Code (IaC)

While traditionally associated with tools like Terraform or Pulumi, this project utilizes Declarative SQL Scripts to treat the Snowflake environment as code. This approach ensures that the entire data warehouse—from security roles to analytical views—can be recreated, version-controlled, and audited.

📜 Reproducible Environment Setup
The repository includes a centralized setup.sql (and associated DDL scripts) that act as the source of truth for the warehouse state. This eliminates "configuration drift" and allows for:
    * **Environment Parity:** The ability to spin up identical DEV, TEST, and PROD environments by executing the same script with different identifiers.
    * **Version Control:** Every change to a table schema or a security role is captured in Git, providing a complete history of the infrastructure's evolution.
    
    🧩 Declarative Object Management
    
     I utilize idempotent SQL patterns (e.g., CREATE OR REPLACE or IF NOT EXISTS) to define the following infrastructure components:

    |   Component       |   IaC Implementation          |   Benefit                   
    |   RBAC Hierarchy  |   CREATE ROLE, GRANT USAGE    |   Codifies the security perimeter; 
    |                   |                               |   ensures no manual "click-ops" in the UI.
    |   Compute Logic   |   CREATE WAREHOUSE            |   Defines the scaling and auto-suspend parameters for cost control.
    |   Storage Schema  |   CREATE SCHEMA, CREATE TABLE |   Documents the data contract and physical layout (Clustering, etc.).
    |   Logic Layer     |   CREATE VIEW                 |   Encapsulates ELT transformations into reusable, versioned code blocks.

🔄 The "Pipeline as Code" Philosophy
By coupling the Snowflake setup scripts with the Python-based Streamlit dashboard and ingestion logic, the entire stack—from the database engine to the end-user visualization—is defined in the codebase. This allows a new engineer to clone the repository and, with valid credentials, deploy the full analytics suite in minutes.

**Design Choice Note:** I opted for View-based Transformations rather than physical tables in the Analytics layer. This reduces storage costs and ensures that the dashboard always reflects the most recent data loaded into the Core layer.