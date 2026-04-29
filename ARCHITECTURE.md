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

## 🕵️ Threat Model & Risk Mitigation
This project was designed to defend against common cloud data infrastructure attacks. By shifting security "left" into the architecture, we mitigate several high-criticality risks.

### 1. Identity & Access Threats

Threat                      | Mitigation Strategy   |   How it works
--------------------------------------------------------------------------------------------------------------------------------
Credential Theft / Phishing | RSA Key-Pair Auth     |   Even if an attacker discovers the ACCOUNT_NAME and USER_NAME, there is 
                            |                       |   no password to steal. Access requires the encrypted private key file,
                            |                       |   which is never stored in the database or transmitted over the wire.
----------------------------|-----------------------|-----------------------------------------------------------------------
Credential Leaks (GitHub)   | Asymmetric Handshake  |   Since the pipeline uses JWTs signed locally, no "secrets" are ever
                            |                       |    committed to code. The public key is an open identifier; the private key
                            |                       |     stays in the local environment's secure vault.
----------------------------|--------------------------------------------------------------------------------------------------   
Blast Radius Escalation     |   Functional RBAC     |   If the INGESTOR_ROLE is somehow compromised, the attacker is "jailed"
                            |                       |    within the Staging schema. They cannot delete the Analytics views,  
                            |                       |    access billing data, or create new users.
----------------------------|-----------------------|---------------------------------------------------------------------------



### 2. Data Integrity & Injection 

Threat                      | Mitigation Strategy   |   How it works
--------------------------------------------------------------------------------------------------------------------------------
SQL Injection               | Parameterized Queries |   The Streamlit-to-Snowflake connection utilizes Parameterized Bindings
                            |                       |   (%s). User inputs (like Category filters) are never concatenated
                            |                       |   directly into SQL strings, neutralizing the risk of malicious code
                            |                       |   injection.
----------------------------|-----------------------|-----------------------------------------------------------------------
Unauthorized Data           | Read-Only Logic Layer |   By using SQL Views for the Analytics layer, we ensure that the "Source of
Modification                |                       |    Truth" in the Core tables cannot be modified by the dashboard 
                            |                       |     role. Analysts have a "window" into the data, not a "handle" on it
----------------------------|--------------------------------------------------------------------------------------------------   
Insider Threat (PII Access) | Masking-Ready Schema  |   Although this project uses public sales data, the architecture supports 
                            |                       |   Dynamic Data Masking. This ensures that even high-privileged developers 
                            |                       |   can be restricted from seeing raw PII unless their role explicitly 
                            |                       |   requires it for a "Right to Know" task.
----------------------------|-----------------------|---------------------------------------------------------------------------

### 3. Infrastructure Threats
ThreatMitigation StrategyHow it works
"Shadow" Data Access    Centralized Audit Logging   
**Design Choice Note:** I opted for View-based Transformations rather than physical tables in the Analytics layer. This reduces storage costs and ensures that the dashboard always reflects the most recent data loaded into the Core layer.

Threat                      | Mitigation Strategy   |   How it works
--------------------------------------------------------------------------------------------------------------------------------
"Shadow" Data Access        | Centralized Audit     | Every query executed by the Python pipeline is logged in Snowflake's    
                            | Logging               | QUERY_HISTORY. Any deviation from normal behavior (e.g., a sudden spike 
                            |                       | in data volume or unauthorized schema scans) is immediately visible on the 
                            |                       | Audit Dashboard.
 ---------------------------|-----------------------|-----------------------------------------------------------------------
Configuration Drift         | Infrastructure as     | Because the environment is defined via SQL scripts, any "manual" changes 
                            | Code (IAC)            | made in the UI are easily detectable. The code acts as the      
                                                    | Immutable Source of Truth for the warehouse state.
----------------------------|--------------------------------------------------------------------------------------------------   

