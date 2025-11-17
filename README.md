# **Azure Data Engineering Project**

### ** Overview**  
This project demonstrates a full **Azure Data Engineering pipeline** using a modern **Medallion Architecture (Bronze → Silver → Gold)**.  
It showcases **incremental ingestion**, **streaming transformations**, **CDC/SCD processing**, **DLT pipelines**, and **production deployment** using **Databricks Asset Bundles**.

---

# **Architecture**
SQL Database → ADF → ADLS Bronze → Databricks Silver → DLT Gold → LakeFlow Jobs 

# **Technologies Used**

| Layer | Tools / Services |
|------|------------------|
| **Ingestion** | Azure Data Factory, Watermark Incremental Loading |
| **Storage** | Azure Data Lake Storage Gen2 (ADLS) |
| **Compute** | Azure Databricks (PySpark, Delta Lake, AutoLoader) |
| **Metadata & Security** | Unity Catalog, Storage Credentials, External Locations |
| **Transformations** | PySpark, reusable transformation functions |
| **Gold Layer** | Delta Live Tables (DLT), SCD Type 2, CDC |
| **Orchestration** | Databricks LakeFlow Jobs |
| **Deployment** | Databricks Asset Bundles (DAB), GitHub |
| **Visualization** | Power BI (yet to do) |


# **Layers Implemented**

### **🟤 Bronze Layer**
- Raw ingestion from SQL Server using ADF  
- Watermark column for incremental loads  
- Data stored in ADLS (Parquet)

### **⚪ Silver Layer**
- Cleaned and transformed datasets using PySpark  
- Implemented:
  - Schema evolution  
  - AutoLoader for incremental micro-batch processing  
  - Reusable transformation functions (`dropColumns`, etc.)  
- Data written as **Delta tables**

### **🟡 Gold Layer**
- Built using **Delta Live Tables (DLT)**  
- Implemented:
  - CDC (Change Data Capture)  
  - SCD Type 2 for history tracking  
  - Data quality expectations with `@dlt.expect_all_or_drop`  
  - Lineage tracking automatically handled by DLT  

---
## Demo Video

[![Watch the demo](https://img.youtube.com/vi/KLdV9Xpsp9o/0.jpg)](https://youtu.be/KLdV9Xpsp9o)

