# Healthcare Delta Live Table Pipeline with Medallion Architecture 


### **Project Overview with the Dataset**:
The project involves creating a **Delta Live Table (DLT) pipeline** for processing healthcare data using **Databricks** and **Delta Lake**. The goal is to transform the diagnosis data, validate its quality, and make it analytics-ready following the **Medallion Architecture**. This pipeline helps organize raw data into structured insights for analysis or reporting.

---

### **Dataset Overview**:
The dataset provided contains diagnosis information:
- **Columns**:
  - `diagnosis_code`: A unique identifier for each diagnosis (e.g., D001 for Hypertension).
  - `diagnosis_description`: A textual description of the diagnosis (e.g., Hypertension, Diabetes Mellitus).
- **Use Case**:
  - Transform and store diagnosis data to track trends in healthcare diagnoses.
  - Enrich the dataset with additional information in later stages, such as patient data or treatment plans.

---

### **Medallion Architecture Applied**:

1. **Bronze Layer (Raw Data)**:
   - **Goal**: Ingest the raw diagnosis dataset into a Delta table without modifications.
   - **Steps**:
     - Load the CSV file into a Delta table (`diagnosis_bronze`).
     - Preserve raw data for traceability.
   - Example Code (DLT):
     ```python
     import dlt

     @dlt.table
     def diagnosis_bronze():
         return spark.read.format("csv").option("header", "true").load("/mnt/data/patients_daily_file_1_2024.csv")
     ```

2. **Silver Layer (Cleaned and Transformed Data)**:
   - **Goal**: Clean and standardize the data for better usability.
   - **Steps**:
     - Validate the `diagnosis_code` format (e.g., starts with "D" followed by three digits).
     - Standardize the text in `diagnosis_description` (e.g., trim whitespace, ensure consistent capitalization).
     - Handle missing or invalid data (e.g., quarantine invalid records).
   - Example Code (DLT):
     ```python
     @dlt.table
     def diagnosis_silver():
         raw_data = dlt.read("diagnosis_bronze")
         return (
             raw_data
             .filter("diagnosis_code RLIKE '^D[0-9]{3}$'")
             .withColumn("diagnosis_description", F.initcap(F.trim(F.col("diagnosis_description"))))
         )
     ```

3. **Gold Layer (Analytics-Ready Data)**:
   - **Goal**: Aggregate or transform the diagnosis data for analytics or reporting.
   - **Steps**:
     - Group diagnoses to calculate the frequency of each diagnosis type.
     - Enrich the data by joining it with other healthcare datasets (e.g., patient demographics).
   - Example Code (DLT):
     ```python
     @dlt.table
     def diagnosis_gold():
         silver_data = dlt.read("diagnosis_silver")
         return silver_data.groupBy("diagnosis_code", "diagnosis_description").count()
     ```

---

### **Data Pipeline Workflow**:

1. **Data Ingestion**:
   - Ingest the raw diagnosis dataset from the uploaded file into the **Bronze layer**.

2. **Data Validation and Cleaning**:
   - Ensure `diagnosis_code` matches the expected format (e.g., `D###`).
   - Clean up text fields like `diagnosis_description` to make them consistent.

3. **Data Transformation and Aggregation**:
   - Add calculated columns (e.g., diagnosis counts).
   - Enrich with other datasets (e.g., patient data for tracking which patients have specific diagnoses).

4. **Analytics**:
   - Use the **Gold layer** to analyze diagnosis trends, such as:
     - Most common diagnoses.
     - Diagnosis frequency over time.

---

### **Outcome**:
- **Bronze Layer**: Raw data stored exactly as ingested for traceability.
- **Silver Layer**: Cleaned, validated, and standardized data, ready for further processing.
- **Gold Layer**: Aggregated and analytics-ready data for business intelligence or machine learning applications.

