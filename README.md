# Databricks Assignment

This repository contains the implementation of a Databricks data engineering assignment involving ingestion, transformation, and storage of structured CSV data and API-based JSON data in Delta format using PySpark.

---

## Question 1 – CSV Pipeline: Employee Data

### Objective
Build a structured ETL pipeline across three layers (source to bronze, bronze to silver, and silver to gold) for three CSV datasets: Employee, Department, and Country.

---

### Folder Structure

- **source_to_bronze/**
  - `utils`: Common utility functions (e.g., column conversion, load date).
  - `employee_source_to_bronze`: Reads CSVs and writes raw data to DBFS.

- **bronze_to_silver/**
  - `employee_bronze_to_silver`: Reads raw files using schema, applies transformations, and writes Delta table.

- **silver_to_gold/**
  - `employee_silver_to_gold`: Performs aggregations and joins, and writes the final gold data.

---

### Steps Overview

1. **source_to_bronze**
   - Read 3 CSVs (`Employee`, `Department`, `Country`) using schema.
   - Use utility notebook for common functions.
   - Write as CSV to DBFS under `/source_to_bronze/`.

2. **bronze_to_silver**
   - Read from `/source_to_bronze/` using custom schema and different read techniques.
   - Convert camelCase to snake_case.
   - Add `load_date` column.
   - Write as Delta table:  
     - Database: `employee_info`  
     - Table: `dim_employee`  
     - Path: `/silver/employee_info/dim_employee`

3. **silver_to_gold**
   - Read silver Delta table.
   - Perform the following:
     - Salary by department (desc).
     - Count of employees per department per country.
     - Join department and country names.
     - Avg age per department.
   - Add `at_load_date`.
   - Write to Delta with overwrite and replaceWhere:
     - Path: `/gold/employee/fact_employee`

---

## Question 2 – API Pipeline: Reqres User Info

### Objective
Fetch paginated API data from `https://reqres.in/api/users` and store as a Delta table.

---

### Steps

1. Use a loop to fetch data across all pages until no data is returned.
2. Apply a custom schema to flatten and clean the JSON structure.
3. Drop metadata fields (`page`, `per_page`, `total`, `total_pages`, and `support`).
4. Derive `site_address` column with value `"reqres.in"` from email.
5. Add `load_date` column.
6. Write the final dataframe as a Delta table:
   - Database: `site_info`
   - Table: `person_info`
   - Path: `/site_info/person_info`
   - Mode: `overwrite`
