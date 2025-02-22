# Data Warehouse and Data Analytics Project 2025

Welcome to the Data Warehouse and Analytics Project 2025 repository! 🚀

This project provides a complete data warehousing and analytics solution, covering the construction of a data warehouse and the extraction of actionable insights. Designed as a portfolio project, it emphasizes industry best practices in data engineering and analytics.

---

## 🧬 Data Architecture
The data architecture for this project follows Medallion Architecture which consists of three layers; Bronze, Silver, and Gold:

![image](https://github.com/user-attachments/assets/17ce0e79-6ff9-4824-a073-0d2ee3b8f612)

- **Bronze Layer:** Captures raw data directly from source systems, ingesting CSV files into a SQL Server database.

- **Silver Layer:** Performs data cleansing, standardization, and normalization to prepare data for analysis.

- **Gold Layer:** Contains business-ready data structured into a star schema for reporting and analytics.

---

## 🚀 Project Requirements

### Data Warehouse Development (Data Engineering)

**Objective:**

Construct a contemporary data warehouse utilizing SQL Server to centralize sales information, facilitating analytical reporting and informed business decisions.

**Specifications:**

- **Data Sources:**
Incorporate data from two primary systems—ERP and CRM—provided in CSV format.

- **Data Quality:**
Perform data cleansing to address and rectify any quality issues before analysis.

- **Integration:**
Merge data from both sources into a cohesive, user-friendly model optimized for analytical queries.

- **Scope:**
Concentrate solely on the most recent dataset; historical data archiving is not necessary.

- **Documentation:**
Develop comprehensive documentation of the data model to assist business stakeholders and analytics teams.
---
### Business Intelligence: Analytics & Reporting (Data Analysis)

**Objective:**
Create SQL-based analytical solutions to provide in-depth insights into:

- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights will equip stakeholders with essential business metrics, supporting strategic decision-making.

For additional information, please refer to docs/requirements.md.

---

## 🛡️ License
This project is licensed under the MIT License. You are free to use, modify, and share this project with proper attribution.

