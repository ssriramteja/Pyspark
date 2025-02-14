# 📌 Hive Table Metadata Extractor for 3000+ tables with Excel Input

## 🔹 Overview
This **PySpark script** extracts metadata from **Hive tables**, reads table names from an **Excel file**, and saves the results in both **CSV** and **Excel formats**. The processed files are stored **locally** and uploaded to **HDFS** for further access.

---

## 🔹 What This Script Does
✔ **Loads table names** from an Excel file (`tables_input.xlsx`).  
✔ **Extracts metadata** for each Hive table, including:
   - 📍 **Table Location**
   - 👤 **Owner**
   - ⏳ **Creation Time**
   - 🕒 **Last Access Time**
   - 🔢 **Row Count**
✔ **Processes tables in parallel** for fast execution.  
✔ **Saves the results** as:
   - 📄 **CSV** (`hive_metadata.csv`)
   - 📊 **Excel** (`hive_metadata.xlsx`)
✔ **Uploads output to HDFS** for easy access.  
✔ **Handles missing or non-existent tables gracefully.**  

---

## 🔹 Technologies Used
- 🐍 **Python**
- 🔥 **PySpark**
- 📊 **Pandas**
- 🗄 **HDFS**
- 💻 **Hue Portal**
- ⚡ **ThreadPoolExecutor** (for parallel processing)
- 📑 **OpenPyXL** (for Excel handling)

---
