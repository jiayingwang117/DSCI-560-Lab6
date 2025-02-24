# DSCI-560-Lab6
# 📄 **PDF Extraction Script (pdf_parse.py)**
The **pdf_parse.py** script extracts information from PDF files related to oil wells and stores the data into a **MySQL** database.

---
## ⚙️ **Prerequisites**
Before running the script, ensure the following requirements are met:

### 1. **Python Libraries**
Install required Python libraries:
```bash
pip install pdfplumber pymysql SQLAlchemy ocrmypdf pytesseract
```

### 2. **MySQL Database Setup**
- Ensure **MySQL** is installed and running.
- Create a database named:
```sql
CREATE DATABASE oil_well_db;
```
- Update the database connection credentials (host, username, password) in your local **.env**.

---
## 🏃 **Running the Script**
Execute the Python script:
```bash
python pdf_parse.py
```

### 4. **Verify Data in MySQL**
After the script runs successfully, check the database tables to ensure data has been populated:
```sql
USE oil_well_db;
SHOW TABLES;
SELECT * FROM well_info / stimulation_data LIMIT 10;
```

---


# 📝 **Data Preprocessing Script (data_preprocessing.py)**
The **data_preprocessing.py** script clean and format the data we get from **pdf.parse.py** and **web_scraper.py** with **BeautifulSoup** and **re**.

---

## 🏃 **Running the Script**  
Execute the Python script:
```bash
python data_preprocess.py
```

## 🔍 **Show Data in MySQL**  
After the script runs successfully, check the database tables to ensure the data has been cleaned and updated:  
```sql
USE lab6;
SHOW TABLES;
SELECT * FROM well_info_cleaned LIMIT 10;
SELECT * FROM stimulation_data_cleaned LIMIT 10;
SELECT * FROM well_scraped_data_cleaned LIMIT 10;
SELECT * FROM block_stats_data LIMIT 10;
SELECT * FROM proppant_details_data LIMIT 10
```

---


