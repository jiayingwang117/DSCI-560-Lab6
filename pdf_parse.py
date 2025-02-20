#!/usr/bin/env python3

import os
import re
import subprocess
import mysql.connector
import pdfplumber
from dotenv import load_dotenv
from datetime import datetime



# Load environment variables from .env file
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# ---------------------------------------------------------------------------
# 1. Database connection and table creation
# ---------------------------------------------------------------------------

def create_db_and_tables():
    """
    Create a MySQL database (if not exists) and tables needed for storing
    the oil well information and stimulation data.
    """
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    conn.commit()
    cursor.close()
    conn.close()

    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    cursor = conn.cursor()

    create_well_info_table = """
    CREATE TABLE IF NOT EXISTS well_info (
        id INT AUTO_INCREMENT PRIMARY KEY,
        operator VARCHAR(255),
        api_number VARCHAR(50),
        well_name VARCHAR(255),
        enseco_job_number VARCHAR(50),
        job_type VARCHAR(255),
        county_state VARCHAR(255),
        well_shl VARCHAR(255),
        latitude VARCHAR(50),
        longitude VARCHAR(50),
        datum VARCHAR(50)
    );
    """


    cursor.execute(create_well_info_table)

    create_stimulation_table = """
    CREATE TABLE IF NOT EXISTS stimulation_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        well_info_id INT,
        date_stimulated DATE,
        stimulated_formation VARCHAR(255),
        top_depth VARCHAR(50),
        bottom_depth VARCHAR(50),
        stimulation_stages INT,
        volume VARCHAR(50),
        volume_units VARCHAR(50),
        acid_percent VARCHAR(50),
        lbs_proppant VARCHAR(50),
        max_treatment_pressure VARCHAR(50),
        max_treatment_rate VARCHAR(50),
        proppant_details TEXT,
        FOREIGN KEY (well_info_id) REFERENCES well_info(id)
    )
    """


    cursor.execute(create_stimulation_table)

    conn.commit()
    cursor.close()
    conn.close()

# ---------------------------------------------------------------------------
# 2. OCR and PDF text extraction
# ---------------------------------------------------------------------------

def ocr_pdf_to_text(pdf_path, temp_ocr_pdf_path="temp_ocr_output.pdf"):
    """
    Use ocrmypdf to create a text-searchable PDF from a scanned PDF,
    then return extracted text via pdfplumber or PyPDF2.
    """
    # 1) Use ocrmypdf to produce an OCR'd PDF
    subprocess.run(["ocrmypdf", "--force-ocr", pdf_path, temp_ocr_pdf_path], check=True)

    # 2) Extract text from the newly created PDF
    text_content = ""
    with pdfplumber.open(temp_ocr_pdf_path) as pdf:
        for page in pdf.pages:
            text_content += page.extract_text() + "\n"

    # Remove the temporary OCR PDF if desired
    os.remove(temp_ocr_pdf_path)

    return text_content


def extract_text_from_pdf(pdf_path):
    """
    Attempt direct extraction with pdfplumber. If that fails or yields no text,
    fallback to OCR with ocrmypdf.
    """
    text_content = ""

    # Try pdfplumber on original PDF
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                extracted = page.extract_text()
                if extracted:
                    text_content += extracted + "\n"
    except Exception as e:
        print(f"[WARN] Could not open {pdf_path} with pdfplumber: {e}")

    # If no text found, assume scanned => do OCR
    if not text_content.strip():
        print(f"[INFO] No text found in {pdf_path}, running OCR...")
        text_content = ocr_pdf_to_text(pdf_path)

    return text_content

# ---------------------------------------------------------------------------
# 3. Parsing logic
# ---------------------------------------------------------------------------

def parse_well_info(text):
    """
    Parse the well info fields from the text by first isolating the section
    after "Well Information" or "WELL DATA SUMMARY", then using regex to extract:
    - operator
    - api_number
    - well_name
    - enseco_job_number
    - job_type
    - county_state
    - well_shl (Well Surface Hole Location)
    - latitude
    - longitude
    - datum

    Returns a dictionary with these fields.
    """
    # Isolate the section after "Well Information" or "WELL DATA SUMMARY"
    section_match = re.search(r"(?:Well Information|WELL DATA SUMMARY)(.*)", text, re.DOTALL | re.IGNORECASE)
    if section_match:
        section_text = section_match.group(1)
    else:
        section_text = text

    # Define regex patterns for each field.
    # operator_pattern       = r"Operator:\s*([^\n]+)"
    operator_pattern       = r"Operator:\s*(.+?)\s+API"
    api_pattern            = r"API\s*#:\s*([0-9\-]+)"
    well_name_pattern      = r"Well Name:\s*([^\n]+)"
    enseco_job_pattern     = r"Enseco\s*Job\s*#:\s*([^\n]+)"
    job_type_pattern       = r"Well\s*Type:\s*([^\n]+)"
    county_state_pattern   = r"County,\s*State:\s*([^\n]+)"
    shl_pattern            = r"Surface Location:\s*([^\n]+)"
    latitude_pattern       = r"Latitude:\s*([^\n]+)"
    longitude_pattern      = r"Longitude:\s*([^\n]+)"
    datum_pattern          = r"Datum:\s*([^\n]+)"

    # Initialize the dictionary with all fields set to None.
    well_data = {
        "operator": None,
        "api_number": None,
        "well_name": None,
        "enseco_job_number": None,
        "job_type": None,
        "county_state": None,
        "well_shl": None,
        "latitude": None,
        "longitude": None,
        "datum": None
    }

    # Helper function to search the section_text with a given pattern and set the value.
    def match_and_set(pattern, key):
        m = re.search(pattern, section_text, re.IGNORECASE)
        if m:
            well_data[key] = m.group(1).strip()

    match_and_set(operator_pattern, "operator")
    match_and_set(api_pattern, "api_number")
    match_and_set(well_name_pattern, "well_name")
    match_and_set(enseco_job_pattern, "enseco_job_number")
    match_and_set(job_type_pattern, "job_type")
    match_and_set(county_state_pattern, "county_state")
    match_and_set(shl_pattern, "well_shl")
    match_and_set(latitude_pattern, "latitude")
    match_and_set(longitude_pattern, "longitude")
    match_and_set(datum_pattern, "datum")

    return well_data

def parse_stimulation_data(text):
    """
    Parse stimulation data from W28654.pdf using a line-by-line approach.
    It captures the first block of stimulation data and the subsequent "Details" block.
    Returns a dictionary of stimulation data fields.
    """
    lines = text.splitlines()
    data = {
        "date_stimulated": None,
        "stimulated_formation": None,
        "type_treatment": None,
        "top_depth": None,
        "bottom_depth": None,
        "stimulation_stages": None,
        "volume": None,
        "volume_units": None,
        "acid_percent": None,
        "lbs_proppant": None,
        "max_treatment_pressure": None,
        "max_treatment_rate": None,
        "proppant_details": None,
    }

    for i, line in enumerate(lines):
        line_stripped = line.strip()
        # Capture the block with Date Stimulated, Formation, depths, stages, volume
        if "Date Stimulated" in line_stripped:
            if i+1 < len(lines):
                next_line = lines[i+1].strip()
                # Expected format: "06/09/2015 Three Forks Second Bench 11185 20754 50 I 126978 Barrels"
                m = re.match(
                    r"(\d{2}/\d{2}/\d{4})\s+(.*?)\s+(\d+)\s+(\d+)\s+(\d+)\s+I\s+(\d+)\s+(\w+)",
                    next_line
                )
                if m:
                    try:
                        raw_date = m.group(1).strip()
                        date_obj = datetime.strptime(raw_date, "%m/%d/%Y")
                        data["date_stimulated"] = date_obj.strftime("%Y-%m-%d")
                    except Exception as e:
                        data["date_stimulated"] = None
                    data["stimulated_formation"] = m.group(2).strip()
                    data["top_depth"] = m.group(3).strip()
                    data["bottom_depth"] = m.group(4).strip()
                    data["stimulation_stages"] = m.group(5).strip()
                    data["volume"] = m.group(6).strip()
                    data["volume_units"] = m.group(7).strip()
        # Capture Type Treatment block
        if "Type Treatment" in line_stripped:
            if i+1 < len(lines):
                next_line = lines[i+1].strip()
                # Expected format: "Sand Frac 4230380 9122 39.0"
                m = re.match(r"(.+?)\s+(\d+)\s+(\d+)\s+([\d.]+)", next_line)
                if m:
                    data["type_treatment"] = m.group(1).strip()
                    data["lbs_proppant"] = m.group(2).strip()
                    data["max_treatment_pressure"] = m.group(3).strip()
                    data["max_treatment_rate"] = m.group(4).strip()
        # Capture the Details block
        if "Details" in line_stripped:
            details_lines = []
            for j in range(i+1, len(lines)):
                l = lines[j].strip()
                # Stop capturing if we hit an empty line or a new header
                if l == "" or l.startswith("Date Stimulated") or l.startswith("Type Treatment"):
                    break
                details_lines.append(l)
            if details_lines:
                data["proppant_details"] = "\n".join(details_lines)
            # We capture only the first details block
            break

    return data



# ---------------------------------------------------------------------------
# 4. Insert data into the database
# ---------------------------------------------------------------------------

def insert_well_info(well_data, host="localhost", user="root", password="root", database="oil_well_data"):
    """
    Insert well info data into the well_info table.
    Return the inserted row's ID (well_info_id).
    """
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor()

    insert_sql = """
    INSERT INTO well_info (
        operator,
        api_number,
        well_name,
        enseco_job_number,
        job_type,
        county_state,
        well_shl,
        latitude,
        longitude,
        datum
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        well_data["operator"],
        well_data["api_number"],
        well_data["well_name"],
        well_data["enseco_job_number"],
        well_data["job_type"],
        well_data["county_state"],
        well_data["well_shl"],
        well_data["latitude"],
        well_data["longitude"],
        well_data["datum"]
    )

    cursor.execute(insert_sql, values)
    conn.commit()
    well_info_id = cursor.lastrowid

    cursor.close()
    conn.close()

    return well_info_id

def insert_stimulation_data(stim_data, well_info_id, host="localhost", user="root", password="root", database="oil_well_data"):
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor()
    insert_sql = """
    INSERT INTO stimulation_data (
        well_info_id,
        date_stimulated,
        stimulated_formation,
        top_depth,
        bottom_depth,
        stimulation_stages,
        volume,
        volume_units,
        acid_percent,
        lbs_proppant,
        max_treatment_pressure,
        max_treatment_rate,
        proppant_details
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        well_info_id,
        stim_data["date_stimulated"],
        stim_data["stimulated_formation"],
        stim_data["top_depth"],
        stim_data["bottom_depth"],
        stim_data["stimulation_stages"],
        stim_data["volume"],
        stim_data["volume_units"],
        stim_data["acid_percent"],
        stim_data["lbs_proppant"],
        stim_data["max_treatment_pressure"],
        stim_data["max_treatment_rate"],
        stim_data["proppant_details"]
    )
    cursor.execute(insert_sql, values)
    conn.commit()
    cursor.close()
    conn.close()


# ---------------------------------------------------------------------------
# 5. Main script
# ---------------------------------------------------------------------------

def main():
    # 1) Create DB and tables if needed
    create_db_and_tables()

    # 2) Path to folder containing the PDFs
    pdf_folder = "pdf_folder"

    # 3) Iterate over PDFs in the folder
    for filename in os.listdir(pdf_folder):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(pdf_folder, filename)
            print(f"Processing PDF: {pdf_path}")

            # Extract text (OCR if needed)
            text_content = extract_text_from_pdf(pdf_path)


            # # Save to a text file
            # output_filename = os.path.basename(pdf_path).replace(".pdf", "_extracted.txt")
            # output_path = os.path.join("extracted_texts", output_filename)

            # # Create folder if it doesn't exist
            # os.makedirs("extracted_texts", exist_ok=True)

            # with open(output_path, "w", encoding="utf-8") as f:
            #     f.write(text_content)

            # print(f"Extracted text saved to: {output_path}")


            # 4) Parse the well info
            well_info = parse_well_info(text_content)



            # (Optional) If you can confirm there's always a well_number in the PDF name,
            # you could parse it from `filename` or do more logic here.

            # 5) Insert well info into DB
            well_info_id = insert_well_info(well_info, host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
            print(f"Inserted well info with ID: {well_info_id}")

            # 6) Parse stimulation data
            stim_data = parse_stimulation_data(text_content)
            print(stim_data)

            # 7) Insert stimulation data
            if stim_data["date_stimulated"] or stim_data["stimulated_formation"]:
                # We only insert if we actually found some real data
                insert_stimulation_data(stim_data, well_info_id, host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
                print("Inserted stimulation data.")

    print("Done processing all PDFs.")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()