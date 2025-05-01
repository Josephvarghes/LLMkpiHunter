import os
import json
import csv
import time
import requests
from dotenv import load_dotenv

# Load API key
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro-latest:generateContent?key={API_KEY}"

# File paths
INPUT_CSV = r"C:\Users\user\OneDrive\Desktop\Crawl4AI\usefull_data\failed_rows.csv"
OUTPUT_FOLDER = "cleaned_category"
OUTPUT_CSV = os.path.join(OUTPUT_FOLDER, "brand_sales_structured.csv")
FAILED_CSV = os.path.join(OUTPUT_FOLDER, "failed_rows.csv")

# Ensure output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Prompt template
def build_prompt(source_url, insight, year):
    return f"""
You are an expert FMCG analyst. Given the following raw insight text:

Source URL: "{source_url}"
Insight: "{insight}"
Year: {year}

Extract and format the structured data in this exact JSON format:

{{
  "Brand": "The brand name mentioned (if any)",
  "Metric": "What is being measured (e.g., turnover, sales, growth)",
  "Value": "Mentioned value or milestone (if any)",
  "Country": "Mentioned region or country, or 'India' if implied, else null",
  "Year": "{year}",
  "Summary": "A clean one-line summary of the insight"
}}

Return only valid JSON. No extra text or markdown.
"""

# Gemini API caller with retry
def extract_with_gemini(source_url, insight, year, retries=2, delay=5):
    prompt = build_prompt(source_url, insight, year)
    payload = { "contents": [ { "parts": [ { "text": prompt } ] } ] }
    headers = { "Content-Type": "application/json" }
    
    # Adding a delay before making the request to avoid hitting rate limits
    time.sleep(delay)

    for attempt in range(retries + 1):
        try:
            response = requests.post(GEMINI_URL, headers=headers, json=payload)
            response.raise_for_status()
            output = response.json()
            text_response = output["candidates"][0]["content"]["parts"][0]["text"] 
            
            # Clean the Gemini response if it's wrapped in markdown
            text_response = text_response.strip()

            # Remove markdown code block if present
            if text_response.startswith("```json"):
                text_response = text_response.replace("```json", "").strip()
            if text_response.endswith("```"):
                text_response = text_response[:-3].strip()

            return json.loads(text_response)

        except json.JSONDecodeError as e:
            print(f"[ERROR] JSON parsing failed:\n{e}\nRaw response: {text_response}")
            break
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] API call failed (Attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                break
    return None

# Main CSV processor
def process_csv():
    print("[INFO] Starting CSV processing...")

    with open(INPUT_CSV, mode='r', encoding='utf-8') as infile, \
         open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as outfile, \
         open(FAILED_CSV, mode='w', newline='', encoding='utf-8') as failedfile:

        reader = csv.DictReader(infile)
        fieldnames = ["Source URL", "Insight", "Year", "Brand", "Metric", "Value", "Country", "Summary"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        failed_writer = csv.DictWriter(failedfile, fieldnames=reader.fieldnames)

        writer.writeheader()
        failed_writer.writeheader()

        for idx, row in enumerate(reader, start=1):
            print(f"[INFO] Processing row {idx}...")
            source_url = row.get("Source URL", "")
            structured = extract_with_gemini(source_url, row["Insight"], row["Year"])
            
            if structured:
                writer.writerow({
                    "Source URL": source_url,
                    "Insight": row["Insight"],
                    "Year": row["Year"],
                    "Brand": structured.get("Brand", "null"),
                    "Metric": structured.get("Metric", "null"),
                    "Value": structured.get("Value", "null"),
                    "Country": structured.get("Country", ""),
                    "Summary": structured.get("Summary", "")
                })
                print(f"[INFO] ✔ Row {idx} processed successfully.")
            else:
                failed_writer.writerow(row)
                print(f"[WARNING] ✖ Row {idx} failed. Saved to failed_rows.csv.")

            time.sleep(2)  # Avoid hitting rate limits

# Run the processor
if __name__ == "__main__":
    process_csv()
