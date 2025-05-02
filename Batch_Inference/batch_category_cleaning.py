import csv
import json
import openai
import os
from dotenv import load_dotenv 
from manual_clean import clean_csv

# Load environment variables
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

INPUT_CSV = "filtered_exports"
OUTPUT_CSV = "cleaned_category"

def build_prompt(source_url, insight, year):
    print(f"[INFO] Building prompt for Source URL: {source_url}, Insight: {insight[:50]}, Year: {year}")
    return f"""
You are an expert FMCG analyst. Given the following raw insight text:

Source URL: "{source_url}"
Insight: "{insight}"
Year: "{year}"

Your task is to extract and return structured data in the following exact JSON format. Generate a concise summary based on the Insight.

{{
  "Source URL": "{source_url}",
  "Insight": "{insight}",
  "Summary": "A concise, clear summary generated from the Insight",
  "Year": "{year} (strictly cleaned, e.g., '2023' or 'FY23')",
  "Brand": "Brand name mentioned (if any)",
  "Metric": "What is being measured (e.g., sales revenue, market share, inventory level — cleaned)",
  "Metric Category": "The broader KPI category this metric falls under (e.g., Sales Performance, Market Share)",
  "Value": "Mentioned value (strictly numeric or numeric + % if applicable)",
  "Unit": "Unit of measurement (e.g., USD, INR, EUR, '%', 'units', 'tons', 'liters')",
  "Country": "Mentioned country/region, or 'India' if implied, else null"
}}

❗Return only valid JSON. No markdown, no explanation, no headings — only JSON.
"""

def extract_structured_data(source_url, insight_text, year):
    try:
        print(f"[INFO] Extracting structured data for Insight: {insight_text[:50]}")
        prompt = build_prompt(source_url, insight_text, year)
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        reply = response['choices'][0]['message']['content'].strip()
        return json.loads(reply)
    except openai.error.OpenAIError as e:
        print(f"[ERROR] OpenAI API Error for insight: {insight_text[:50]}... | Error: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error for insight: {insight_text[:50]}... | Error: {e}")
    return None

def process_csv():
    print("[INFO] Starting CSV processing...")

    # ## Updated to process all CSV files in a folder and save each separately ##
    for filename in os.listdir(INPUT_CSV):  # INPUT_CSV is now a folder path
        if filename.endswith(".csv"):
            input_file_path = os.path.join(INPUT_CSV, filename)
            output_file_path = os.path.join(OUTPUT_CSV, filename)  # Save with same name

            print(f"[INFO] Processing file: {filename}")

            with open(input_file_path, mode='r', encoding='utf-8') as infile, \
                 open(output_file_path, mode='w', newline='', encoding='utf-8') as outfile:

                reader = csv.DictReader(infile)
                fieldnames = ["Source URL", "Insight", "Summary", "Year", "Brand", "Metric", "Metric Category", "Value","Unit","Country"]
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()

                row_count = 0
                for row in reader:
                    row_count += 1
                    print(f"[INFO] Processing row {row_count} in {filename}...")

                    source_url = row.get("Source URL", "")
                    structured = extract_structured_data(source_url, row["Insight"], row["Year"])

                    if structured:
                        writer.writerow({
                            "Source URL": structured.get("Source URL", ""),
                            "Insight": structured.get("Insight", ""),
                            "Summary": structured.get("Summary", ""),
                            "Year": structured.get("Year", ""),
                            "Brand": structured.get("Brand", ""),
                            "Metric": structured.get("Metric", ""),
                            "Metric Category": structured.get("Metric Category", ""),
                            "Value": structured.get("Value", ""),
                            "Unit": structured.get("Unit", ""),
                            "Country": structured.get("Country", "")
                        })
                        print(f"[INFO] Successfully processed row {row_count} in {filename}.")
                    else:
                        print(f"[WARNING] Failed to process row {row_count} in {filename}.")

    print("[INFO] CSV processing completed.")


if __name__ == "__main__":
    process_csv()

    # ## Clean each output CSV file individually ##
    for filename in os.listdir(OUTPUT_CSV):
        if filename.endswith(".csv"):
            output_file_path = os.path.join(OUTPUT_CSV, filename)
            print(f"[INFO] Cleaning file: {filename}")
            clean_csv(output_file_path)