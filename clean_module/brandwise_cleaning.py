import csv
import json
import openai
import os
from dotenv import load_dotenv 
from manual_clean import clean_csv

# Load environment variables
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

INPUT_CSV = r"C:\Users\user\OneDrive\Desktop\Crawl4AI\filtered_exports\Total_Sales_Performance.csv"
OUTPUT_CSV = "../cleaned_category/Total_Sales_Performance.csv"

def build_prompt(source_url, insight, year):
    print(f"[INFO] Building prompt for Source URL: {source_url}, Insight: {insight[:50]}, Year: {year}")
    return f"""
You are an expert FMCG analyst. Given the following raw insight text:

Source URL: "{source_url}"
Insight: "{insight}"
Year: {year}

Extract and format the structured data in this exact JSON format:

{{
  "Brand": "The brand name mentioned (if any)",
  "Metric": "What is being measured (e.g., turnover, sales, growth, strictly cleaned)",
  "Value": "Mentioned value or milestone (if any,strictly cleaned, must be numeric)",
  "Country": "Mentioned region or country, or 'India' if implied, else null",
  "Year": "{year}(strictly clenaned(eg: 2023))",
  "Source URLl": "{source_url}",
}}

Return only valid JSON. No extra text or markdown.
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
    with open(INPUT_CSV, mode='r', encoding='utf-8') as infile, \
         open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = ["Country", "Year", "Brand", "Metric", "Value", "Source URL"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        row_count = 0
        for row in reader:
            row_count += 1
            print(f"[INFO] Processing row {row_count}...")

            source_url = row.get("Source URL", "")
            structured = extract_structured_data(source_url, row["Insight"], row["Year"])
            
            if structured:
                writer.writerow({
                    "Source URL": source_url,
                    "Year": row["Year"],
                    "Brand": structured.get("Brand", ""),
                    "Metric": structured.get("Metric", ""),
                    "Value": structured.get("Value", ""),
                    "Country": structured.get("Country", ""),

                })
                print(f"[INFO] Successfully processed row {row_count}.")
            else:
                print(f"[WARNING] Failed to process row {row_count}.")

    print("[INFO] CSV processing completed.")

if __name__ == "__main__":
    process_csv() 
    clean_csv(OUTPUT_CSV)
