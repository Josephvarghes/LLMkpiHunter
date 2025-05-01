import csv
import json
import openai
import os
from dotenv import load_dotenv

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
  "Source Link": "{source_url}",
  "Brand": "The brand or company mentioned in the insight, if any, else null",
  "Metric": "The key business metric mentioned (e.g., revenue, sales growth, profit margin)",
  "Metric Value": "Numeric value or estimate mentioned (e.g., ₹5000 Cr, $6.83B), else null(must ensure contain only numeric values)",
  "Currency": "INR, USD, or null if unspecified",
  "Country": "The country or region mentioned, else default to 'India'",
  "Year": "{year}(make sure it is cleaned)",
  "Time Span": "If the insight spans over multiple years, specify the range (e.g., 2016–2021), else null",
  "City Tier": "If city size/tier is mentioned (e.g., Tier 1, Tier 2), else null",
  "Growth Indicator": "Yes if growth or increase is mentioned, else No",
  "Growth Value (%)": "If growth is quantified (e.g., 10x = 900%), else null",
  
  
}}

Return only valid JSON. No extra text or markdown.

"""

def extract_structured_data(source_url, insight_text, year):
    try:
        print(f"[INFO] Extracting structured data for Insight: {insight_text[:50]}")
        prompt = build_prompt(source_url, insight_text, year)
        response = openai.ChatCompletion.create(
            model="gpt-4",
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
        fieldnames = ["Country", "Year", "Brand", "Metric", "Metric Value" ,"Currency", "Time Span","City Tier","Growth Indicator", "Growth Value (%)","Source Link"]
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
                    "Country": structured.get("Country", ""),
                    "Year": row["Year"],
                    "Brand": structured.get("Brand", ""),
                    "Metric": structured.get("Metric", ""),
                    "Metric Value": structured.get("Metric Value", ""),
                    "Currency": structured.get("Currency", ""),
                    "Time Span": structured.get("Time Span", ""),
                    "City Tier": structured.get("City Tier", ""),
                    "Growth Indicator": structured.get("Growth Indicator", ""),
                    "Growth Value (%)": structured.get("Growth Value (%)", ""),
                    "Source Link": row["Source URL"]
                })
                print(f"[INFO] Successfully processed row {row_count}.")
            else:
                print(f"[WARNING] Failed to process row {row_count}.")

    print("[INFO] CSV processing completed.")

if __name__ == "__main__":
    process_csv()
