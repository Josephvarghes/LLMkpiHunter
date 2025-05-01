# import asyncio
# import openai
# import json
# import csv
# import os
# from bs4 import BeautifulSoup
# from dotenv import load_dotenv
# from crawl4ai import AsyncWebCrawler

# # Load environment variables
# load_dotenv()
# openai.api_key = os.getenv("OPENAI_API_KEY")

# # Create or reset CSV file with headers
# with open("insights_output.csv", "w", newline='', encoding='utf-8') as csvfile:
#     writer = csv.writer(csvfile)
#     writer.writerow(["Source URL", "Category", "Insight"])

# # Load links from JSON file
# def load_links_from_json(filepath="i4.json"):
#     with open(filepath, "r") as f:
#         data = json.load(f)
#     all_links = []
#     for country, urls in data.items():
#         for url in urls:
#             all_links.append((country, url))
#     return all_links

# # Pre-clean HTML content
# def pre_clean_html(raw_html):
#     soup = BeautifulSoup(raw_html, "html.parser")
#     for tag in soup(["nav", "footer", "aside", "script", "style"]):
#         tag.decompose()
#     text = soup.get_text(separator="\n")
#     return "\n".join(line.strip() for line in text.splitlines() if line.strip())

# # Split long content into chunks
# def split_text(text, chunk_size=5000):
#     return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

# # Prompt for extracting insights
# def build_prompt(text):
#     return f"""
# You are an expert in FMCG performance analysis. Below is the content from a webpage related to FMCG industry insights.

# Your task is to extract only relevant insights based on the following performance categories:

# 1. Total Sales Performance
# 2. Channel-wise Performance
# 3. Promotions Impact
# 4. Customer Retention
# 5. Market Share & ASP
# 6. Innovation & Features
# 7. Demand & Inventory
# 8. Cost Optimization
# 9. Dealer Stock
# 10. Brand-wise Sales

# For each category, return only the most relevant quantitative or qualitative insights(must include numerical values), if found.

# Format your response in this exact JSON-like structure:

# {{
#   "Total Sales Performance": ["insight 1", "insight 2"],
#   "Channel-wise Performance": ["insight 1"],
#   "Promotions Impact": [],
#   ...
# }}

# Each list should only contain clear, actionable insights. Skip irrelevant or generic content (e.g., history, leadership quotes, etc.)

# Now analyze and extract from the following content:
# {text}
# """

# # Call OpenAI API to extract insights
# async def extract_insights_from_chunk(text_chunk):
#     prompt = build_prompt(text_chunk)
#     response = await openai.ChatCompletion.acreate(
#         model="gpt-4",
#         messages=[{"role": "user", "content": prompt}]
#     )
#     return response['choices'][0]['message']['content'].strip()

# # Save extracted insights to CSV and TXT
# async def save_insights(insights_json: str, url: str):
#     # Save raw to TXT
#     with open("insights_output.txt", "a", encoding='utf-8') as f:
#         f.write(f"\n[Source] {url}\n{insights_json}\n{'-'*80}\n")

#     # Parse and save to CSV
#     try:
#         data = json.loads(insights_json)
#         with open("insights_output.csv", "a", newline='', encoding='utf-8') as csvfile:
#             writer = csv.writer(csvfile)
#             for category, insights in data.items():
#                 for insight in insights:
#                     writer.writerow([url, category, insight])
#     except Exception as e:
#         print(f"[WARN] Could not parse insights JSON from {url}: {e}")

# # Main logic
# async def main():
#     links = load_links_from_json()
#     async with AsyncWebCrawler() as crawler:
#         for idx, (country, url) in enumerate(links):
#             try:
#                 print(f"[INFO] ({idx+1}/{len(links)}) Crawling: {url}")
#                 result = await crawler.arun(url=url)
#                 raw_text = result.markdown
#                 clean_text = pre_clean_html(raw_text)
#                 limited_text = clean_text[:15000]
#                 chunks = split_text(limited_text, chunk_size=5000)

#                 all_insights = {}
#                 for i, chunk in enumerate(chunks):
#                     print(f"  [INFO] Processing chunk {i+1}/{len(chunks)}")
#                     insights = await extract_insights_from_chunk(chunk)

#                     # Merge JSON results if multiple chunks
#                     try:
#                         chunk_data = json.loads(insights)
#                         for cat, val in chunk_data.items():
#                             if cat not in all_insights:
#                                 all_insights[cat] = []
#                             all_insights[cat].extend(val)
#                     except Exception as e:
#                         print(f"[ERROR] Parsing chunk failed: {e}")

#                 # Save all insights for this URL
#                 final_json = json.dumps(all_insights, indent=2)
#                 await save_insights(final_json, url)

#             except Exception as e:
#                 print(f"[ERROR] Failed for {url}: {e}")

# if __name__ == "__main__":
#     asyncio.run(main())
import asyncio
import openai
import json
import csv
import os
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from crawl4ai import AsyncWebCrawler

# Load environment variables
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# Create or reset CSV file with headers
with open("insights_output.csv", "w", newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Source URL", "Category", "Insight", "Year"])

# Load links from JSON file
def load_links_from_json(filepath="Germany_links_0_100.json"):
    with open(filepath, "r") as f:
        data = json.load(f)
    all_links = []
    for country, urls in data.items():
        for url in urls:
            all_links.append((country, url))
    return all_links

# Pre-clean HTML content
def pre_clean_html(raw_html):
    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup(["nav", "footer", "aside", "script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())

# Split long content into chunks
def split_text(text, chunk_size=5000):
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

# Prompt for extracting insights
def build_prompt(text):
    return f"""
You are an expert in FMCG performance analysis. Below is the content from a webpage related to FMCG industry insights.

Your task is to extract only relevant insights based on the following performance categories:

1. Total Sales Performance
2. Channel-wise Performance
3. Promotions Impact
4. Customer Retention
5. Market Share & ASP
6. Innovation & Features
7. Demand & Inventory
8. Cost Optimization
9. Dealer Stock
10. Brand-wise Sales

For each category, return only the most relevant quantitative or qualitative insights (must include numerical values) along with the year or time period mentioned (e.g., 2022, FY23, Q3 2024, etc.).

Format your response in this exact JSON-like structure:

{{
  "Total Sales Performance": [
    {{"insight": "Total revenue grew by 12%", "year": "Q3 FY24"}},
    ...
  ],
  "Channel-wise Performance": [
    {{"insight": "E-commerce share rose to 28%", "year": "2023"}}
  ],
  ...
}}

Each list should only contain clear, actionable insights. Skip irrelevant or generic content (e.g., history, leadership quotes, etc.).

Now analyze and extract from the following content:
{text}
"""

# Call OpenAI API to extract insights
async def extract_insights_from_chunk(text_chunk):
    prompt = build_prompt(text_chunk)
    response = await openai.ChatCompletion.acreate(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    return response['choices'][0]['message']['content'].strip()

# Save extracted insights to CSV and TXT
async def save_insights(insights_json: str, url: str):
    # Save raw to TXT
    with open("insights_output.txt", "a", encoding='utf-8') as f:
        f.write(f"\n[Source] {url}\n{insights_json}\n{'-'*80}\n")

    # Parse and save to CSV
    try:
        data = json.loads(insights_json)
        with open("insights_output.csv", "a", newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for category, insights in data.items():
                for item in insights:
                    if isinstance(item, dict):
                        insight_text = item.get("insight", "")
                        year = item.get("year", "")
                    else:
                        insight_text = item
                        year = ""
                    writer.writerow([url, category, insight_text, year])
    except Exception as e:
        print(f"[WARN] Could not parse insights JSON from {url}: {e}")

# Main logic
async def main():
    links = load_links_from_json()
    async with AsyncWebCrawler() as crawler:
        for idx, (country, url) in enumerate(links):
            try:
                print(f"[INFO] ({idx+1}/{len(links)}) Crawling: {url}")
                result = await crawler.arun(url=url)
                raw_text = result.markdown
                clean_text = pre_clean_html(raw_text)
                limited_text = clean_text[:15000]
                chunks = split_text(limited_text, chunk_size=5000)

                all_insights = {}
                for i, chunk in enumerate(chunks):
                    print(f"  [INFO] Processing chunk {i+1}/{len(chunks)}")
                    insights = await extract_insights_from_chunk(chunk)

                    # Merge JSON results if multiple chunks
                    try:
                        chunk_data = json.loads(insights)
                        for cat, val in chunk_data.items():
                            if cat not in all_insights:
                                all_insights[cat] = []
                            all_insights[cat].extend(val)
                    except Exception as e:
                        print(f"[ERROR] Parsing chunk failed: {e}")

                # Save all insights for this URL
                final_json = json.dumps(all_insights, indent=2)
                await save_insights(final_json, url)

            except Exception as e:
                print(f"[ERROR] Failed for {url}: {e}")

if __name__ == "__main__":
    asyncio.run(main())
