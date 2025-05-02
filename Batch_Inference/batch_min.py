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
CSV_FILE = "insights_output.csv"
TXT_FILE = "insights_output.txt"
CHECKPOINT_FILE = "checkpoint.json"

with open(CSV_FILE, "w", newline='', encoding='utf-8') as csvfile:
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


# Save extracted insights to CSV and TXT
async def save_insights(insights_json: str, url: str):
    # Save raw to TXT
    with open(TXT_FILE, "a", encoding='utf-8') as f:
        f.write(f"\n[Source] {url}\n{insights_json}\n{'-'*80}\n")

    # Parse and save to CSV
    try:
        data = json.loads(insights_json)
        with open(CSV_FILE, "a", newline='', encoding='utf-8') as csvfile:
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


# === RETRY LOGIC ===
import random
import time

def retry_with_backoff(max_retries=5, base_delay=1, max_delay=60):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            retries = 0
            delay = base_delay
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if "limit" in str(e).lower() or "timeout" in str(e).lower() or "server error" in str(e).lower():
                        print(f"[RETRY] {e}. Retrying in {delay}s (attempt {retries + 1}/{max_retries})...")
                        await asyncio.sleep(delay)
                        retries += 1
                        delay = min(delay * 2 + random.uniform(0, 1), max_delay)
                    else:
                        print(f"[FATAL] {e}")
                        break
            return None  # Return None if all retries fail
        return wrapper
    return decorator


@retry_with_backoff(max_retries=5, base_delay=1, max_delay=60)
async def extract_insights_from_chunk(url, chunk_index, text_chunk):
    prompt = build_prompt(text_chunk)
    try:
        response = await openai.ChatCompletion.acreate(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            timeout=30
        )
        return {
            "url": url,
            "chunk_index": chunk_index,
            "insights": response['choices'][0]['message']['content'].strip()
        }
    except Exception as e:
        print(f"[ERROR] Failed chunk: {url}, chunk {chunk_index} - {e}")
        return {
            "url": url,
            "chunk_index": chunk_index,
            "insights": ""
        }


# Load checkpoint
def load_checkpoint(filepath=CHECKPOINT_FILE):
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return set(json.load(f))
    return set()


# Save checkpoint
def save_checkpoint(processed_chunks, filepath=CHECKPOINT_FILE):
    with open(filepath, "w") as f:
        json.dump(list(processed_chunks), f, indent=2)


# Main logic
async def main():
    links = load_links_from_json()
    all_chunks_with_url = []

    BATCH_SIZE = 5  # Adjust based on your OpenAI rate limits

    # Step 1: Collect all chunks across URLs
    async with AsyncWebCrawler() as crawler:
        for idx, (country, url) in enumerate(links):
            try:
                print(f"[INFO] ({idx+1}/{len(links)}) Crawling: {url}")
                result = await crawler.arun(url=url)
                raw_text = result.markdown
                clean_text = pre_clean_html(raw_text)
                limited_text = clean_text[:15000]
                chunks = split_text(limited_text, chunk_size=5000)

                for i, chunk in enumerate(chunks):
                    all_chunks_with_url.append((url, i, chunk))

            except Exception as e:
                print(f"[ERROR] Failed crawling {url}: {e}")

    # Step 2: Load already processed chunks
    processed_chunks = load_checkpoint()

    # Step 3: Filter out already processed
    filtered_chunks = [
        (url, idx, chunk) 
        for url, idx, chunk in all_chunks_with_url 
        if f"{url}||{idx}" not in processed_chunks
    ]
    print(f"\n[RESUME] Found {len(all_chunks_with_url) - len(filtered_chunks)} / {len(all_chunks_with_url)} already processed.\n")

    # Step 4: Process in batches
    for i in range(0, len(filtered_chunks), BATCH_SIZE):
        batch = filtered_chunks[i:i+BATCH_SIZE]
        print(f"\n[INFO] Processing batch {i//BATCH_SIZE + 1} of {len(filtered_chunks)//BATCH_SIZE + 1}...\n")

        tasks = [
            extract_insights_from_chunk(url, idx, chunk)
            for url, idx, chunk in batch
        ]

        results = await asyncio.gather(*tasks)

        for result in results:
            if result and result["insights"]:
                await save_insights(result["insights"], result["url"])
                processed_chunks.add(f"{result['url']}||{result['chunk_index']}")

        save_checkpoint(processed_chunks)

    print("\n✅ All processing complete!")


if __name__ == "__main__":
    asyncio.run(main())