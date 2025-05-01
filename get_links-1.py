import os
import re
import asyncio
import aiohttp
import json
import time
import random
from datetime import datetime
from dotenv import load_dotenv
from openai import AsyncOpenAI
from bs4 import BeautifulSoup
import tiktoken

# Load environment variables
load_dotenv()
BING_SUBSCRIPTION_KEY = os.getenv("BING_SEARCH_V7_SUBSCRIPTION_KEY")
BING_ENDPOINT = os.getenv("BING_SEARCH_V7_ENDPOINT").rstrip('/')
BING_SEARCH_URL = f"{BING_ENDPOINT}/v7.0/search"

def generate_industry_search_queries(industry, country):
    """Generates Bing search queries with year-wise loop and trusted sources."""
    years = list(range(2015, 2025))
    sources = [ 
        
        "Amazon", "Flipkart", "Walmart Connect", "Google Trends", "Facebook Ad Library", "Census Data", "USDA", "FDA", "World Bank", "10-K Filings", "Annual Reports", "Statista", "Crunchbase", "PitchBook", "Kickstarter", "SimilarWeb", "SEMrush", "Ahrefs", "App Annie", "Costco", "Tesco", "DMart"
    ]
        # "NielsenIQ", "Kantar Worldpanel", "Euromonitor International", "Mintel",

        # "Statista", "IBISWorld", "MarketResearch.com", "Research and Markets",
        # "Amazon", "Flipkart", "Walmart Connect", "Stackline", "Profitero", "DataWeave",
        # "Costco", "Tesco", "DMart", "Google Trends", "SEMrush", "Ahrefs", "Sprinklr",
        # "Brandwatch", "App Annie", "SimilarWeb", "Facebook Ad Library", "Annual Reports",
        # "10-K Filings", "Seeking Alpha", "FactSet", "Investor Presentations",
        # "Capital IQ", "Bloomberg", "Reuters", "Crunchbase", "PitchBook",
        # "Innova", "Mintel GNPD", "Amazon Launchpad", "Kickstarter", "TechNavio", "CB Insights",
        # "USDA", "FDA", "FSSAI", "National Bureau of Statistics", "Census Data",
        # "Trade Associations", "World Bank", "IMF", "McKinsey", "BCG", "Bain", "EY",
        # "Deloitte", "Accenture", "Capgemini", "AT Kearney", "Bain Net Promoter Benchmarks"
    
    
    queries = []
    for year in years:
        for source in sources:
            queries.append(f"{industry} {source} report {year} {country}")
            queries.append(f"{industry} {source} insights {year} {country}")
            queries.append(f"{industry} {source} sales performance {year} {country}")
            queries.append(f"{industry} {source} market trends {year} {country}")
            queries.append(f"{industry} {source} analysis {year} {country}")
    return queries

async def bing_search(session, query, count=1):
    """Search Bing using the Bing Search API and return a list of URLs."""
    headers = {"Ocp-Apim-Subscription-Key": BING_SUBSCRIPTION_KEY}
    params = {"q": query, "count": count, "mkt": "en-US"}
    
    try:
        async with session.get(BING_SEARCH_URL, headers=headers, params=params) as response:
            response.raise_for_status()
            search_results = await response.json()
            urls = []
            if "webPages" in search_results:
                urls = [result["url"] for result in search_results["webPages"]["value"]]
            print(f"Bing returned {len(urls)} URLs for '{query}'")
            return urls
    except Exception as e:
        print(f"Bing search error for '{query}': {e}")
        return []

async def get_links(topic, LOCATIONs, file_name, count=1):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\nStarting research: {topic} at {current_time}\n")
    
    all_urls = {}
    async with aiohttp.ClientSession() as session:
        for LOCATION in LOCATIONs:
            urls = []
            queries = generate_industry_search_queries(topic, LOCATION)
            print(f"Generated {len(queries)} queries for {LOCATION}")

            for query in queries:
                result = await bing_search(session, query, count)
                if result:
                    urls.extend(result)

                # 🛑 Add random small delay between requests to avoid Bing API rate limits
                # await asyncio.sleep(random.uniform(1.5, 3.0))  # Sleep between 1.5 and 3 seconds randomly

            all_urls[LOCATION] = list(set(urls))
            print(f"✅ {LOCATION}: Total unique URLs collected: {len(all_urls[LOCATION])}")

            # Save partial results after each LOCATION ✅
            with open(file_name, "w") as f:
                json.dump(all_urls, f, indent=4)
            print(f"📂 Saved progress after {LOCATION}\n")

if __name__ == "__main__":
    topic = "FMCG"
    LOCATIONs = ["Germany"]
    #  "Japan", "USA", "UK", "Singapore", "Dubai", "Switzerland", "China",
    #              "France",]  
    # "India"
    # "Italy", "Spain", "Brazil", "Canada", "Mexico", "Russia", "South Korea", "Australia"]
    file_name = "i4.json"
    asyncio.run(get_links(topic, LOCATIONs, file_name))
