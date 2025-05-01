# # from crawl4ai import Crawler
# # from crawl4ai.extractors import LinkExtractor
# # from crawl4ai.filters import DomainFilter, KeywordFilter
# # from crawl4ai.strategies import GoogleSearchStrategy  # More specific search engine
# # from crawl4ai.models import ProxyConfig  # New proxy handling

# # Initialize crawler with modern config
# crawler = Crawler(
#     strategy=GoogleSearchStrategy(
#         num_results=20,  # Results per page
#         lang="en",  # Default language
#         geo=None  # Auto-detect from proxy
#     ),
#     api_key="YOUR_API_KEY",
#     headless_mode="dynamic",  # Better anti-detection
#     proxy_config=ProxyConfig(
#         rotation_interval=5  # Rotate proxies every 5 requests
#     )
# )

# # Modern proxy format with authentication
# proxies = {
#     "us": "socks5://user:pass@proxy-us.example.com:1080",
#     "in": "http://user:pass@proxy-in.example.com:3128",
#     "br": "http://user:pass@proxy-br.example.com:8080",
#     "de": "http://user:pass@proxy-de.example.com:8888"
# }

# # Enhanced country-specific queries with local SEO keywords
# queries = {
#     "us": "site:.com FMCG companies USA",
#     "in": "site:.in FMCG brands India",
#     "br": "site:.br marcas FMCG Brasil",
#     "de": "site:.de FMCG Unternehmen Deutschland"
# }

# for country, query in queries.items():
#     print(f"🚀 Processing {country}...")
    
#     # Configure proxy with modern rotation
#     crawler.strategy.proxy = proxies[country]
#     crawler.strategy.geo = country.upper()  # Set geolocation
    
#     # Execute crawl with advanced parameters
#     result = crawler.run(
#         input=query,
#         extractor=LinkExtractor(
#             include_patterns=[
#                 rf".*\.{country}$",  # Country-specific TLDs
#                 r"/(about|products|contact)/"  # Relevant pages
#             ],
#             exclude_anchors=True
#         ),
#         filters=[
#             KeywordFilter(
#                 required=["FMCG", "fast-moving"],
#                 exclude=["job", "career", "advert"]
#             ),
#             DomainFilter(
#                 include=[f".{country}", "linkedin.com", "crunchbase.com"],
#                 exclude=["amazon", "alibaba"]
#             )
#         ],
#         navigation_params={
#             "wait_until": "networkidle2",  # Modern page load check
#             "timeout": 30000  # 30 seconds timeout
#         },
#         max_pages=3  # Search result pages to crawl
#     )
    
#     # Advanced cleaning with URL normalization
#     clean_links = list({
#         link.split('?')[0].rstrip('/')  # Remove params and trailing slashes
#         for link in result.links
#         if not any(blocked in link.lower() for blocked in ["tracking", "analytic"])
#     })
    
# #     # Save with metadata
# #     with open(f"fmcg_{country}_links_{datetime.now().strftime('%Y%m%d')}.txt", "w", encoding="utf-8") as f:
# #         f.write(f"# {country.upper()} FMCG Links\n")
# #         f.write(f"# Generated: {datetime.now()}\n")
# #         f.write("\n".join(sorted(clean_links)))
    
# #     print(f"✅ {country}: {len(clean_links)} quality links saved")

# # print("🎉 FMCG link harvesting complete!")