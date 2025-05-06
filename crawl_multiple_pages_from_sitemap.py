import asyncio
from typing import List
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
import requests
from xml.etree import ElementTree

async def crawl_sequential(urls: List[str]):
    print("\n=== Sequential Crawling with Session Reuse ===")

    browser_config = BrowserConfig(
        headless=True,
        extra_args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"],
    )

    crawl_config = CrawlerRunConfig()

    # Create the crawler (opens the browser)
    crawler = AsyncWebCrawler(config=browser_config)
    await crawler.start()

    try:
        session_id = "session1"  # Reuse the same session across all URLs
        for url in urls:
            result = await crawler.arun(
                url=url,
                config=crawl_config,
                session_id=session_id
            )
            if result.success:
                print(f"Successfully crawled: {url}")
                print(f"Markdown length: {len(result.markdown.raw_markdown)}")
            else:
                print(f"Failed: {url} - Error: {result.error_message}")
    finally:
        await crawler.close()

def get_sitemap_urls():
    sitemap_url = "https://ai.pydantic.dev/sitemap.xml"
    try:
        response = requests.get(sitemap_url)
        response.raise_for_status()

        # Parse the XML
        root = ElementTree.fromstring(response.content)
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = [loc.text for loc in root.findall('.//ns:loc', namespace)]

        return urls
    except Exception as e:
        print(f"Error fetching sitemap: {e}")
        return []

async def main():
    urls = get_sitemap_urls()
    if urls:
        print(f"Found {len(urls)} URLs to crawl")
        await crawl_sequential(urls)
    else:
        print("No URLs found to crawl")

if __name__ == "__main__":
    asyncio.run(main())
