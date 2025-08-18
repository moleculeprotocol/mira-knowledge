import asyncio
import json
import time

from crawl4ai import AsyncUrlSeeder, AsyncWebCrawler, CrawlerRunConfig, SeedingConfig
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy


async def main():
    seeder = AsyncUrlSeeder()
    seeding_config = SeedingConfig(
        source="sitemap", extract_head=True, pattern="*/blog/*"
    )

    print("🔍 Discovering blog URLs from sitemap...")
    urls = await seeder.urls("https://www.molecule.to", seeding_config)

    # Keep the full seeder results to preserve title information
    url_data = {}  # Map URL to seeder data
    url_list = []
    for url_info in urls:
        url = url_info["url"]
        url_list.append(url)
        url_data[url] = url_info  # Store the full seeder data

    print(f"📚 Found {len(url_list)} blog URLs from seeder")

    # URL exclusion patterns - add more patterns as needed
    url_exclusion_patterns = [
        "gamifying-longevity-increasing-participation-accelerating-breakthroughs",
        "vitalik-buterin-on-decentralized-science-aging-ai-and-scientific-progress",
        "exploring-decentralized-science-with-balaji-srinivasan-on-the-desci-podcast",
        "desci-berlin-2025-bigger-busier-bolder",
        "desci-berlin-2023",
        "desci-berlin-2024-recap",
        "desci-berlin",
    ]

    filtered_url_list = []
    excluded_urls = []

    for url in url_list:
        should_exclude = False
        for pattern in url_exclusion_patterns:
            if pattern in url:
                should_exclude = True
                excluded_urls.append(url)
                break

        if not should_exclude:
            filtered_url_list.append(url)

    url_list = filtered_url_list

    print(f"🔍 Excluded {len(excluded_urls)} URLs based on exclusion patterns")
    if excluded_urls:
        for url in excluded_urls[:3]:  # Show first 3 excluded URLs
            print(f"  ❌ {url}")
        if len(excluded_urls) > 3:
            print(f"  ... and {len(excluded_urls) - 3} more excluded")

    print(f"✅ {len(url_list)} URLs remaining to crawl")
    for url in url_list[:5]:  # Show first 5 URLs
        print(f"  → {url}")
    if len(url_list) > 5:
        print(f"  ... and {len(url_list) - 5} more")

    crawler_config = CrawlerRunConfig(
        scraping_strategy=LXMLWebScrapingStrategy(),
        css_selector=".read-content",  # Common blog content selectors
        verbose=True,
    )

    async with AsyncWebCrawler() as crawler:
        start_time = time.perf_counter()
        print(f"\n🚀 Starting async crawl of {len(url_list)} blog pages...")

        # Use arun_many for concurrent crawling
        results = await crawler.arun_many(urls=url_list, config=crawler_config)

        print(f"✅ Crawled {len(results)} blog pages")

        # Extract page data for JSON export
        pages_data = []
        seen_content = set()

        for result in results:
            # Skip if crawling failed
            if not result.success:
                print(f"❌ Failed to crawl: {result.url}")
                continue

            # Skip duplicate content (same markdown content)
            if result.markdown:
                content_hash = hash(result.markdown)
                if content_hash in seen_content:
                    continue
                seen_content.add(content_hash)

            # Get title from seeder data (extracted head) or fallback to crawler metadata
            seeder_info = url_data.get(result.url, {})
            seeder_title = seeder_info.get("head_data", {}).get("title", "").strip()
            crawler_title = result.metadata.get("title", "").strip()

            # Prefer seeder title (from extract_head) over crawler title
            final_title = seeder_title or crawler_title

            page_data = {
                "title": final_title,
                "url": result.url,
                "markdown": result.markdown,
                "source": "Molecule Blog",
            }
            pages_data.append(page_data)

        # Save to JSON file
        with open("data/molecule_blog.json", "w", encoding="utf-8") as f:
            json.dump(pages_data, f, indent=2, ensure_ascii=False)

        print(f"\n💾 Saved {len(pages_data)} blog pages to data/molecule_blog.json")
        print(
            f"✅ Performance: {len(results)} pages in {time.perf_counter() - start_time:.2f} seconds"
        )


if __name__ == "__main__":
    asyncio.run(main())
