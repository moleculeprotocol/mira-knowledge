import asyncio
import json
import re
import time

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.filters import DomainFilter, FilterChain, URLPatternFilter


def extract_title_from_markdown(markdown_content):
    """Extract the first h1 heading from markdown content as the page title."""
    if not markdown_content:
        return ""

    # Look for the first level 1 heading (# Title)
    match = re.search(r"^#\s+(.+?)$", markdown_content, re.MULTILINE)
    if match:
        # Clean up the title - remove any emoji and extra whitespace
        title = match.group(1).strip()
        # Remove all emojis using comprehensive Unicode ranges
        # This covers all emoji blocks and handles compound emojis with ZWJ sequences
        emoji_pattern = re.compile(
            r"[\U0001F600-\U0001F64F]|"  # emoticons
            r"[\U0001F300-\U0001F5FF]|"  # symbols & pictographs
            r"[\U0001F680-\U0001F6FF]|"  # transport & map
            r"[\U0001F1E0-\U0001F1FF]|"  # flags (iOS)
            r"[\U00002600-\U000026FF]|"  # miscellaneous symbols
            r"[\U00002700-\U000027BF]|"  # dingbats
            r"[\U0001F900-\U0001F9FF]|"  # supplemental symbols and pictographs
            r"[\U0001FA70-\U0001FAFF]|"  # symbols and pictographs extended-A
            r"[\U0000FE00-\U0000FE0F]|"  # variation selectors
            r"[\U0000200D]|"  # zero width joiner (for compound emojis)
            r"[\U00002000-\U0000206F]"  # general punctuation (includes various spaces)
        )
        title = emoji_pattern.sub("", title).strip()
        return title

    return ""


async def main():
    domain_filter = DomainFilter(allowed_domains=["docs.molecule.to"])

    # Filter out GitBook revision URLs (containing "~/revisions/") and noise
    url_pattern_filter = URLPatternFilter(
        patterns=["*~/revisions/*", "*www.wrappr.wtf*"], reverse=True
    )

    filter_chain = FilterChain([domain_filter, url_pattern_filter])

    config = CrawlerRunConfig(
        deep_crawl_strategy=BFSDeepCrawlStrategy(
            max_depth=25, filter_chain=filter_chain
        ),
        scraping_strategy=LXMLWebScrapingStrategy(),
        css_selector="main",
        verbose=True,  # Show progress during crawling
    )

    async with AsyncWebCrawler() as crawler:
        start_time = time.perf_counter()
        results = await crawler.arun(
            url="https://docs.molecule.to/documentation", config=config
        )

        # Group results by depth to visualize the crawl tree
        pages_by_depth = {}
        for result in results:
            depth = result.metadata.get("depth", 0)
            if depth not in pages_by_depth:
                pages_by_depth[depth] = []
            pages_by_depth[depth].append(result.url)

        print(f"✅ Crawled {len(results)} pages total")

        # Display crawl structure by depth
        for depth, urls in sorted(pages_by_depth.items()):
            print(f"\nDepth {depth}: {len(urls)} pages")
            # Show first 3 URLs for each depth as examples
            for url in urls[:3]:
                print(f"  → {url}")
            if len(urls) > 3:
                print(f"  ... and {len(urls) - 3} more")

                # Extract page data for JSON export
        pages_data = []
        seen_content = set()
        for result in results:
            # Skip duplicate content (same markdown content)
            content_hash = hash(result.markdown)
            if content_hash in seen_content:
                continue
            seen_content.add(content_hash)

            # Extract title from markdown content
            extracted_title = extract_title_from_markdown(result.markdown)

            page_data = {
                "title": extracted_title or result.metadata.get("title", ""),
                "url": result.url,
                "markdown": result.markdown,
                "source": "Molecule Docs",
            }
            pages_data.append(page_data)

        # Sort pages by title before saving
        pages_data.sort(key=lambda x: x.get("title", "").lower())

        # Save to JSON file
        with open("data/molecule_docs.json", "w", encoding="utf-8") as f:
            json.dump(pages_data, f, indent=2, ensure_ascii=False)

        print(f"\n💾 Saved {len(pages_data)} pages to data/molecule_docs.json")
        print(
            f"\n✅ Performance: {len(results)} pages in {time.perf_counter() - start_time:.2f} seconds"
        )


if __name__ == "__main__":
    asyncio.run(main())
