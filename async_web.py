
import asyncio
import json
import time
import urllib.request
from collections import deque
from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag, urlparse


class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            for key, value in attrs:
                if key.lower() == "href" and value:
                    self.links.append(value)


def normalize_url(url):
    url, _ = urldefrag(url)
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        return None

    if not parsed.netloc:
        return None

    path = parsed.path or "/"

    if path != "/":
        path = path.rstrip("/")

    return parsed._replace(path=path).geturl()


def same_origin(url1, url2):
    a = urlparse(url1)
    b = urlparse(url2)

    return (
        a.scheme.lower(),
        a.netloc.lower()
    ) == (
        b.scheme.lower(),
        b.netloc.lower()
    )


def blocking_fetch(url):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "PracticeCrawler/1.0"
        },
    )

    with urllib.request.urlopen(request, timeout=8) as response:
        content_type = response.headers.get("Content-Type", "")

        if "text/html" not in content_type.lower():
            return response.status, ""

        body = response.read(1_000_000)
        charset = response.headers.get_content_charset() or "utf-8"

        return response.status, body.decode(
            charset,
            errors="replace"
        )


async def fetch(url, semaphore):
    async with semaphore:
        try:
            return await asyncio.to_thread(
                blocking_fetch, url
            )
        except Exception as error:
            return None, str(error)


class AsyncWebCrawler:
    def __init__(
        self,
        start_url,
        max_pages=20,
        concurrency=5,
        delay_seconds=0.5,
    ):
        start_url = normalize_url(start_url)

        if not start_url:
            raise ValueError("Invalid HTTP/HTTPS URL")

        if max_pages < 1 or concurrency < 1:
            raise ValueError("Limits must be positive")

        self.start_url = start_url
        self.max_pages = max_pages
        self.semaphore = asyncio.Semaphore(concurrency)
        self.delay_seconds = delay_seconds

        self.visited = set()
        self.results = []
        self.host_locks = {}
        self.last_request = {}

    async def polite_fetch(self, url):
        host = urlparse(url).netloc.lower()

        if host not in self.host_locks:
            self.host_locks[host] = asyncio.Lock()

        async with self.host_locks[host]:
            previous = self.last_request.get(host, 0)
            wait = self.delay_seconds - (
                time.monotonic() - previous
            )

            if wait > 0:
                await asyncio.sleep(wait)

            self.last_request[host] = time.monotonic()

            return await fetch(url, self.semaphore)

    async def crawl(self):
        queue = deque([self.start_url])
        queued = {self.start_url}

        while queue and len(self.visited) < self.max_pages:
            batch = []

            while (
                queue
                and len(batch) < self.semaphore._value
                and len(self.visited) < self.max_pages
            ):
                url = queue.popleft()
                queued.discard(url)

                if url not in self.visited:
                    self.visited.add(url)
                    batch.append(url)

            if not batch:
                continue

            pages = await asyncio.gather(
                *[
                    self.polite_fetch(url)
                    for url in batch
                ]
            )

            for url, (status, content) in zip(batch, pages):
                if status is None:
                    print(f"[ERROR] {url}: {content}")

                    self.results.append({
                        "url": url,
                        "status": "ERROR",
                        "error": content,
                    })
                    continue

                parser = LinkParser()
                parser.feed(content)

                new_links = set()

                for href in parser.links:
                    link = normalize_url(urljoin(url, href))

                    if (
                        link
                        and same_origin(self.start_url, link)
                        and link not in self.visited
                        and link not in queued
                        and len(self.visited) + len(queue)
                        < self.max_pages
                    ):
                        new_links.add(link)

                for link in sorted(new_links):
                    queue.append(link)
                    queued.add(link)

                self.results.append({
                    "url": url,
                    "status": status,
                    "links_found": len(new_links),
                })

                print(
                    f"[{status}] {url} "
                    f"| New links: {len(new_links)}"
                )

        return self.results

    def save_json(self, filename="crawl_results.json"):
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(
                self.results,
                file,
                indent=2,
                ensure_ascii=False,
            )

        print(f"Results saved to {filename}")


async def main():
    crawler = AsyncWebCrawler(
        start_url="https://example.com/",
        max_pages=10,
        concurrency=3,
        delay_seconds=0.5,
    )

    results = await crawler.crawl()
    crawler.save_json()

    print(f"\nCrawled pages: {len(results)}")


if __name__ == "__main__":
    asyncio.run(main())




