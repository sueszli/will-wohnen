import asyncio
import multiprocessing

from aiohttp import ClientSession
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def get_links(url):
    async def fetch(url, session):
        async with session.get(url) as response:
            return await response.text()

    async def get_links(html):
        soup = BeautifulSoup(html, 'html.parser')
        return [urljoin(url, link.get('href')) for link in soup.find_all('a')]

    async def main():
        async with ClientSession() as session:
            html = await fetch(url, session)
            return await get_links(html)

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(main())

def main():
    urls = ['https://www.google.com', 'https://www.yahoo.com', 'https://www.bing.com']
    
    with multiprocessing.Pool() as pool:
        results = pool.map(get_links, urls)
    
    for url, links in zip(urls, results):
        print(f'Links in {url}: {links}')

if __name__ == '__main__':
    main()
