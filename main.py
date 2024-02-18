import asyncio

import aiohttp
from bs4 import BeautifulSoup


async def get_page_content(url):
    for _ in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    content = await response.text()
                    return content
        except aiohttp.ClientError as e:
            print(f"Ошибка при получении контента страницы: {e}")
            await asyncio.sleep(1)
    return None


def extract_group_array(content):
    soup = BeautifulSoup(content, "lxml")
    script_tag = soup.find("script", src=False)
    script_content = script_tag.string

    start_index = script_content.find("var query = [")
    end_index = script_content.find("];", start_index)
    query_string = script_content[start_index + len("var query = ") : end_index + 1]

    query_array = eval(query_string)

    result = list(map(lambda item: item.strip(), query_array))
    result = list(filter(lambda item: item and item[0].isdigit(), result))

    return result


async def main():
    url = "https://www.polessu.by/ruz/"
    content = await get_page_content(url)
    groups = extract_group_array(content)
    print(groups)


if __name__ == "__main__":
    asyncio.run(main())
