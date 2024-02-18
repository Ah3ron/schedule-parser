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


def extract_week_start_dates(content):
    soup = BeautifulSoup(content, "lxml")
    week_dates = {}
    for li in soup.find("ul", id="weeks-menu").find_all("li"):
        if not li.a:
            continue

        if li.a["href"] == "#":
            continue

        week_num = li.a["href"][2:]
        week_dates[week_num] = li.a.text.split("(")[1][:5]
    return week_dates


async def main():
    url = "https://www.polessu.by/ruz/term2/?q=22%D0%98%D0%A2-1"
    content = await get_page_content(url)
    # groups = extract_group_array(content)
    week_start_dates = extract_week_start_dates(content)
    print(week_start_dates)


if __name__ == "__main__":
    asyncio.run(main())
