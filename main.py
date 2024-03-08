from datetime import datetime, timedelta
import asyncio
import json
import re

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

    result = [item.strip() for item in query_array if item and item[0].isdigit() and not '(' in item and not ')' in item]

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


def extract_schedule(content):
    soup = BeautifulSoup(content, "lxml")
    table = soup.find("tbody", id="weeks-filter")

    schedule = []
    current_day_of_week = None

    week_start_dates = extract_week_start_dates(content)

    for row in table.find_all("tr"):
        if "wa" in row["class"]:
            current_day_of_week = row.find("th").text
            continue

        for class_name in row["class"]:
            if not class_name.startswith("w"):
                continue

            lesson_info = row.find_all("td")

            lesson = {
                "date": calculate_date(
                    week_start_dates[class_name[1:]], current_day_of_week
                ),
                "day_of_week": current_day_of_week,
                "time": lesson_info[0].text,
                "name": lesson_info[1].text,
                "location": lesson_info[2].text or None,
                "teacher": lesson_info[3].text or None,
                "group": lesson_info[4].text or None,
            }

            schedule.append(lesson)

    return schedule


def convert_day_to_number(day_of_week):
    day_numbers = {
        "Понедельник": 0,
        "Вторник": 1,
        "Среда": 2,
        "Четверг": 3,
        "Пятница": 4,
        "Суббота": 5,
        "Воскресенье": 6,
    }

    return day_numbers.get(day_of_week, -1)


def calculate_date(week_start_date, day_of_week):
    date = datetime.strptime(week_start_date, "%d.%m").replace(year=datetime.now().year)
    date += timedelta(days=convert_day_to_number(day_of_week))
    return date.strftime("%d.%m")

def check_schedule_exists(content):
    soup = BeautifulSoup(content, "lxml")
    error_message = soup.find("p", string="Ничего не найдено.")
    return error_message is None

async def main():
    url = "https://www.polessu.by/ruz/term2/?q=22%D0%98%D0%A2-1"
    content = await get_page_content(url)
    # groups = extract_group_array(content)
    # week_start_dates = extract_week_start_dates(content)
    schedule = extract_schedule(content)

    json_string = json.dumps(schedule, indent=4, ensure_ascii=False)
    print(json_string)
    # print(len(schedule))


if __name__ == "__main__":
    asyncio.run(main())
