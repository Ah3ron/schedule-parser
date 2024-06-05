import asyncio
import logging
import os
import re
import time
from datetime import datetime, timedelta

import aiohttp
import asyncpg
from bs4 import BeautifulSoup
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

WEEKDAY_TO_NUMBER = {
    "Понедельник": 0,
    "Вторник": 1,
    "Среда": 2,
    "Четверг": 3,
    "Пятница": 4,
    "Суббота": 5,
    "Воскресенье": 6,
}
BASE_URL = "https://www.polessu.by/ruz/"


def calculate_date(week_start_date, day_of_week):
    date = datetime.strptime(week_start_date, "%d.%m").replace(year=datetime.now().year)
    date += timedelta(days=WEEKDAY_TO_NUMBER[day_of_week])
    return date.strftime("%d.%m")


async def fetch_content_with_retries(session, url, retries=3, delay=1):
    for _ in range(retries):
        try:
            async with session.get(url) as response:
                return await response.text()
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching page content: {e}")
            await asyncio.sleep(delay)
    return None


def extract_group_list(soup):
    script_tag = soup.find("script", src=False)
    if not script_tag:
        return []
    script_content = script_tag.string
    query_string = re.search(r"var query = (\[.*?\]);", script_content)
    if not query_string:
        return []
    query_array = eval(query_string.group(1))
    return [
        item.strip()
        for item in query_array
        if item and item[0].isdigit() and "(" not in item and ")" not in item
    ]


def extract_week_start_dates(soup):
    week_start_dates = {}
    for li in soup.find("ul", id="weeks-menu").find_all("li"):
        if li.a and li.a["href"] != "#":
            week_num = li.a["href"][2:]
            week_start_dates[week_num] = li.a.text.split("(")[1][:5]
    return week_start_dates


def extract_lessons_from_table(table, week_start_dates):
    schedule = []
    current_day_of_week = None

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
                "subgroup": lesson_info[4].text or None,
            }
            schedule.append(lesson)

    return schedule


async def parse_schedule(content):
    soup = BeautifulSoup(content, "lxml")
    table = soup.find("tbody", id="weeks-filter")
    if not table:
        logger.warning("Failed to find schedule on the page")
        return None
    week_start_dates = extract_week_start_dates(soup)
    return extract_lessons_from_table(table, week_start_dates)


async def fetch_group_schedule(session, group):
    urls = [
        f"{BASE_URL}term2/?q={group}&f=1",
        f"{BASE_URL}term2/?q={group}&f=2",
        f"{BASE_URL}?q={group}&f=1",
        f"{BASE_URL}?q={group}&f=2",
    ]
    for url in urls:
        content = await fetch_content_with_retries(session, url)
        if content:
            schedule = await parse_schedule(content)
            if schedule:
                logger.info(f"Schedule for group {group} successfully fetched")
                return group, schedule
        else:
            logger.error(f"Failed to fetch page content for group {group}")
    return group, None


async def fetch_last_update_date(session, url):
    async def get_update_date(url):
        content = await fetch_content_with_retries(session, url)
        if not content:
            return None
        soup = BeautifulSoup(content, "lxml")

        containers = soup.find_all("div", class_="container")
        for container in containers:
            update_tag = container.find("p", class_="small")

        if update_tag:
            date_match = re.search(
                r"\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}", update_tag.text.strip()
            )
            if date_match:
                return datetime.strptime(date_match.group(), "%d.%m.%Y %H:%M")
        return None

    main_date = await get_update_date(url)
    term2_date = await get_update_date(url + "term2")
    return (
        max(main_date, term2_date)
        if main_date and term2_date
        else main_date or term2_date
    )


async def create_tables_if_not_exists(conn):
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lessons (
            id SERIAL PRIMARY KEY,
            group_name VARCHAR(16) NOT NULL,
            lesson_date VARCHAR(8) NOT NULL,
            day_of_week VARCHAR(12) NOT NULL,
            lesson_time VARCHAR(16) NOT NULL,
            lesson_name VARCHAR(192) NOT NULL,
            location VARCHAR(32),
            teacher VARCHAR(384),
            subgroup VARCHAR(16)
        );
        CREATE INDEX IF NOT EXISTS idx_lesson_date_and_group_name ON lessons (lesson_date, group_name);
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schedule_updates (
            id SERIAL PRIMARY KEY,
            last_update_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


async def main():
    load_dotenv()
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))

    await create_tables_if_not_exists(conn)

    async with aiohttp.ClientSession() as session:
        last_update_date = await fetch_last_update_date(session, BASE_URL)
        if not last_update_date:
            logger.error("Failed to fetch last update date")
            await conn.close()
            return

        stored_last_update_date = await conn.fetchval(
            "SELECT MAX(last_update_date) FROM schedule_updates"
        )

        if stored_last_update_date and last_update_date <= stored_last_update_date:
            logger.info("No new schedule update available")
            await conn.close()
            return

        content = await fetch_content_with_retries(session, BASE_URL)
        if not content:
            logger.error("Failed to fetch content from the main page")
            await conn.close()
            return

        soup = BeautifulSoup(content, "lxml")
        group_list = extract_group_list(soup)

        tasks = [fetch_group_schedule(session, group) for group in group_list]
        results = await asyncio.gather(*tasks)

        batch_data = [
            (
                group,
                lesson["date"],
                lesson["day_of_week"],
                lesson["time"],
                lesson["name"],
                lesson["location"],
                lesson["teacher"],
                lesson["subgroup"],
            )
            for group, schedule in results
            if schedule
            for lesson in schedule
        ]

        if batch_data:
            await conn.execute("DELETE FROM lessons")
            start = time.time()
            await conn.executemany(
                """
                INSERT INTO lessons (
                    group_name, 
                    lesson_date, 
                    day_of_week, 
                    lesson_time, 
                    lesson_name, 
                    location, 
                    teacher, 
                    subgroup
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                batch_data,
            )
            logger.info(
                f"The new schedule was saved in {time.time() - start:.2f} seconds"
            )

        await conn.execute(
            """
            INSERT INTO schedule_updates (last_update_date) VALUES ($1)
            """,
            last_update_date,
        )

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
