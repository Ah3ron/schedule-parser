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


def calculate_date(week_start_date, day_of_week):
    date = datetime.strptime(week_start_date, "%d.%m").replace(year=datetime.now().year)
    date += timedelta(days=convert_day_to_number(day_of_week))
    return date.strftime("%d.%m")


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


async def get_or_create_table(conn):
    exists_lessons = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'lessons'
        )
        """
    )
    if not exists_lessons:
        await conn.execute(
            """
            CREATE TABLE lessons (
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
            """
        )
        await conn.execute(
            """
            CREATE INDEX idx_lesson_date_and_group_name ON lessons (lesson_date, group_name);
            """
        )

    exists_schedule_updates = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'schedule_updates'
        )
        """
    )
    if not exists_schedule_updates:
        await conn.execute(
            """
            CREATE TABLE schedule_updates (
                id SERIAL PRIMARY KEY,
                last_update_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )


async def fetch_page_content(session, url):
    for _ in range(3):
        try:
            async with session.get(url) as response:
                content = await response.text()
                return content
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching page content: {e}")
            await asyncio.sleep(1)
    return None


def extract_group_array(soup):
    result = []
    script_tag = soup.find("script", src=False)
    if not script_tag:
        return result

    script_content = script_tag.string
    start_index = script_content.find("var query = [")
    end_index = script_content.find("];", start_index)

    if start_index == -1 or end_index == -1:
        return result

    query_string = script_content[start_index + len("var query = ") : end_index + 1]
    query_array = eval(query_string)
    result = [
        item.strip()
        for item in query_array
        if item and item[0].isdigit() and "(" not in item and ")" not in item
    ]
    return result


def get_week_start_dates(soup):
    week_start_dates = {}
    for li in soup.find("ul", id="weeks-menu").find_all("li"):
        if not li.a or li.a["href"] == "#":
            continue
        week_num = li.a["href"][2:]
        week_start_dates[week_num] = li.a.text.split("(")[1][:5]
    return week_start_dates


def extract_lessons(table, week_start_dates):
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

    week_start_dates = get_week_start_dates(soup)
    schedule = extract_lessons(table, week_start_dates)
    return schedule


async def extract_schedule(session, group):
    urls = [
        f"https://www.polessu.by/ruz/term2/?q={group}&f=1",
        f"https://www.polessu.by/ruz/term2/?q={group}&f=2",
        f"https://www.polessu.by/ruz/?q={group}&f=1",
        f"https://www.polessu.by/ruz/?q={group}&f=2",
    ]
    all_schedules = {}

    for url in urls:
        content = await fetch_page_content(session, url)
        if content:
            schedule = await parse_schedule(content)
            if schedule:
                all_schedules[group] = schedule
                logger.info(f"Schedule for group {group} successfully fetched")
                break
        else:
            logger.error(f"Failed to fetch page content for group {group}")

    return group, all_schedules.get(group)


async def fetch_last_update_date(session, url):
    content = await fetch_page_content(session, url)
    if not content:
        logger.error("Failed to fetch content from the main page")
        return None

    soup = BeautifulSoup(content, "lxml")
    containers = soup.find_all("div", class_="container")

    for container in containers:
        last_update_tag = container.find("p", class_="small")
        if last_update_tag:
            last_update_text = last_update_tag.text.strip()
            date_match = re.search(r"\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}", last_update_text)
            if date_match:
                last_update_str = date_match.group()
                last_update_date = datetime.strptime(last_update_str, "%d.%m.%Y %H:%M")
                return last_update_date

    logger.error("Failed to find last update information on the page")
    return None


async def main():
    load_dotenv()
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))

    await get_or_create_table(conn)

    url = "https://www.polessu.by/ruz"
    async with aiohttp.ClientSession() as session:
        last_update_date = await fetch_last_update_date(session, url)
        if not last_update_date:
            logger.error("Failed to fetch last update date")
            await conn.close()
            return

        stored_last_update_date = await conn.fetchval(
            "SELECT MAX(last_update_date) FROM schedule_updates"
        )
        if stored_last_update_date and last_update_date <= stored_last_update_date:
            logger.info("No new schedule update available")
        else:
            content = await fetch_page_content(session, url)
            if not content:
                logger.error("Failed to fetch content from the main page")
                await conn.close()
                return

            soup = BeautifulSoup(content, "lxml")
            group_array = extract_group_array(soup)

            tasks = [extract_schedule(session, group) for group in group_array]

            results = await asyncio.gather(*tasks)

            batch_data = []

            for group, schedule in results:
                if schedule:
                    for lesson in schedule:
                        batch_data.append(
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
                        )

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
                    f"The new schedule was saved in {time.time() - start:.2f} seconds\n"
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
