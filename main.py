import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp
import asyncpg
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def get_or_create_table(conn):
    exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'lessons'
        )
        """
    )
    if not exists:
        await conn.execute(
            """
            CREATE TABLE lessons (
                id SERIAL PRIMARY KEY,
                group_name VARCHAR(255) NOT NULL,
                lesson_date VARCHAR(5) NOT NULL,
                day_of_week VARCHAR(255) NOT NULL,
                lesson_time VARCHAR(255) NOT NULL,
                lesson_name VARCHAR(255) NOT NULL,
                location VARCHAR(255),
                teacher VARCHAR(255),
                subgroup VARCHAR(255)
            )
            """
        )


async def get_page_content(url):
    for _ in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    content = await response.text()
                    return content
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка при получении контента страницы: {e}")
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

    result = [
        item.strip()
        for item in query_array
        if item and item[0].isdigit() and not "(" in item and not ")" in item
    ]

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


async def extract_schedule(content):
    soup = BeautifulSoup(content, "lxml")
    table = soup.find("tbody", id="weeks-filter")
    if not table:
        logger.warning("Не удалось найти расписание на странице")
        return None

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
                "subgroup": lesson_info[4].text or None,
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


async def fetch_schedule(group):
    urls = [
        f"https://www.polessu.by/ruz/term2/?q={group}&f=1",
        f"https://www.polessu.by/ruz/term2/?q={group}&f=2",
        f"https://www.polessu.by/ruz/?q={group}&f=1",
        f"https://www.polessu.by/ruz/?q={group}&f=2",
    ]

    all_schedules = {}

    for url in urls:
        content = await get_page_content(url)
        if not content:
            logger.error(f"Не удалось получить контент страницы для группы {group}")
            continue

        schedule = await extract_schedule(content)
        if schedule:
            logger.info(f"Расписание для группы {group} успешно получено")
            all_schedules[group] = schedule
        else:
            logger.warning(f"Не удалось получить расписание для группы {group}")

    return group, all_schedules.get(group)


async def main():
    conn = await asyncpg.connect(
        "postgresql://postgres:WKGylwALZFqzCNcDZcBQsrHFSnZyhxrU@roundhouse.proxy.rlwy.net:36744/railway"
    )

    await get_or_create_table(conn)

    url = "https://www.polessu.by/ruz"
    content = await get_page_content(url)
    if not content:
        logger.error("Не удалось получить контент главной страницы")
        return

    groups = extract_group_array(content)

    all_schedules = {}
    for group in groups:
        _, schedule = await fetch_schedule(group)
        if schedule:
            await conn.execute("DELETE FROM lessons WHERE group_name = $1", group)
            for lesson in schedule:
                await conn.execute(
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
                    group,
                    lesson["date"],
                    lesson["day_of_week"],
                    lesson["time"],
                    lesson["name"],
                    lesson["location"],
                    lesson["teacher"],
                    lesson["subgroup"],
                )

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

