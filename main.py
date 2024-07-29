import asyncio
import psycopg2
from psycopg2 import sql
import aiohttp
from telegram import Bot

db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5433'
}
telegram_token = "6585545379:AAGaPRfB-dow1quO2OXWbuWILxzoRWZtHZY"
channel_id = "-1002149273980"
bot = Bot(token=telegram_token)

async def create_data_base():
    try:
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cur = conn.cursor()
        # cur.execute("CREATE DATABASE Petitions")
        print("Database - success")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS petitions (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                apply_date VARCHAR(255) NOT NULL,
                signatures_number INTEGER NOT NULL,
                link TEXT NOT NULL
            )
        """)
        print("Table - success")

    except psycopg2.Error as e:
        print(f"Error: {e}")

    finally:
        cur.close()
        conn.close()

async def add_row_to_db(title, apply_date, num_of_signatures, link):
    try:
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO petitions (title, apply_date, signatures_number, link) VALUES (%s, %s, %s, %s)",
            (title, apply_date, num_of_signatures, link)
        )
        print("Data has been added successfully")

    except psycopg2.Error as e:
        print(f"Error: {e}")

    finally:
        cur.close()
        conn.close()

async def get_first_title():
    url = "https://epetition.kz/api/public/v1/petitions/short?size=21&page=0"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                first_title = data['content'][0]['title']
                return first_title
            else:
                print("Failed to fetch data from API")
                return None

async def check_if_petition_new(title):
    first_title_from_db = await get_first_title_from_db()
    if title == first_title_from_db:
        return False
    else:
        return True

async def get_first_title_from_db():
    try:
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT title FROM petitions ORDER BY id DESC LIMIT 1;")
        first_row = cur.fetchone()
        if first_row:
            first_title = first_row[0]
        else:
            first_title = None
        return first_title

    except psycopg2.Error as e:
        print(f"Error: {e}")
        return None

    finally:
        cur.close()
        conn.close()

async def get_data_about_petition():
    url = "https://epetition.kz/api/public/v1/petitions/short?size=21&page=0"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                content = data['content'][0]
                signatures_number = content['signersCount']
                apply_date = content['applyDate'][:10]
                id = content['id']
                title = content['title']
                data_about_petition = {
                    'id': id,
                    'title': title,
                    'apply_date': apply_date,
                    'signatures_number': signatures_number
                }
                return data_about_petition
            else:
                print("Failed to fetch petition data")
                return None

async def print_data_about_petition(data_about_petition):
    message = (
        "Появилась новая петиция.\n"
        f"Название: {data_about_petition['title']}\n"
        f"Дата публикации: {data_about_petition['apply_date'][:10]}\n"
        f"Количество подписей: {data_about_petition['signatures_number']}\n"
        f"Ссылка: https://epetition.kz/petition/{data_about_petition['id']}?commentPage=0"
    )
    await bot.send_message(chat_id=channel_id, text=message)

async def job():
    first_title_from_website = await get_first_title()
    if first_title_from_website:
        is_petition_new = await check_if_petition_new(first_title_from_website)
        if is_petition_new:
            data_about_petition = await get_data_about_petition()
            if data_about_petition:
                await add_row_to_db(
                    data_about_petition['title'],
                    data_about_petition['apply_date'],
                    data_about_petition['signatures_number'],
                    f'https://epetition.kz/petition/{data_about_petition["id"]}?commentPage=0'
                )
                await print_data_about_petition(data_about_petition)
            else:
                print("Failed to get petition details")
        else:
            await bot.send_message(chat_id=channel_id, text="Новых петиций не появилось.")
    else:
        print("Failed to get the first title")

def clear_the_table():
    try:
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute('DELETE FROM petitions')
        print("Data has been deleted successfully")

    except psycopg2.Error as e:
        print(f"Error: {e}")

    finally:
        cur.close()
        conn.close()

async def main():
    await create_data_base()
    while True:
        await job()
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())



