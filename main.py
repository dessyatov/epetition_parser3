from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, select
from sqlalchemy.orm import sessionmaker, Session
import asyncio
import aiohttp
from telegram import Bot
import os
import dotenv


dotenv.load_dotenv()
database_url = os.getenv("DATABASE_URL")
telegram_token = os.getenv("TOKEN")
channel_id = os.getenv('CHANNEL')
bot = Bot(token=telegram_token)


engine = create_engine(database_url)
metadata = MetaData()
session = sessionmaker(bind=engine)


petitions_table = Table(
    'petitions_table', metadata,
    Column('id', Integer(), primary_key=True),
    Column('title', String(200), nullable=False),
    Column('apply_date', String(200), nullable=False),
    Column('signatures_number', Integer(), nullable=False),
    Column('link', String(200), default=None)
)

class Petition:
    def __init__(self, title, apply_date, signatures_number, link):
        self.title = title
        self.apply_date = apply_date
        self.signatures_number = signatures_number
        self.link = link

def create_database():
    try:
        metadata.create_all(engine)
        session.commit()
        print("Database and table created successfully.")
    except Exception as e:
        print(f"Error creating database or table: {e}")



def clear_table():
    try:
        with engine.connect() as conn:
            conn.execute(petitions_table.delete())
            print("Table cleared successfully.")
    except Exception as e:
        print(f"Error clearing table: {e}")




def add_row_to_db(petition):
    try:
        ins = petitions_table.insert().values(
            title=petition.title,
            apply_date=petition.apply_date,
            signatures_number=petition.signatures_number,
            link=petition.link
        )
        with engine.connect() as conn:
            conn.execute(ins)
            conn.commit()
        print("Row was added to db")
    except Exception as e:
        print(f"Error adding row to database: {e}")


async def get_first_title():
    url = "https://epetition.kz/api/public/v1/petitions/short?size=21&page=0"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                first_title = data['content'][0]['title']
                print("First title from website was received")
                print(first_title)
                return first_title
            else:
                print("Failed to fetch data from API")
                return None


def check_if_petition_new(title):
    try:
        first_title_from_db = get_first_title_from_db()
        if first_title_from_db == title:
            print("Petition is not new")
            return False
        else:
            print("Petition is new")
            return True
    except Exception as e:
        print(f"Error checking if petition is new: {e}")
        return None


def get_first_title_from_db():
    try:
        stmt = select(petitions_table.c.title).order_by(petitions_table.c.id.desc()).limit(1)
        with engine.connect() as conn:
            result = conn.execute(stmt)
            row = result.fetchone()
            if row:
                print("First title from db was received")
                print(row[0])

                return row[0]

            else:
                print("First title from db was not received")
            return None
    except Exception as e:
        print(f"Error fetching first title from database: {e}")
        return None


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
                link = f"https://epetition.kz/petition/{id}?commentPage=0"
                petition = Petition(title, apply_date, signatures_number, link)
                return petition
            else:
                print("Failed to fetch petition data")
                return None


async def print_data_about_petition(petition):
    message = (
        "Появилась новая петиция.\n"
        f"Название: {petition.title}\n"
        f"Дата публикации: {petition.apply_date}\n"
        f"Количество подписей: {petition.signatures_number}\n"
        f"Ссылка: {petition.link}"
    )
    await bot.send_message(chat_id=channel_id, text=message)

async def job():
    first_title_from_website = await get_first_title()
    if first_title_from_website:
        is_petition_new = check_if_petition_new(first_title_from_website)
        if is_petition_new:
            petition = await get_data_about_petition()
            if petition:
                add_row_to_db(petition)
                await print_data_about_petition(petition)
            else:
                print("Failed to get petition details")
        else:
            await bot.send_message(chat_id=channel_id, text="Новых петиций не появилось.")
    else:
        print("Failed to get the first title")


async def main():
    while True:
        await job()
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())