import asyncio
from aiohttp import ClientSession
import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


PG_DSN = 'postgresql+asyncpg://app:1234@127.0.0.1:5431/async'
engine = create_async_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
CHUNK_SIZE = 10


class Person(Base):
    __tablename__ = 'starwars_persons'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(Integer)
    homeworld = Column(String)
    mass = Column(Integer)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String, nullable=True)
    vehicles = Column(String, nullable=True)


class PageNotFound(Exception):
    pass


async def get_page(page_number: int) -> dict:
    async with ClientSession() as session:
        response = await session.get(f'https://swapi.dev/api/people/?page={page_number}')
        if response.status == 200:
            page_data = await response.json()
            return page_data
    raise PageNotFound


def parse_page(page: dict) -> list:
    page_people = []
    for item in page["results"]:
        peoples_dict = dict()
        peoples_dict['name'] = item['name']
        peoples_dict['birth_year'] = item['birth_year']
        peoples_dict['eye_color'] = item['eye_color']
        peoples_dict['films'] = ", ".join(item['films'])
        peoples_dict['gender'] = item['gender']
        try:
            height = int(item["height"])
        except ValueError:
            height = None
        peoples_dict['height'] = height
        peoples_dict['hair_color'] = item['hair_color']
        peoples_dict['homeworld'] = item['homeworld']
        try:
            mass = int(item["mass"])
        except ValueError:
            mass = None
        peoples_dict['mass'] = mass
        peoples_dict['skin_color'] = item['skin_color']
        peoples_dict['species'] = ", ".join(item['species'])
        peoples_dict['starships'] = ", ".join(item['starships'])
        peoples_dict['vehicles'] = ", ".join(item['vehicles'])
        peoples_dict["id"] = int(item["url"].split("/")[-2])
        page_people.append(peoples_dict)
    return page_people


async def load_all_people() -> list:
    result = []
    try:
        page_number = 1
        while True:
            page = await get_page(page_number)
            page_number += 1
            people = parse_page(page)
            result += people
    except PageNotFound:
        return result


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    for item in await load_all_people():
        print(item)
        async with Session() as session:
            session.add(Person(id=item['id'],
                               name=item['name'],
                               birth_year=item['birth_year'],
                               eye_color=item['eye_color'],
                               films=item['films'],
                               gender=item['gender'],
                               height=item['height'],
                               hair_color=item['hair_color'],
                               homeworld=item['homeworld'],
                               mass=item['mass'],
                               skin_color=item['skin_color'],
                               species=item['species'],
                               starships=item['starships'],
                               vehicles=item['vehicles']))
            await session.commit()


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
