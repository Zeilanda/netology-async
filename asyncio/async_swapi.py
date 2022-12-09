import asyncio
from aiohttp import ClientSession
import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from more_itertools import chunked

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


async def chunked_async(async_iter, size):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_people_amount():
    print(f'begin people amount get')
    async with ClientSession() as session:
        response = await session.get(f'https://swapi.dev/api/people/')
        json_data = await response.json()
        amount = json_data['count']
    print(f'end people amount')
    return amount


async def get_person(person_id):
    print(f'begin {person_id}')
    async with ClientSession() as session:
        response = await session.get(f'https://swapi.dev/api/people/{person_id}')
        json_data = await response.json()
        peoples_dict = dict()
        if 'detail' not in json_data:
            peoples_dict['id'] = person_id
            peoples_dict['name'] = json_data['name']
            peoples_dict['birth_year'] = json_data['birth_year']
            peoples_dict['eye_color'] = json_data['eye_color']
            peoples_dict['films'] = ", ".join(json_data['films'])
            peoples_dict['gender'] = json_data['gender']
            peoples_dict['height'] = json_data['height']
            peoples_dict['hair_color'] = json_data['hair_color']
            peoples_dict['homeworld'] = json_data['homeworld']
            peoples_dict['mass'] = json_data['mass']
            peoples_dict['skin_color'] = json_data['skin_color']
            peoples_dict['species'] = ", ".join(json_data['species'])
            peoples_dict['starships'] = ", ".join(json_data['starships'])
            peoples_dict['vehicles'] = ", ".join(json_data['vehicles'])
    print(f'end {person_id}')
    return peoples_dict


async def get_people():
    coroutines = []
    amount = await get_people_amount()
    for i in range(1, amount + 1):
        coroutines.append(get_person(person_id=i))
    results = await asyncio.gather(*coroutines)
    for item in results:
        print(item)
        yield item

    # coroutines = []
    # amount = await get_people_amount()
    # for i in range(1, amount + 1):
    #     coroutines.append(get_person(person_id=i))
    # results = await asyncio.gather(*coroutines)
    # for item in results:
    #     yield item


async def insert_people(people_chunk):
    async with Session() as session:
        for item in people_chunk:
            session.add(Person(**item))
        # session.add_all([Person(id='id',
        #                         name='name',
        #                         birth_year='birth_year',
        #                         eye_color='eye_color',
        #                         films='films',
        #                         gender='gender',
        #                         height='height',
        #                         homeworld='homeworld',
        #                         mass='mass',
        #                         skin_color='skin_color',
        #                         species='species',
        #                         starships='starships',
        #                         vehicles='vehicles') for item in people_chunk])
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for item in get_people():
        async with Session() as session:
            session.add(Person(id=item['id'],
                               name=item['name'],
                               birth_year=item['birth_year'],
                               eye_color=item['eye_color'],
                               films=item['films'],
                               gender=item['gender'],
                               height=int(item['height']),
                               hair_color=item['hair_color'],
                               homeworld=item['homeworld'],
                               mass=int(item['mass']),
                               skin_color=item['skin_color'],
                               species=item['species'],
                               starships=item['starships'],
                               vehicles=item['vehicles']))
            await session.commit()


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
