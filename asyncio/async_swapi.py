import asyncio
from aiohttp import ClientSession
import datetime


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
    print(f'end {person_id}')
    return json_data


async def main():
    coroutines = []
    amount = await get_people_amount()
    for i in range(1, amount + 1):
        coroutines.append(get_person(person_id=i))
    results = await asyncio.gather(*coroutines)
    for result in results:
        print(result)


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
