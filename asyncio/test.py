import requests


json_data = requests.get(f'https://swapi.dev/api/people/1').json()

people_dict = dict()
people_dict['name'] = json_data['name']
print(people_dict)
print(json_data)
