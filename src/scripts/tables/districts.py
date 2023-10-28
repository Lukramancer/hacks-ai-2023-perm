from OSMPythonTools.overpass import Overpass, OverpassResult
from requests import get
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from env import DATABASE
from src.tables import District

overpass_api = Overpass()
districts_query_result: OverpassResult = overpass_api.query("rel(56.035, 51.394, 61.867, 59.875)[boundary=administrative];out;")
possible_districts = districts_query_result.elements()

parsed_districts = list[District]()

import csv

with (open("../../../Данные/Данные по метеостанциям. Соответствие МО.csv") as input_table_file):
    table_reader = csv.reader(input_table_file, delimiter=',')
    table_header = next(table_reader)
    for district_name, meteorology_station_name in table_reader:
        district_name, meteorology_station_name = district_name.strip(), meteorology_station_name.strip()

        osm_id = None

        if osm_id is None:
            expanded_name: str = district_name.replace("МО", "муниципальный округ").replace("ГО", "городской округ")
            for element in possible_districts:
                if expanded_name in element.tag("name").replace('ё', 'е'):
                    osm_id = element.id()
                    break

        if osm_id is None:
            shortened_name: str = district_name.replace("МО", '').replace("ГО", '').strip().split()[1]
            for element in possible_districts:
                if shortened_name in element.tag("name").replace('ё', 'е'):
                    osm_id = element.id()
                    break

        if osm_id is None:
            print(f"Error: \"{district_name}\" was not found, skipping")
            continue

        meteorology_station_name: str = meteorology_station_name \
            .replace("г. ", '').replace("п. ", '').replace("с. ", '').strip()

        geometry = get(f"https://polygons.openstreetmap.fr/get_geojson.py?id={osm_id}&params=0").json()

        parsed_districts.append(District(osm_id=osm_id, name=district_name, meteorology_station_name=meteorology_station_name, geometry=geometry))


engine = create_engine(DATABASE)
db_session_maker = sessionmaker(bind=engine)
db_session = db_session_maker()

District.__table__.drop(engine)
District.__table__.create(engine)

db_session.add_all(parsed_districts)
db_session.commit()
