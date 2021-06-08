"""Unittest for postgress client adapter"""
import os
import json
from datetime import datetime as dt
# dt = str(datetime.now(timezone.utc))

from mc_automation_tools import postgress
user = os.environ['PG_USER']
password = os.environ['PG_PASS']
host = os.environ['PG_HOST']
db_name = os.environ['PG_DB_NAME']
table_name = 'test_full'

entity_sample = {"geo_json": {
    "type": "Polygon",
    "coordinates": [
        [
            [35.201037526130676, 31.769924871200327],
            [35.20086854696274, 31.769892946554663],
            [35.201058983802795, 31.76914499456106],
            [35.20123064517975, 31.769181480164423],
            [35.201037526130676, 31.769924871200327]
        ]
    ]
}}
geo_json = json.dumps(entity_sample['geo_json'])
entity_sample = json.dumps(entity_sample)
uuid = "'acf8c600-423f-402c-815a-f986c34352ec'"

# geo_json = entity_sample["geo_json"]
# geo_json= entity_sample
command = f'INSERT INTO "v_buildings"("entity_id","layer_id","name","type",dateCreation,updateCreation,"polygon","json_object")' \
          f"VALUES({uuid},'416','building_1','building','{str(dt.now())}','{str(dt.now())}',ST_GeomFromGeoJSON('{geo_json}'),'{entity_sample}')"
def test_connection():
    """This test check connection to db"""
    client = postgress.PGClass(host, db_name, user, password)
    assert True
    commands = ["CREATE TABLE v_buildings "
                "(entity_id UUID PRIMARY KEY,"
                "layer_id VARCHAR(255) NOT NULL,"
                "name VARCHAR(255),"
                "type VARCHAR(255),"
                "dateCreation TIMESTAMP NOT NULL,"
                "updateCreation TIMESTAMP NOT NULL,"
                "polygon geometry,"
                "json_object JSON NOT NULL)",
                "CREATE INDEX geo_coordinate_idx ON v_buildings USING GIST(polygon)"]

    client.command_execute(commands)


# command = f'INSERT INTO "v_buildings"("entity_id","name","type","dateCreation","updateCreation","polygon",json_object' \
#           f'VALUES("{uuid}","416","building_1","building",{dt},{dt},{entity_sample["geo_json"]},{json.dumps(entity_sample)} )'
def test_insert(command):
    client = postgress.PGClass(host, db_name, user, password)
    client.command_execute([command])
# test_connection()
test_insert(command)
# ST_GeomFromGeoJSON()