import boto3
import json
import pickle
import requests

from chalicelib.sensorafrica import (
    get_sensors_africa_locations,
    get_sensors_africa_nodes,
    get_sensors_africa_sensor_types,
    get_sensors_africa_sensors,
    post_location, 
    post_node,
    post_node,
    post_sensor,
    post_sensor_data,
    post_sensor_type, )

from chalicelib.settings import S3_BUCKET_NAME, S3_CHANNEL_START_KEY, S3_OBJECT_KEY, OWNER_ID
from chalicelib.utils import address_converter

from time import localtime, sleep, strftime

def get_airqo_node_sensors_data(node_id):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"}
    response = requests.get(url="https://thingspeak.com/channels/{}/feeds.json".format(node_id), headers=headers)
    if not response.ok:
        raise Exception(response.reason)
    return response.json()


def history(app):
    #function assumes node & sensors already exists
    nodes = get_sensors_africa_nodes()
    sensors = get_sensors_africa_sensors()


def run(app):
    locations = get_sensors_africa_locations()
    nodes = get_sensors_africa_nodes()
    sensors = get_sensors_africa_sensors()
    sensor_types = get_sensors_africa_sensor_types()


    s3client = boto3.client("s3", region_name="eu-west-1")
    try:
        response = s3client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_OBJECT_KEY)
        body = response['Body'].read()
        channel_last_entry_dict = pickle.loads(body)
    except:
        channel_last_entry_dict = dict()

    try:
        res = s3client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_CHANNEL_START_KEY)
        channel_start_index = pickle.loads(res['Body'].read())
    except:
        channel_start_index = { "start": 0 }
   
    with open("chalicelib/channels.json") as data:
        channels = json.load(data)

        sliced_channels = channels[channel_start_index["start"] : channel_start_index["start"] + 10]

        for channel in sliced_channels:
            
            channel_data = get_airqo_node_sensors_data(channel["id"])
            #if channel id key does not exist in the map dict initiate it with 0
            if not channel["id"] in channel_last_entry_dict:
                channel_last_entry_dict[channel["id"]] = 0

            last_entry = channel_last_entry_dict[channel["id"]]

            if channel_data and channel_data["channel"]["last_entry_id"] > last_entry:
                lat_log = f'{channel["latitude"]}, {channel["longitude"]}'
                address = address_converter(lat_log)
                
                location = [loc.get(lat_log) for loc in locations if loc.get(lat_log)]
                if len(location) > 0:
                    location = location[0]
                else:
                    location = post_location({
                        "location": address.get("display_name"),
                        "latitude": channel["latitude"],
                        "longitude": channel["longitude"],
                        "country": address.get("country"),
                        "postalcode": address.get("postcode")
                    })

                #post node objects if it does not exist
                airqo_node = [node.get("id") for node in nodes if node.get('uid') == str(channel["id"])]
                if len(airqo_node) > 0:
                    airqo_node = airqo_node[0]
                else:
                    airqo_node = post_node(node={"uid": channel["id"], 'owner': int(OWNER_ID), 'location': location})

                sensor_type = [s_type.get("id") for s_type in sensor_types if str(s_type.get("uid")).lower() == "pms5003"]
                if len(sensor_type) > 0:
                    sensor_type = sensor_type[0]
                else:
                    sensor_type = post_sensor_type({ "uid": "pms5003","name": "PMS5003","manufacturer": "PlanTower" })
  
                # aiqo channel result has 4 field data that we need from 2 different sensors
                # field1- Sensor1 PM2.5_CF_1_ug/m3, 
                # field2 -Sensor1 PM10_CF_1_ug/m3, 
                # field3 - Sensor2PM2.5_CF_1_ug/m3, 
                # field4 - Sensor2 PM10_CF_1_ug/m3
                #So we will create 2 sensors for each node

                sensor_1_id = [sen.get("id") for sen in sensors if sen.get("node") == airqo_node and sen.get("pin") == "1" and sen.get("sensor_type") == sensor_type]
                if len(sensor_1_id) > 0:
                    sensor_1_id = sensor_1_id[0]
                else:
                    sensor_1_id = post_sensor({
                        "node": airqo_node,
                        "pin": "1",
                        "sensor_type": sensor_type,
                        "public": False
                    })

                sensor_2_id = [sen.get("id") for sen in sensors if sen.get("node") == airqo_node and sen.get("pin") == "3" and sen.get("sensor_type") == sensor_type]

                if len(sensor_2_id) > 0:
                    sensor_2_id = sensor_2_id[0]
                else:
                    sensor_2_id = post_sensor({
                        "node": airqo_node,
                        "pin": "3",
                        "sensor_type": sensor_type,
                        "public": False
                    })

                #loop through feed and post data values                    
                for feed in channel_data["feeds"]:
                    if feed["entry_id"] > last_entry:
                        sensor_1_data_values = [
                            {
                                "value": float(feed["field1"]),
                                "value_type": "P2"
                            },
                            {
                                "value": float(feed["field2"]),
                                "value_type": "P1"
                            }
                        ]

                        sensor_2_data_values = [
                            {
                                "value": float(feed["field3"]),
                                "value_type": "P2"
                            },
                            {
                                "value": float(feed["field4"]),
                                "value_type": "P1"
                            }
                        ]

                        post_sensor_data({ 
                            "sensordatavalues": sensor_1_data_values, 
                            "timestamp": feed["created_at"]
                            }, channel["id"], "1")

                        post_sensor_data({ 
                            "sensordatavalues": sensor_2_data_values, 
                            "timestamp": feed["created_at"]
                            }, channel["id"], "3")
                
                #update pickle variable               
                channel_last_entry_dict[channel["id"]] = channel_data["channel"]["last_entry_id"]
                s3client.put_object(Body=pickle.dumps(channel_last_entry_dict), Bucket=S3_BUCKET_NAME, Key=S3_OBJECT_KEY)
            else:
                app.log.warn("Channel feed - %s missing or not updated" % channel["id"])
                
            sleep(5)

        channel_start_index = { "start": channel_start_index["start"] + 10 if channel_start_index["start"] + 10 < len(channels) else 0}
        s3client.put_object(Body=pickle.dumps(channel_start_index), Bucket=S3_BUCKET_NAME, Key=S3_CHANNEL_START_KEY)

        return {
            "Last Updated": strftime("%a, %d %b %Y %H:%M:%S", localtime()),
            "Channels Updated": [{"id": c["id"], "name": c["name"] } for c in sliced_channels]
        }
