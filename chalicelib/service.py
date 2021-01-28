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

from chalicelib.settings import S3_BUCKET_NAME, S3_OBJECT_KEY,SMART_CITIZEN_AUTH_TOKEN, OWNER_ID
from chalicelib.utils import address_converter

from time import localtime, sleep, strftime
from datetime import datetime as dt

def get_device_data(device_id):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
        "Authorization": f"Bearer {SMART_CITIZEN_AUTH_TOKEN}"}
    response = requests.get(url="https://api.smartcitizen.me/v0/devices/{}".format(device_id), headers=headers)
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
        device_last_entry_dict = pickle.loads(body)
    except:
        device_last_entry_dict = dict()
   
    with open("chalicelib/devices.json") as data:
        devices = json.load(data)

        for d in devices:
            device = get_device_data(d)

            if not device["id"] in device_last_entry_dict:
                device_last_entry_dict[device["id"]] = "2000-01-01T00:00:00Z"

            last_entry = device_last_entry_dict[device["id"]]

            if device and dt.strptime(device["updated_at"], "%Y-%m-%dT%H:%M:%SZ") > dt.strptime(last_entry, "%Y-%m-%dT%H:%M:%SZ"):
                device_location = device["data"]["location"]
                lat_log = f"{round(float(device_location['latitude']), 6)}, {round(float(device_location['longitude']), 6)}"
                address = address_converter(lat_log)
                
                location = [loc.get(lat_log) for loc in locations if loc.get(lat_log)]
                if len(location) > 0:
                    location = location[0]
                else:
                    location = post_location({
                        "location": address.get("display_name", "{} - SC{}".format(device_location["city"], device["id"])),
                        "latitude": round(float(device_location['latitude']), 6),
                        "longitude": round(float(device_location['longitude']), 6),
                        "country": device_location["country"],
                        "city": device_location["city"]
                    })

                #post node objects if it does not exist
                smart_citizen__node = [node.get("id") for node in nodes if node.get('uid') == "sc_n{}".format(device["id"])]
                if len(smart_citizen__node) > 0:
                    smart_citizen__node = smart_citizen__node[0]
                else:
                    smart_citizen__node = post_node(node={
                        "uid": "sc_n{}".format(device["id"]),
                        "description": device.get("description", ""),
                        "inactive": "offline" in device["system_tags"],
                        "indoor": "indoor" in device["system_tags"],
                        "name": device["name"],
                        "owner": int(OWNER_ID),
                        "location": location
                        })
                #We are interested with temperature, humidity & particle matter readings
                #These have measurement ids in the API of 1 - temp, 2 - humidity, 
                #14 - PM 2.5, 13 - PM 10 & 27 - PM 1

                #we will filter sensor reading having those measurements
                device_sensors = [ dev for dev in device["data"]["sensors"] if dev.get("measurement_id") in [1, 2, 13, 14, 27]]

                unique_sensor_values_map = {}
                for dev_sensor in device_sensors:
                    pin = dev_sensor["ancestry"]
                    if pin is None:
                        pin = str(dev_sensor["id"])
                    unique_sensor_values_map[pin] = []

                for sensor in device_sensors:
                    sensor_type = [s_type.get("id") for s_type in sensor_types if str(s_type.get("uid")).lower() in sensor["name"].lower()]
                    if len(sensor_type) > 0:
                        sensor_type = sensor_type[0]
                    else:
                        sensor_type = post_sensor_type(
                            { 
                                "uid": sensor["name"].split("-")[0],
                                "name": "SmartCitizen - {}".format(sensor["name"].split("-")[0]),
                                "manufacturer": "SmartCitizen" 
                            })

                    pin = sensor["ancestry"]
                    if pin is None:
                        pin = str(sensor["id"])
                    sensor_id = [sen.get("id") for sen in sensors 
                                    if sen.get("node") == smart_citizen__node and 
                                    (sen.get("pin") == pin) and 
                                    sen.get("sensor_type") == sensor_type]
                    if len(sensor_id) > 0:
                        sensor_id = sensor_id[0]
                    else:
                        sensor_id = post_sensor({
                            "node": smart_citizen__node,
                            "pin":  pin,
                            "sensor_type": sensor_type,
                            "public": False
                        })

                    if sensor["measurement_id"] == 1:
                        value_type = "temperature" 
                    elif sensor["measurement_id"] == 2:
                        value_type = "humidity"
                    elif sensor["measurement_id"] == 13:
                        value_type = "P1"
                    elif sensor["measurement_id"] == 14:
                        value_type = "P2"
                    else:
                        value_type = "P0" 

                    unique_sensor_values_map[pin].append(
                        {
                            "value": sensor["value"],
                            "value_type": value_type
                        }
                    )

                for key in unique_sensor_values_map:
                    post_sensor_data({ 
                        "sensordatavalues": unique_sensor_values_map[key], 
                        "timestamp": device["data"]["recorded_at"]
                        }, "sc_n{}".format(device["id"]), key)
                
                #update pickle variable               
                device_last_entry_dict[device["id"]] = device["updated_at"]
                s3client.put_object(Body=pickle.dumps(device_last_entry_dict), Bucket=S3_BUCKET_NAME, Key=S3_OBJECT_KEY)
            else:
                app.log.warn("device feed - %s missing or not updated" % device["id"])
                
            sleep(5)
        return {}