import requests

from chalicelib.settings import SENSORS_AFRICA_API, SENSORS_AFRICA_AUTH_TOKEN

def post_node(node):
    response = requests.post(f"{SENSORS_AFRICA_API}/v2/nodes/",
    data=node,
    headers={"Authorization": f"Token {SENSORS_AFRICA_AUTH_TOKEN}"})
    if response.ok and "id" in response.json():
        return response.json()['id']

def post_location(location):
    response = requests.post(f"{SENSORS_AFRICA_API}/v2/locations/",
    data=location,
    headers={"Authorization": f"Token {SENSORS_AFRICA_AUTH_TOKEN}"})
    if response.ok and "id" in response.json():
        return response.json()['id']
    return None

def post_sensor(sensor):
    response = requests.post(f"{SENSORS_AFRICA_API}/v2/sensors/",
    data=sensor,
    headers={"Authorization": f"Token {SENSORS_AFRICA_AUTH_TOKEN}"})
    if response.ok and "id" in response.json():
        return response.json()['id']
    return None

def post_sensor_type(sensor_type):
    response = requests.post(f"{SENSORS_AFRICA_API}/v2/sensor-types/",
    json=sensor_type,
    headers={"Authorization": f"Token {SENSORS_AFRICA_AUTH_TOKEN}"})
    if response.ok and "id" in response.json():
        return response.json()['id']
    return None

def post_sensor_data(data, node_uid, pin):
    response = requests.post(f"{SENSORS_AFRICA_API}/v1/push-sensor-data/",
    json=data,
    headers={
        "Authorization": f"Token {SENSORS_AFRICA_AUTH_TOKEN}",
        "SENSOR": str(node_uid),
        "PIN": pin
        }
    )
    if response.ok:
        return response.json()
    return []

def get_sensors_africa_sensor_types():
    response = requests.get(f"{SENSORS_AFRICA_API}/v2/sensor-types/",
    headers={"Authorization": f"Token {SENSORS_AFRICA_AUTH_TOKEN}"})
    if response.ok:
        return response.json()
    return []
    
def get_sensors_africa_sensors():
    response = requests.get(f"{SENSORS_AFRICA_API}/v2/sensors/",
    headers={"Authorization": f"Token {SENSORS_AFRICA_AUTH_TOKEN}"})
    if response.ok:
        return response.json()
    return []
    
def get_sensors_africa_nodes():
    response = requests.get(f"{SENSORS_AFRICA_API}/v1/node/",
    headers={"Authorization": f"Token {SENSORS_AFRICA_AUTH_TOKEN}"})
    if response.ok:
        return response.json()
    return []

def get_sensors_africa_locations():
    response = requests.get(f"{SENSORS_AFRICA_API}/v2/locations/", 
    headers={"Authorization": f"Token {SENSORS_AFRICA_AUTH_TOKEN}"})
    if response.ok:
        """
            Using latitude, longitude as a key and location id as value to help us find already existing location latter without having to ping the server
            Using round ensures latitude, longitude value will be the same as lat_log in the run method.
        """
        formated_response = [{f'{round(float(location["latitude"]), 3)}, {round(float(location["longitude"]), 3)}':
                            f'{location["id"]}'} for location in response.json() if location["latitude"] and location["longitude"]]

        return formated_response
    return []
