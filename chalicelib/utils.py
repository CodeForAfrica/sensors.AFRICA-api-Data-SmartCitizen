from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="sensors-api-airqo")

def address_converter(lat_long):
    try:
        location = geolocator.reverse(lat_long)
        location.raw["address"].update({"display_name": location.address})
        return location.raw["address"]
    except:
        return {}
        