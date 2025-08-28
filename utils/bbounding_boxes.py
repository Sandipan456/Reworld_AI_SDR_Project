import math

# Earth's radius in miles
EARTH_RADIUS_MILES = 3958.8

def miles_to_degrees_lat(miles):
    return miles / 69.0  # 1 degree latitude ≈ 69 miles

def miles_to_degrees_lon(miles, latitude):
    return miles / (math.cos(math.radians(latitude)) * 69.172)  # adjusted for latitude

# 1. Create a bounding box of 300 miles radius
def create_bounding_box_300_miles(lat, lon):
    lat_delta = miles_to_degrees_lat(300)
    lon_delta = miles_to_degrees_lon(300, lat)

    return {
        "minLat": lat - lat_delta,
        "maxLat": lat + lat_delta,
        "minLon": lon - lon_delta,
        "maxLon": lon + lon_delta
    }

# 2. Generate smaller bounding boxes (25 miles) from a larger box
def generate_25_mile_bounding_boxes(bbox):
    lat_step = miles_to_degrees_lat(25)
    lon_step = miles_to_degrees_lon(25, (bbox["minLat"] + bbox["maxLat"]) / 2)

    boxes = []
    lat = bbox["minLat"]
    while lat < bbox["maxLat"]:
        next_lat = min(lat + lat_step, bbox["maxLat"])
        lon = bbox["minLon"]
        while lon < bbox["maxLon"]:
            next_lon = min(lon + lon_step, bbox["maxLon"])
            boxes.append({
                "minLat": lat,
                "maxLat": next_lat,
                "minLon": lon,
                "maxLon": next_lon
            })
            lon = next_lon
        lat = next_lat

    return boxes
