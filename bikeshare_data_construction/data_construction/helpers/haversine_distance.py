from math import sin, cos, sqrt, atan2, radians

def haversine_distance(lat_1, lon_1, lat_2, lon_2):
    lat_1 = radians(lat_1)
    lon_1 = radians(lon_1)
    lat_2 = radians(lat_2)
    lon_2 = radians(lon_2)

    # approximate radius of earth in km
    R = 6373.0

    d_lon = lon_2 - lon_1
    d_lat = lat_2 - lat_1

    a = sin(d_lat / 2)**2 + cos(lat_1) * cos(lat_2) * sin(d_lon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance