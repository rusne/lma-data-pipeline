# Automate geolocation


# Add column to the previous dataframe e.g. ontdoener_location & it's coordinates in WKT, RD_new
# Validate locations (point in postcode polygon) Spatial_data/Postcodegebied SHP

import pandas as pd
import geopandas as gpd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from shapely import wkt
import logging


def get_addresses(dataframe, role):
    # address-related column names
    names = [
        'Straat',  # street
        'Huisnr',  # house number
        'Postcode',  # post code
        'Plaats',  # city
    ]
    cols = ["_".join([role, name]) for name in names]

    # extract columns & cast as strings
    addresses = dataframe[cols].astype(str)

    # concatenate to full address
    addresses['Address'] = addresses[cols[0]].str.cat(addresses[cols[1:]], sep=', ')

    return addresses


def geocode(addresses, role):
    # nominatim geocoding
    # delay between request to assert access to server
    locator = Nominatim(user_agent="CustomGeocoder")
    geocoder = RateLimiter(locator.geocode, min_delay_seconds=1)
    addresses['Location'] = addresses['Address'].apply(geocoder)

    # extract location coordinates (WGS84)
    for coord in ['longitude', 'latitude']:
        func = lambda loc: getattr(loc, coord) if loc else None
        addresses[coord.capitalize()] = addresses['Location'].apply(func)

    # create geodataframe with postcode & location
    geometry = gpd.points_from_xy(addresses.Longitude, addresses.Latitude)
    locations = gpd.GeoDataFrame(addresses[role + '_Postcode'], geometry=geometry)
    locations = locations.set_crs(epsg=4326, inplace=True)  # specify CRS

    return locations


def add_locations(locations, role):
    # import postcode districts
    districts = gpd.read_file("Spatial_data/Postcodegebied_PC4_WGS84.shp")
    districts = districts[['geometry', 'PC4']]
    districts['Centroid'] = districts.geometry.centroid

    # convert locations postcodes to 4-digit
    locations['PC4_loc'] = locations[role + '_Postcode'].str[:4]

    # cast district 4-digit postcodes as strings
    districts['PC4'] = districts['PC4'].astype(str)

    # merge locations with districts on geometry
    # point in polygon (with spatial indexes)
    merge_geom = gpd.sjoin(locations, districts, how="left", op="within")

    # merge locations with districts on postcode
    # preserve original indices from locations
    merge_code = locations.merge(districts, how='left', left_on='PC4_loc', right_on='PC4')
    merge_code = merge_code.set_axis(locations.index)

    # check if there is a match on geometry
    # false: no/wrong point from geocoding
    condition = merge_geom['PC4'] != merge_code['PC4']

    # if no match on geometry, match locations & districts on postcode
    # assign district centroid as location
    merge_geom.loc[condition, 'geometry'] = merge_code['Centroid']

    # convert geometry into WKT
    merge_geom['WKT'] = merge_geom.geometry.apply(lambda x: wkt.dumps(x))

    return merge_geom['WKT']


def run(dataframe, roles):
    # silence geopy logger
    logger = logging.getLogger('geopy')
    logger.propagate = False

    # silence fiona logger
    logger = logging.getLogger('fiona')
    logger.propagate = False

    logging.info('Start geocoding...')
    # assign locations to companies
    for role in roles:
        # collect address-related columns
        addresses = get_addresses(dataframe, role)

        # geocode with nominatim
        locations = geocode(addresses, role)

        # update dataset with locations
        dataframe[role + '_Location'] = add_locations(locations, role)

    logging.info('Geocoding complete!')
    return dataframe