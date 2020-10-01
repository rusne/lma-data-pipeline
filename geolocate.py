# Automate geolocation


# Add column to the previous dataframe e.g. ontdoener_location & it's coordinates in WKT, RD_new
# Validate locations (point in postcode polygon) Spatial_data/Postcodegebied SHP

import pandas as pd
import geopandas as gpd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from shapely import wkt


def get_addresses(dataset):
    """
    Extract address-related columns for geocoding
    :param dataset: dataset pandas dataframe
    :return: address pandas dataframe
    """
    # address-related column names
    names = [
        'Straat',  # street
        'Huisnr',  # house number
        'Postcode',  # post code
        'Plaats',  # city
        'Land',  # country
    ]
    cols = ["_".join(["Ontdoener", name]) for name in names]

    # extract columns & cast as strings
    addresses = dataset.loc[:, cols].astype(str)

    # concatenate to full address
    addresses['Address'] = addresses[cols[0]].str.cat(addresses[cols[1:]], sep=', ')
    # addresses = addresses.loc[:5]

    return addresses


def geocode(addresses):
    """
    Find locations by address with nominatim service
    :param addresses: address pandas dataframe
    :return: address pandas dataframe with geographic coordinates
    """
    # nominatim geocoding
    # delay between request to assert access to server
    locator = Nominatim(user_agent="CustomGeocoder")
    geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
    addresses['Location'] = addresses['Address'].apply(geocode)

    # extract location coordinates (WGS84)
    for coord in ['longitude', 'latitude']:
        func = lambda loc: getattr(loc, coord) if loc else None
        addresses[coord.capitalize()] = addresses['Location'].apply(func)

    # create geodataframe with postcode & location
    geometry = gpd.points_from_xy(addresses.Longitude, addresses.Latitude)
    locations = gpd.GeoDataFrame(addresses['Ontdoener_Postcode'], geometry=geometry)
    locations = locations.set_crs(epsg=4326, inplace=True)  # specify CRS

    return locations


def add_locations(locations):
    """
    Add locations to dataset in WKT format
    :param locations: location geodataframe
    :return: WKT geometry
    """
    # import postcode districts
    districts = gpd.read_file("Spatial_data/Postcodegebied_PC4_WGS84.shp")
    districts['Centroid'] = districts.geometry.centroid

    # cast district 4-digit postcodes as strings
    districts['PC4'] = districts['PC4'].astype(str)

    # convert locations postcodes to 4-digit
    locations['PC4_loc'] = locations['Ontdoener_Postcode'].str[:4]

    # merge locations & districts on geometry
    # point in polygon (with spatial indexes)
    merge_geom = gpd.sjoin(locations, districts, how="left", op="within")

    # merge locations & districts on postcode
    merge_code = pd.merge(locations, districts, how="left", left_on='PC4_loc', right_on='PC4')
    print(merge_code)

    # check if there is a match on geometry
    # false: no/wrong point from geocoding
    condition = merge_geom['PC4_loc'] == merge_geom['PC4']

    # if no match on geometry, match locations & districts on postcode
    # assign district centroid as location
    merge_geom.loc[condition, 'geometry'] = merge_code['Centroid']

    # convert geometry into WKT
    merge_geom['WKT'] = merge_geom.geometry.apply(lambda x: wkt.dumps(x))

    return merge_geom['WKT']


if __name__ == "__main__":
    # import testing dataset as pandas dataframe
    dataset = pd.read_excel("Testing_data/3_cleaned_dataset.xlsx")

    # collect address-related columns
    addresses = get_addresses(dataset)

    # geocode with nominatim
    print('Start geocoding...')
    locations = geocode(addresses)
    print('Geocoding complete!')

    # update dataset with locations
    dataset['Ontdoener_Location'] = add_locations(locations)