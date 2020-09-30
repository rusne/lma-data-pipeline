# Automate geolocation


# Add column to the previous dataframe e.g. ontdoener_location & it's coordinates in WKT, RD_new
# Validate locations (point in postcode polygon) Spatial_data/Postcodegebied SHP

import pandas as pd
import geopandas as gpd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


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

    geometry = gpd.points_from_xy(addresses.Longitude, addresses.Latitude)
    locations = gpd.GeoDataFrame(addresses['Ontdoener_Postcode'], geometry=geometry)
    locations.set_crs(epsg=4326, inplace=True)  # specify CRS

    return locations


if __name__ == "__main__":
    # import testing dataset as pandas dataframe
    dataset = pd.read_excel("Testing_data/3_cleaned_dataset.xlsx")

    # collect address-related columns
    addresses = get_addresses(dataset)

    # geocode with nominatim
    print('Geocoding...')
    locations = geocode(addresses)

    # import postcode districts
    districts = gpd.read_file("Spatial_data/Postcodegebied_PC4_WGS84.shp")
    districts['Centroids'] = districts.geometry.centroid

    print('Point in polygon...')
    # point in polygon (with spatial indexes)
    merging = gpd.sjoin(locations, districts, how="left", op="within")

    # check if location & district code match
    # false: no/wrong point from geocoding
    condition = merging['Ontdoener_Postcode'].str.contains("|".join(merging['PC4'].astype(str)))
    print(condition)

