# Automate geolocation


# Add column to the previous dataframe e.g. ontdoener_location & it"s coordinates in WKT, RD_new
# Validate locations (point in postcode polygon) Spatial_data/Postcodegebied SHP

import pandas as pd
import geopandas as gpd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from shapely import wkt
import logging

# silence geopy logger
logger = logging.getLogger("geopy")
logger.propagate = False

# silence fiona logger
logger = logging.getLogger("fiona")
logger.propagate = False


def geocode(addresses):
    # nominatim geocoding
    # delay between request to assert access to server
    locator = Nominatim(user_agent="CustomGeocoder")
    geocoder = RateLimiter(locator.geocode, min_delay_seconds=1)
    addresses["location"] = addresses["adres"].apply(geocoder)

    # extract location coordinates (WGS84)
    for coord in ["longitude", "latitude"]:
        func = lambda loc: getattr(loc, coord) if loc else None
        addresses[coord] = addresses["location"].apply(func)

    # create geodataframe with postcode & location
    geometry = gpd.points_from_xy(addresses.longitude, addresses.latitude)
    locations = gpd.GeoDataFrame(addresses, geometry=geometry, crs={"init": "epsg:4326"})
    locations = locations.to_crs(epsg=28992)  # specify CRS

    return locations


def add_wkt(locations):
    # divide into locations in or out the country
    # locations without country will be processed as in the country
    is_in = locations['land'].isin(['NEDERLAND', 'NAN'])
    country_in = locations[is_in]
    country_out = locations[~is_in]

    # ------------------------------------------------------
    # IN COUNTRY
    # ------------------------------------------------------

    # import postcode districts
    districts = gpd.read_file("Spatial_data/Postcodegebied_PC4_RDnew.shp")
    districts = districts[["geometry", "PC4"]]
    districts["centroid"] = districts.geometry.centroid

    # convert locations postcodes to 4-digit
    country_in["PC4_loc"] = country_in["postcode"].str[:4]

    # cast district 4-digit postcodes as strings
    districts["PC4"] = districts["PC4"].astype(str)

    # merge locations with districts on geometry
    # point in polygon (with spatial indexes)
    merge_geom = gpd.sjoin(country_in, districts, how="left", op="within")

    # merge locations with districts on postcode
    # preserve original indices from locations
    merge_code = pd.merge(country_in, districts, how="left", left_on="PC4_loc", right_on="PC4")
    merge_code.index = country_in.index  # keep original index

    # check if there is a match on geometry
    # false: no/wrong point from geocoding
    condition = merge_geom["PC4"] != merge_code["PC4"]

    # if no match on geometry, match locations & districts on postcode
    # assign district centroid as location
    country_in.loc[condition, "geometry"] = merge_code["centroid"]

    # ------------------------------------------------------
    # OUT COUNTRY
    # ------------------------------------------------------

    # load countries
    countries = gpd.read_file("Spatial_data/countries.shp")
    countries['country_nl'] = countries['country_nl'].astype(str).str.upper()
    countries = countries[countries['country_nl'] != 'NEDERLAND']
    countries.set_crs(epsg=4326, inplace=True)
    countries.to_crs(epsg=28992, inplace=True)
    countries["centroid"] = districts.geometry.centroid

    # merge locations with countries on geometry
    # point in polygon (with spatial indexes)
    merge_geom = gpd.sjoin(country_out, countries, how="left", op="within")

    # merge by country name (in Dutch)
    merge_code = country_out.merge(countries, how="left", left_on="land", right_on="country_nl")
    merge_code.index = country_out.index  # keep original index

    # check if there is a match on geometry
    # false: no/wrong point from geocoding
    condition = merge_geom["country_nl"] != merge_code["country_nl"]

    # if no match on geometry, match locations & countries on name
    # assign country centroid as location
    country_out.loc[condition, "geometry"] = merge_code["centroid"]

    # merge in & out of country again
    locations = pd.concat([country_in, country_out]).sort_index()

    # convert geometry into WKT
    locations["wkt"] = locations[locations.geometry.notnull()].geometry.apply(lambda x: wkt.dumps(x))

    return locations["wkt"]


def run(addresses):
    # assign locations to addresses
    # geocode with nominatim
    logging.info(f"Start geocoding {len(addresses.index)} addresses...")
    locations = geocode(addresses)

    # update locations with Well-Known Text (WKT)
    locations["wkt"] = add_wkt(locations)

    logging.info("Geocoding complete!")
    return locations["wkt"]