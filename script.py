import httpx as http
from dotenv import load_dotenv, find_dotenv
import os
import pandas as pd
import numpy as np

load_dotenv(find_dotenv())

REGIONS_BASE_URL = "https://restcountries-v1.p.rapidapi.com/"
REGIONS_BASE_HEADERS = {
    "x-rapidapi-key": os.getenv("RAPID_API"),
    "x-rapidapi-host": os.getenv("RAPID_API_HOST")
}

COUNTRY_BASE_URL = "https://restcountries.eu/rest/v2/region/"


def get_regions():
    """
    :return:
    :rtype:
    """
    response = http.get(REGIONS_BASE_URL + "all", headers=REGIONS_BASE_HEADERS)
    response = response.json()
    data = {
        "Region": [],
        "City Name": [],
        "Language": [],
        "Time": []
    }
    for country in response:
        if country["region"]:
            data["Region"].append(country["region"])
            response = http.get(
                COUNTRY_BASE_URL + country["region"].lower()
            ).json()

    return


print(get_regions())
