"""
Zinobe test
"""
import httpx
import httpx as http
from dotenv import load_dotenv, find_dotenv
import os
import pandas as pd
import numpy as np
import time
import hashlib
import sqlite3
from tqdm.auto import tqdm

# Dotenv loader
load_dotenv(find_dotenv())

# Global variables
REGIONS_BASE_URL = "https://restcountries-v1.p.rapidapi.com/"
REGIONS_BASE_HEADERS = {
    "x-rapidapi-key": os.getenv("RAPID_API"),
    "x-rapidapi-host": os.getenv("RAPID_API_HOST")
}

COUNTRY_BASE_URL = "https://restcountries.eu/rest/v2/region/"


class Challenge:
    """
    Main Class of the challenge
    """
    def __init__(self):
        """
        Initializer method
        """
        # Data Init
        self.data = self.init_data()
        # Connection to SQLite Database
        self.conn = sqlite3.connect(os.getenv("SQLITE_DB", "example.db"))
        # Operation cursor
        self.cursor = self.conn.cursor()
        self.init_db()

    def init_db(self):
        """
        Database initializer, this method creates all the needed tables.
        """
        try:
            self.cursor.execute("""CREATE TABLE countries(
                id INTEGER PRIMARY KEY,
                region TEXT,
                city_name TEXT,
                language TEXT,
                time FLOAT
            )""")
        except Exception as e:
            print(str(e))

    @staticmethod
    def get_regions():
        """
        Method to obtain all the regions from
        RapidAPI
        :return: The list-converted keys belonging
        to the retrieved regions
        :rtype: list
        """
        response = httpx.get(
            REGIONS_BASE_URL + "all",
            headers=REGIONS_BASE_HEADERS
        ).json()
        regions = {}
        for country in response:
            if country["region"]:
                regions[country["region"]] = 1
        return list(regions.keys())

    def init_data(self):
        """
        Method to obtain the table data
        :return: A Dataframe with al the required data
        :rtype: pd.DataFrame
        """
        data = {
            "Region": [],
            "City Name": [],
            "Language": [],
            "Time": []
        }
        # Region Getter
        regions = self.get_regions()
        for region in tqdm(regions):
            start_time = time.time()
            data["Region"].append(region)

            # Request of all data
            response = http.get(
                COUNTRY_BASE_URL + region.lower()
            ).json()

            # Random country pick
            random_country_index = np.random.randint(0, len(response))
            country = response[random_country_index]
            data["City Name"].append(country["name"])

            # Random language pick
            random_language_index = np.random.randint(0, len(country["languages"]))
            language = country["languages"][random_language_index]

            # SHA-1 Encryption
            data["Language"].append(
                hashlib.sha1(
                    language["name"].encode("utf-8")
                ).hexdigest()
            )
            # Time calculation
            end_time = time.time()
            data["Time"].append(np.round(end_time - start_time, 2))
        return pd.DataFrame(data)

    def time_statistics(self):
        """
        Statistics over the retrieving time
        :return: The aggregation methods for total, mean,
        min and max description
        """
        return self.data.agg({
            "Time": ["sum", "mean", "min", "max"]
        })

    def insert_data(self):
        """
        Data Insertion from the dataframe table to the SQL
        table
        """
        for index, row in self.data.iterrows():
            try:
                # SQL declaration
                self.cursor.execute(f"""INSERT INTO countries(
                    id,  region, city_name, 
                    language, time
                ) VALUES (
                    {index},
                    '{row["Region"]}',
                    '{row["City Name"]}',
                    '{row["Language"]}',
                    {row["Time"]}
                )""")
            except Exception as e:
                print(str(e))
        self.conn.commit()

    def save_json(self):
        """
        JSON saver method
        """
        self.data.to_json("data.json", orient="split", index=False)

    def finish(self):
        """
        Drops the table
        :return:
        :rtype:
        """
        self.cursor.execute("DROP TABLE countries")
        self.conn.commit()


challenge = Challenge()
print("Initiated")
print(challenge.data)
print("Data")
print(challenge.time_statistics())
print("Statistics")
challenge.insert_data()
print("Data inserted")
challenge.save_json()
print("JSON Generated")
challenge.finish()
print("Table deleted")
