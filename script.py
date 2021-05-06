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

load_dotenv(find_dotenv())

REGIONS_BASE_URL = "https://restcountries-v1.p.rapidapi.com/"
REGIONS_BASE_HEADERS = {
    "x-rapidapi-key": os.getenv("RAPID_API"),
    "x-rapidapi-host": os.getenv("RAPID_API_HOST")
}

COUNTRY_BASE_URL = "https://restcountries.eu/rest/v2/region/"


class Challenge:
    """

    """
    def __init__(self):
        """

        """
        self.data = self.init_data()
        self.conn = sqlite3.connect(os.getenv("SQLITE_DB", "example.db"))
        self.cursor = self.conn.cursor()
        self.init_db()

    def init_db(self):
        """

        :return:
        :rtype:
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

        :return:
        :rtype:
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

        :return:
        :rtype:
        """
        data = {
            "Region": [],
            "City Name": [],
            "Language": [],
            "Time": []
        }
        regions = self.get_regions()
        for region in tqdm(regions):
            start_time = time.time()
            data["Region"].append(region)
            response = http.get(
                COUNTRY_BASE_URL + region.lower()
            ).json()
            random_country_index = np.random.randint(0, len(response))
            country = response[random_country_index]
            data["City Name"].append(country["name"])
            random_language_index = np.random.randint(0, len(country["languages"]))
            language = country["languages"][random_language_index]
            data["Language"].append(
                hashlib.sha1(
                    language["name"].encode("utf-8")
                ).hexdigest()
            )
            end_time = time.time()
            data["Time"].append(np.round(end_time - start_time, 2))
        return pd.DataFrame(data)

    def time_statistics(self):
        """

        :return:
        :rtype:
        """
        return self.data.agg({
            "Time": ["sum", "mean", "min", "max"]
        })

    def insert_data(self):
        """

        :return:
        :rtype:
        """
        for index, row in self.data.iterrows():
            try:
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
        return

    def save_json(self):
        """

        :return:
        :rtype:
        """
        self.data.to_json("data.json", orient="split", index=False)

    def finish(self):
        """

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
challenge.finish()
print("Table deleted")
challenge.save_json()
print("JSON Generated")
