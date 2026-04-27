import requests
import pandas as pd
import logging
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)

GDELT_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"


def get_latest_export_url():
    response = requests.get(GDELT_URL)
    lines = response.text.split("\n")
    for line in lines:
        if "export.CSV.zip" in line:
            return line.split(" ")[2]
    raise Exception("No export URL found")


def extract_domain(url):
    try:
        return urlparse(url).netloc
    except:
        return None


def fetch_gdelt_sample():
    url = get_latest_export_url()
    logging.info(f"Downloading: {url}")

    df = pd.read_csv(url, sep="\t", header=None, low_memory=False)

    logging.info(f"Raw shape: {df.shape}")

    # Full GDELT schema (important)
    full_columns = [
        "GLOBALEVENTID","SQLDATE","MonthYear","Year","FractionDate",
        "Actor1Code","Actor1Name","Actor1CountryCode","Actor1KnownGroupCode",
        "Actor1EthnicCode","Actor1Religion1Code","Actor1Religion2Code",
        "Actor1Type1Code","Actor1Type2Code","Actor1Type3Code",
        "Actor2Code","Actor2Name","Actor2CountryCode","Actor2KnownGroupCode",
        "Actor2EthnicCode","Actor2Religion1Code","Actor2Religion2Code",
        "Actor2Type1Code","Actor2Type2Code","Actor2Type3Code",
        "IsRootEvent","EventCode","EventBaseCode","EventRootCode",
        "QuadClass","GoldsteinScale","NumMentions","NumSources",
        "NumArticles","AvgTone",
        "Actor1Geo_Type","Actor1Geo_FullName","Actor1Geo_CountryCode",
        "Actor1Geo_ADM1Code","Actor1Geo_ADM2Code","Actor1Geo_Lat","Actor1Geo_Long",
        "Actor1Geo_FeatureID",
        "Actor2Geo_Type","Actor2Geo_FullName","Actor2Geo_CountryCode",
        "Actor2Geo_ADM1Code","Actor2Geo_ADM2Code","Actor2Geo_Lat","Actor2Geo_Long",
        "Actor2Geo_FeatureID",
        "ActionGeo_Type","ActionGeo_FullName","ActionGeo_CountryCode",
        "ActionGeo_ADM1Code","ActionGeo_ADM2Code","ActionGeo_Lat","ActionGeo_Long",
        "ActionGeo_FeatureID",
        "DATEADDED","SOURCEURL"
    ]

    # Adjust in case of mismatch (robust handling)
    df = df.iloc[:, :len(full_columns)]
    df.columns = full_columns

    # Convert timestamp
    df["event_timestamp"] = pd.to_datetime(
        df["DATEADDED"], format="%Y%m%d%H%M%S", errors="coerce"
    )

    # Extract domain
    df["source_domain"] = df["SOURCEURL"].apply(extract_domain)

    # Select only useful columns (still RAW, just trimmed)
    df = df[[
        "GLOBALEVENTID",
        "event_timestamp",
        "SQLDATE",
        "EventCode",
        "EventBaseCode",
        "EventRootCode",
        "QuadClass",
        "GoldsteinScale",
        "NumMentions",
        "NumSources",
        "NumArticles",
        "AvgTone",
        "Actor1Name",
        "Actor1CountryCode",
        "Actor2Name",
        "Actor2CountryCode",
        "ActionGeo_FullName",
        "ActionGeo_CountryCode",
        "ActionGeo_Lat",
        "ActionGeo_Long",
        "source_domain",
        "SOURCEURL"
    ]]

    logging.info(f"Final shape: {df.shape}")

    return df


if __name__ == "__main__":
    df = fetch_gdelt_sample()

    # Save sample for exploration only
    df.head(1000).to_csv("gdelt_sample.csv", index=False)

    print(df.head())