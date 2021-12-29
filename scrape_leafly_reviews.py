import json
import math
import os
import random
import time
from itertools import chain

import pandas as pd
import requests
from requests.exceptions import RequestException
from tqdm import tqdm, trange

STRAINS_API_URL = (
    "https://consumer-api.leafly.com/api/strain_playlists/v2?&skip={}&take={}"
)
REVIEWS_API_URL = (
    "https://www.leafly.com/web-strain-explorer/api/strains/{}/reviews?take=50&page={}"
)
NUM_STRAINS = 5867


def get_strain_metadata():
    if os.path.exists("strains_metadata.json"):
        # Load existing data dump
        with open("strains_metadata.json", "r") as f:
            strain_list = json.load(f)
    else:
        strains_raw = []
        for skip in trange(0, NUM_STRAINS, 50):
            r = requests.get(STRAINS_API_URL.format(skip, 50))
            data = json.loads(r.content)
            strains_raw.append(data)
        batches = [batch["hits"]["strain"] for batch in strains_raw]
        strain_list = list(chain.from_iterable(batches))
        with open("strains_metadata.json", "w") as f:
            json.dump(strain_list, f)
    return strain_list


if __name__ == "__main__":
    # Make a dataframe of strain metadata
    strain_list = get_strain_metadata()
    strains = pd.DataFrame()
    for col in ["id", "category", "name", "reviewCount", "slug"]:
        strains[col] = [s[col] for s in strain_list]
    strains["url"] = "https://leafly.com/strain" + strains["slug"]
    for chem in ["cbc", "cbd", "cbg", "thc", "thcv"]:
        strains[f"{chem}_p50"] = [
            s["cannabinoids"][chem]["percentile50"] for s in strain_list
        ]
    strains = strains.rename(columns={"id": "strain_id", "reviewCount": "review_count"})
    strains = strains.set_index("strain_id")

    # Make a dataframe of review data
    reviews = []
    for sid in tqdm(strains.sort_values(by=["review_count"], ascending=False).index):
        reviews_raw = []
        n_reviews = strains.at[sid, "review_count"]
        n_pages = math.ceil(n_reviews / 50)
        for page in trange(1, n_pages + 1):
            reviews_uri = REVIEWS_API_URL.format(strains.at[sid, "slug"], page)
            r = requests.get(reviews_uri)
            if not r.ok:
                raise RequestException
            reviews_raw.append(json.loads(r.content)["data"])
            time.sleep(random.gauss(3, 1))
        # Why not always as many elements as n_reviews?
        review_list = list(chain.from_iterable(reviews_raw))
        df = pd.DataFrame()
        for col in [
            "id",
            "username",
            "created",
            "form",
            "language",
            "rating",
            "upvotesCount",
            "text",
        ]:
            df[col] = [r[col] for r in review_list]
        df["strain_id"] = [
            strains.loc[strains["slug"] == r["strainSlug"], :].index[0]
            for r in review_list
        ]
        df = df.rename(columns={"id": "review_id", "upvotesCount": "upvotes"})
        reviews.append(df)
        # Checkpoint every 10 strains
        if len(reviews) % 10 == 0:
            with open("reviews_data.json", "w") as f:
                json.dump(reviews, f)
