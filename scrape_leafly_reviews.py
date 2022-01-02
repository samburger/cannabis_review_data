import json
import logging as log
import math
import os
import random
import time
from itertools import chain

import pandas as pd
import requests
from fp.fp import FreeProxy
from tqdm import tqdm, trange

log.basicConfig(level=log.INFO)

STRAINS_API_URL = (
    "https://consumer-api.leafly.com/api/strain_playlists/v2?&skip={}&take={}"
)
REVIEWS_API_URL = (
    "https://www.leafly.com/web-strain-explorer/api/strains/{}/reviews?take=50&page={}"
)
NUM_STRAINS = 5867

_proxy = FreeProxy()
PROXIES = _proxy.get_proxy_list()


def scrape_strain_metadata() -> pd.DataFrame:
    if os.path.exists("strains_metadata.json"):
        log.info("Loading existing strain metadata")
        with open("strains_metadata.json", "r") as f:
            strain_list = json.load(f)
    else:
        log.info("Scraping strain metadata")
        strains_raw = []
        proxies = {"http": random.choice(PROXIES)}
        for page in trange(1, math.ceil(NUM_STRAINS / 50) + 1):
            if len(strains_raw) % 50 == 0:
                proxies = {"http": random.choice(PROXIES)}
                proxies["https"] = proxies["http"]
                log.info(f"New proxy: {proxies['http']}")
            skip = (page - 1) * 50
            r = requests.get(STRAINS_API_URL.format(skip, 50), proxies=proxies)
            data = json.loads(r.content)
            strains_raw.append(data)
        batches = [batch["hits"]["strain"] for batch in strains_raw]
        strain_list = list(chain.from_iterable(batches))
        with open("strains_metadata.json", "w") as f:
            json.dump(strain_list, f)
    strains = pd.DataFrame()
    for col in ["id", "category", "name", "reviewCount", "slug"]:
        strains[col] = [s[col] for s in strain_list]
    strains["url"] = "http://leafly.com/strain" + strains["slug"]
    for chem in ["cbc", "cbd", "cbg", "thc", "thcv"]:
        strains[f"{chem}_p50"] = [
            s["cannabinoids"][chem]["percentile50"] for s in strain_list
        ]
    strains = strains[strains["reviewCount"] > 0]
    strains = strains.rename(columns={"id": "strain_id", "reviewCount": "review_count"})
    strains = strains.drop_duplicates(subset=["strain_id"])
    strains = strains.set_index("strain_id")
    return strains


def scrape_reviews(strain_slug: str) -> pd.DataFrame:
    """
    This is the task given to each process.  Scrape the reviews for a given strain.
    This way, we can divide a list of strains into n_cores parts for each process.
    """
    reviews = []
    proxies = {"http": random.choice(PROXIES)}
    first_page = requests.get(REVIEWS_API_URL.format(strain_slug, 1), proxies=proxies)
    if first_page.status_code == 429:
        log.warning(f"Sleeping for {first_page.headers['Retry_After']} due to 429")
        time.sleep(int(first_page.headers["Retry-After"]))
        first_page = requests.get(
            REVIEWS_API_URL.format(strain_slug, 1), proxies=proxies
        )
    review_data, metadata = json.loads(first_page.content).values()
    reviews.append(review_data)
    n_reviews = metadata["totalCount"]
    n_pages = math.ceil(n_reviews / 50)
    log.info(f"Scraping {n_reviews} reviews for strain {strain_slug}")
    page_list = trange(2, n_pages + 1) if n_pages > 2 else range(2, n_pages + 1)
    for page in page_list:
        page_req = requests.get(
            REVIEWS_API_URL.format(strain_slug, page), proxies=proxies
        )
        if page_req.status_code == 429:
            time.sleep(int(page_req.headers["Retry-After"]))
            log.warning(f"Sleeping for {page_req.headers['Retry_After']} due to 429")
            page_req = requests.get(
                REVIEWS_API_URL.format(strain_slug, page), proxies=proxies
            )
            if not page_req.ok:
                raise requests.RequestException
        review_data, metadata = json.loads(page_req.content).values()
        reviews.append(review_data)
        time.sleep(3)
    return reviews


if __name__ == "__main__":
    # Make a dataframe of strain metadata
    strains = scrape_strain_metadata()

    # Make a dataframe of review data
    all_strain_slugs = list(
        strains.sort_values(by=["review_count"], ascending=False)["slug"]
    )

    reviews_raw = []
    random.shuffle(all_strain_slugs)
    all_strain_slugs = [
        i for i in all_strain_slugs if not os.path.exists(f"./reviews/reviews_{i}.json")
    ]
    for strain in tqdm(all_strain_slugs):
        if os.path.exists(f"./reviews/reviews_{strain}.json"):
            continue
        reviews_raw.append(scrape_reviews(strain))
        with open(f"./reviews/reviews_{strain}.json", "w") as f:
            json.dump(reviews_raw[-1], f)
        if len(reviews_raw[-1]) <= 1:
            time.sleep(3)
    # TODO: load from reviews dir and then flatten
    # review_list = list(chain.from_iterable(chain.from_iterable(reviews_raw)))
    # df = pd.DataFrame()
    # for col in [
    #     "id",
    #     "username",
    #     "created",
    #     "form",
    #     "language",
    #     "rating",
    #     "upvotesCount",
    #     "text",
    # ]:
    #     df[col] = [r[col] for r in review_list]
    # df["strain_id"] = [
    #     strains.loc[strains["slug"] == r["strainSlug"], :].index[0] for r in review_list
    # ]
    # df = df.rename(columns={"id": "review_id", "upvotesCount": "upvotes"})
