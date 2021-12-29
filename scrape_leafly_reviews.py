import pandas as pd
from playwright.sync_api import sync_playwright

import random

with open("useragents.txt", "r") as f:
    USER_AGENTS = f.readlines()

HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,image/avif,image/webp,"
        "*/*;q=0.8"
    ),
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "DNT": "1",
    "Host": "www.leafly.com",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "TE": "trailers",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": random.choice(USER_AGENTS),
}

with sync_playwright() as playwright:
    # Setup
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()

    # Get URLs of all strain pages
    # Load strains list
    page.goto(
        "https://www.leafly.com/strains",
        wait_until="networkidle",
    )
    # Close age gate
    page.set_checked("id=remember-user-checkbox", checked=True, force=True)
    page.click("id=tou-continue")
    # Check number of strains
    num_strains = int(
        page.query_selector("text=/\d+ strains/i").inner_text().split()[0]
    )
    # Check result pages count
    num_pages = int(page.query_selector("text=/\d+ of \d+/i").inner_text().split()[-1])
    data = list()
    count = 0
    # TODO: see if we actually get the data for all cards/pages in a single XHR
    while page.is_visible("text=Next", strict=True) and (count < 5):
        count += 1
        page.locator("[itemprop='name']").last.wait_for()
        # Grab strain name + url for each strain on page
        strain_cards = page.locator("data-testid=strain-list__strain-card")
        strain_values = list(
            map(lambda x: x.split("\n")[1:], strain_cards.all_inner_texts())
        )
        df = pd.DataFrame(
            {
                "Type": [strain[0] for strain in strain_values if strain[0]],
                "Name": [strain[1] for strain in strain_values],
                "Slug": [
                    strain.get_attribute("href")
                    for strain in strain_cards.locator("[class=p-md]").element_handles()
                ],
            }
        )
        df = df[df["Type"].isin(["Hybrid", "Indica", "Sativa"])]
        data.append(df)
        # Load next page
        page.query_selector("text=Next").click()
    # Merge each page's results
    data = pd.concat(data, ignore_index=True)
    data["URL"] = data["Slug"].apply(lambda x: f"https://leafly.com{x}")
    print("Breakme")

    # Load /reviews page for each strain url
    # Check number of reviews and pages
    # for page in range(page_max_reviews)
    # copy pseudoname, review, date, ratings, etc.
