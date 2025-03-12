from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import nodriver as nd
import json
import asyncio
from dataclasses import dataclass
import logging
import sys


@dataclass
class MotorcycleListing:
    listing_id: int
    creation_date: str
    location_of_seller: str
    brand: str
    model_name: str
    model_year: str
    engine_displacement: int
    license_rank: str
    kilometrage: int
    amount_of_owners: int
    color: str
    listed_price: int
    active: bool = True

    def __post_init__(self):
        if self.license_rank is None:
            self.license_rank = 'A' if self.engine_displacement > 500 else 'A1'
        else:
            self.license_rank = exctract_license_rank(self.license_rank)

        if self.model_name is None:
            self.model_name = "N/A"


@dataclass
class ExctracedPage:
    page_num: int
    max_page_available: int
    listings: list[MotorcycleListing]


# Exctracts the license rank from string
def exctract_license_rank(s: str) -> str:
    if "47" in s:
        return "A1"
    elif "A2" in s:
        return "A2"
    else:
        return "A"


# Exctracts english variant name from listing dict
def exctract_english_variant(listing: dict) -> str:
    if listing:
        s = listing.get('textEng')
        if s is None:
            s = listing.get('text')
        return s
    else:
        return None


# Exctracts all yad2 page data
async def exctract_page_data(page_num: int, browser: nd.Browser) -> ExctracedPage:
    logger = logging.getLogger(__name__)
    url = f"http://www.yad2.co.il/vehicles/motorcycles?page={page_num}"
    logger.info(f"started scraping page number {page_num}")

    # Goes to the given page in Yad2
    tab = await browser.get(url)
    await tab.sleep(2)

    # Tries exctracting the json data embedded in the page
    script_element = await tab.query_selector("#__NEXT_DATA__")
    if script_element is None:  # In case of captcha
        logger.info("Encountered Captcha")
        await tab.sleep(60)
        script_element = await tab.query_selector("#__NEXT_DATA__")
    await tab.sleep(1)

    # Parses and loads needed data into one list with all listings
    json_text = await script_element.get_html()
    logger.info(f"succesfully exctracted json data from {page_num}")
    soup = BeautifulSoup(json_text, 'html.parser')
    json_text = soup.find('script', id='__NEXT_DATA__', type='application/json').get_text(strip=True)
    data = json.loads(json_text)
    commercial_listings = data["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["commercial"]
    private_listings = data["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["private"]
    all_listings = commercial_listings + private_listings

    # For each listing creates a MotorcycleListing dataclass
    all_moto_listings = []
    for listing in all_listings:

        moto_listing = MotorcycleListing(
            listing_id=listing['adNumber'],
            creation_date=datetime.fromisoformat(listing['dates']['createdAt']).year,
            location_of_seller=listing['address']['area']['textEng'],
            brand=exctract_english_variant(listing['manufacturer']),
            model_name=exctract_english_variant(listing['model']),
            model_year=listing['vehicleDates']['yearOfProduction'],
            engine_displacement=listing['engineVolume'],
            license_rank=listing['license'].get('text'),
            kilometrage=listing['km'],
            amount_of_owners=listing['hand']['id'],
            color=exctract_english_variant(listing.get('color')),
            listed_price=listing['price']
        )
        logger.debug(moto_listing)
        all_moto_listings.append(moto_listing)

    max_page = data["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["pagination"]["pages"]
    exctracted_page = ExctracedPage(page_num=page_num, max_page_available=max_page, listings=all_moto_listings)
    logger.info(f"Succesfully scraped page number {page_num}")
    return exctracted_page


# Inserts a pages data into the db
def insert_page_into_db(exctracted_page: ExctracedPage, connection: sqlite3.Connection):
    logger = logging.getLogger(__name__)
    cursor = connection.cursor()
    query = """
        INSERT INTO motorcycle_listings (
            listing_id,
            creation_date,
            location_of_seller,
            brand,
            model_name,
            model_year,
            engine_displacement,
            license_rank,
            kilometrage,
            amount_of_owners,
            color,
            listed_price,
            active,
            last_seen
        )
        VALUES (
            :listing_id,
            :creation_date,
            :location_of_seller,
            :brand,
            :model_name,
            :model_year,
            :engine_displacement,
            :license_rank,
            :kilometrage,
            :amount_of_owners,
            :color,
            :listed_price,
            :active,
            date('now')
        )
        ON CONFLICT(listing_id) DO UPDATE
        SET last_seen = date('now');
        """
    data = [vars(listing) for listing in exctracted_page.listings]
    try:
        cursor.executemany(query, data)
        logger.info(f"Succesfully inserted listings of page number {exctracted_page.page_num} into db\n")
    except Exception:
        logger.exception(f"\nError inserting data into db for page {exctracted_page.page_num}\n")
    connection.commit()
    cursor.close()


# Sets up the logging configuration under __name__ logger
def set_up_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler("scraper.log", mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.info("\n\n\nNEW SCRAPING BATCH")


async def main():
    logger = logging.getLogger(__name__)
    page = 1
    browser = await nd.start()
    await browser.sleep(2)
    connection = sqlite3.connect("yad2_motorcycles_listings.db")

    while True:
        exctracted_page = await exctract_page_data(page, browser)

        insert_page_into_db(exctracted_page, connection)
        logger.info(f"Succesfully inserted listings of page number {page} into db\n")

        if len(exctracted_page.listings) != 40:
            logger.warning(f"Page number {page} or {exctracted_page.page_num} contained less then 40 entries but {len(exctracted_page.listings)}")

        if exctracted_page.max_page_available == exctracted_page.page_num:
            logger.info("Finished scraping all pages")
            break

        page += 1


if __name__ == "__main__":
    set_up_logging()
    nd.loop().run_until_complete(main())
