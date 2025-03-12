import requests
from bs4 import BeautifulSoup
import time
import re
from urllib.parse import urljoin
import random
from dotenv import load_dotenv
import os
import sqlite3
from datetime import datetime
from googletrans import Translator
import nodriver as nd
import json
import time
import asyncio
from dataclasses import dataclass
load_dotenv()


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


@dataclass
class ExctracedPage:
    page_num: int
    max_page_available: int
    listings: list[MotorcycleListing]


# Exctracts the license rank from string
def exctract_license_rank(s: str) -> str:
    if "47" in s:
        return "A1"
    else:
        return "A"


# Exctracts english variant name from listing dict
def exctract_english_variant(listing: dict) -> str:
    s = listing.get('textEng')
    if s is None:
        s = listing['text']
    return s


# Exctracts all yad2 page data
async def exctract_page_data(page_num: int, browser: nd.Browser) -> ExctracedPage:

    url = f"http://www.yad2.co.il/vehicles/motorcycles?page={page_num}"
    print(f"started scraping page{page_num}")
    # Goes to the given page in Yad2
    tab = await browser.get(url)
    await tab.sleep(5)

    script_element = await tab.query_selector("#__NEXT_DATA__")
    await tab.sleep(3)

    json_text = await script_element.get_html()
    await tab
    print(f"succesfully parsed json data from {page_num}")

    # Parses and loads needed data into one list with all listings
    soup = BeautifulSoup(json_text, 'html.parser')
    json_text = soup.find('script', id='__NEXT_DATA__', type='application/json').get_text(strip=True)
    data = json.loads(json_text)
    commercial_listings = data["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["commercial"]
    private_listings = data["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["private"]
    all_listings = commercial_listings + private_listings

    # For each listing creates a MotorcycleListing dataclass
    all_moto_listings = []
    for listing in all_listings:
        # Debugging printouts
        print("listing_id:", listing['adNumber'])
        print("creation_date:", datetime.fromisoformat(listing['dates']['createdAt']).year)
        print("location_of_seller:", listing['address']['area']['textEng'])
        print("brand:", exctract_english_variant(listing['manufacturer']))
        print("model_name:", exctract_english_variant(listing['model']))
        print("model_year:", listing['vehicleDates']['yearOfProduction'])
        print("engine_displacement:", listing['engineVolume'])
        print("license_rank:", listing['license'].get('text'))
        print("kilometrage:", listing['km'])
        print("amount_of_owners:", listing['hand']['id'])
        print("color:", listing['color']['textEng'])
        print("listed_price:", listing['price'])

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
            color=listing['color']['textEng'],
            listed_price=listing['price']
        )
        print(moto_listing)
        all_moto_listings.append(moto_listing)

    max_page = data["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["pagination"]["pages"]
    exctracted_page = ExctracedPage(page_num=page_num, max_page_available=max_page, listings=all_moto_listings)
    return exctracted_page


async def main():
    page = 1
    browser = await nd.start()
    while True:
        exctracted_page = await exctract_page_data(page, browser)
        if exctracted_page.max_page_available == exctracted_page.page_num:
            print("Scraped all pages")
            break
        print(f"Succesfully scraped page number {page}")
        page += 1


if __name__ == "__main__":
    nd.loop().run_until_complete(main())
