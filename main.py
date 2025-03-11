import requests
from bs4 import BeautifulSoup
import time
import re
from urllib.parse import urljoin
import random
from dotenv import load_dotenv
import os
load_dotenv()


class yad2_motorcycle_listing:
    def __init__(self, listing_id, creation_date, model_name, engine_displacement, license_rank, year, kilometrage, amount_of_owners, color, listed_price):
        self.listing_id = listing_id
        self.creation_date = creation_date
        self.model_name = model_name
        engine_displacement = int(engine_displacement.replace(",", ""))
        self.engine_displacement = engine_displacement
        if license_rank is None:
            if engine_displacement < 500:
                self.license_rank = 'A1'
            elif engine_displacement > 500:
                self.license_rank = 'A'
        else:
            self.license_rank = license_rank
        self.year = year
        self.kilometrage = kilometrage
        self.amount_of_owners = amount_of_owners
        self.color = color
        self.listed_price = listed_price
        # self.yitzhak_levi_price = retrieve_YL_price()

    def __str__(self):
        return f"LISTING ID:{self.id} MODEL:{self.model_name} ENGINE CC:{self.engine_displacement} LICENSE_RANK:{self.license_rank} YEAR:{self.year} KILOMETRAGE:{self.kilometrage} YAD:{self.amount_of_owners} COLOR:{self.color} LISTED PRICE:{self.listed_price}"


# Returns a list of links for each individual listing in the page.
def exctract_individual_listings_links(main_page_html: BeautifulSoup, base_url: str) -> list[str]:
    listings = main_page_html.find_all('div', class_=re.compile("feed-item-base_feedItemBox"))
    links = []
    for listing in listings:
        a_tag = listing.find('a', href=True)
        if a_tag:
            full_url = urljoin(base_url, a_tag['href'])
            links.append(full_url)
    return links


# Recieves html of a listing page of a motorcycle and returns the yad2 mototrycle listing class
def scrape_listing(listing_html: BeautifulSoup) -> yad2_motorcycle_listing:

    # Exctracts listing id
    listing_id = listing_html.find_all('div', class_=re.compile("ad_adNumber"))[0].get_text(strip=True)
    print(f"Listing ID: {listing_id}")

    # Exctracts creation date of the listing
    creation_date = listing_html.find('span', class_=re.compile("report-ad_createdAt")).get_text(strip=True).split()[-1]

    # Exctracts kilometrage,color,license_rank and engine displacement
    dl = listing_html.find("dl")
    children = dl.find_all(["dd", "dt"])
    details = {}
    for i in range(0, len(children), 2):
        label = children[i].get_text(strip=True)
        value = children[i+1].get_text(strip=True)
        details[label] = value
    kilometrage = details.get("קילומטראז׳")
    color = details.get("צבע")
    license_rank = details.get("דרגת רשיון")
    engine_displacement = details.get("נפח מנוע")

    # Exctracts the amount of owners and year
    moto_data = listing_html.find_all('span', class_=re.compile("details-item_itemValue"))
    year = moto_data[0].get_text(strip=True)
    amount_of_owners = moto_data[1].get_text(strip=True)

    # Exctracts listed price
    listed_price = listing_html.find("span", {"data-testid": "price"}).get_text(strip=True)

    # Exctracts model
    model = listing_html.find("h1", {"data-nagish": "upper-heading-title"}).get_text(strip=True)

    motorcycle_listing = yad2_motorcycle_listing(listing_id, creation_date, model, engine_displacement, license_rank, year, kilometrage, amount_of_owners, color, listed_price)
    print(motorcycle_listing)
    return motorcycle_listing


proxy_url = "https://api.brightdata.com/request"
headers = {
    "Authorization": f"Bearer {os.getenv("API_TOKEN")}",
    "Content-Type": "application/json"
}
payload = {
    "zone": os.getenv("ZONE"),
    "format": "raw",
    "method": "GET"
}


def main():

    page = 1
    while True:

        url = f"http://www.yad2.co.il/vehicles/motorcycles?page={page}"
        payload["url"] = url
        response = requests.request("POST", proxy_url, json=payload, headers=headers)

        if response.status_code != 200:
            print(f"Failed to retrieve page number {page}")
            break

        soup = BeautifulSoup(response.text, "html.parser")
        print(f"FINISHED LISTING TOP X CHARACTERS of page number {page}: \n\n")

        # Gathers links for each individual listing page in the original page
        links = exctract_individual_listings_links(soup, url)

        # Loops over every listing in the current page
        for link in links:
            # Retrieves page HTML
            payload["url"] = link
            response = requests.request("POST", proxy_url, json=payload, headers=headers)
            soup = BeautifulSoup(response.text, "html.parser")
            moto_listing = scrape_listing(soup)

        page += 1


if __name__ == "__main__":
    main()
