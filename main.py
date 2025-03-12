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
load_dotenv()

hebrew_to_english_brand_map = {
    "ימאהה": "Yamaha",
    "סי אף מוטו": "CFMoto",
    "הונדה": "Honda",
    "ק.ט.מ": "KTM",
    "קליבלנד": "Cleveland",
    "סאן יאנג": "SYM",
    "קאוואסאקי": "Kawasaki",
    "רויאל": "Royal Enfield",
    "דוקאטי": "Ducati",
    "טריומף": "Triumph",
    "ב.מ.וו": "B.M.W",
    "מוטוגוצי": "Moto Gucci",
    "בנלי": "Benelli",
    "הוסקוורנה": "Husqvarna",
    "סוזוקי": "Suzuki",
    "הרלי": "Harley Davidson",
    "אינדיאן": "Indian",
    "Voge": "Voge",
    "פאנטיק": "Fantic"
}

hebrew_to_english_color_map = {
    "לבן": "White",
    "לבן מטאלי": "White",
    "שחור": "Black",
    "אדום": "Red",
    "כחול בהיר": "Light Blue",
    "צהוב": "Yellow",
    "כחול": "Blue",
    "אפור": "Gray",
    "סגול": "Purple",
    "כתום": "Orange",
    "ירוק": "Green",
    "חום מטאלי": "Metallic Brown",
    "כסוף מטאלי": "Silver",

}


class MotorcycleListing:
    def __init__(self, listing_id, creation_date, model_and_brand, model_year, engine_displacement, license_rank, kilometrage, amount_of_owners, color, listed_price):

        self.listing_id = listing_id
        self.creation_date = format_date(creation_date)
        self.brand, self.model_name = exctract_brand_and_model(model_and_brand)
        self.model_year = model_year
        engine_displacement = int(engine_displacement.replace(",", ""))
        self.engine_displacement = engine_displacement

        # Inits license rank
        if license_rank is None:
            if engine_displacement < 500:
                self.license_rank = 'A1'
            elif engine_displacement > 500:
                self.license_rank = 'A'
            else:
                self.license_rank = 'N/A'
        else:
            if "47" in license_rank:
                self.license_rank = "A1"
            else:
                self.license_rank = "A"
        # Kilometrage
        self.kilometrage = int(re.sub(r"[^\d]", "", kilometrage))
        # Amount of owners
        self.amount_of_owners = amount_of_owners

        # Initilizes Color translates it from Heb to Eng
        if len(color.split()) > 1:
            color = color.split()[0]
        if color is None:
            print(color)
            color = "Other"
        self.color = hebrew_to_english_color_map.get(color)

        # Inits the listed price into an int
        if listed_price == "לא צוין מחיר":
            self.listed_price = None
        else:
            self.listed_price = int(re.sub(r"[^\d]", "", listed_price))

        self.active = True

    def __str__(self):
        return (
            f"LISTING ID:{self.listing_id}\n"
            f"CREATION DATE:{self.creation_date}\n"
            f"BRAND:{self.brand}\n"
            f"MODEL:{self.model_name}\n"
            f"MODEL_YEAR:{self.model_year}\n"
            f"ENGINE CC:{self.engine_displacement}\n"
            f"LICENSE RANK:{self.license_rank}\n"
            f"YEAR:{self.model_year}\n"
            f"KILOMETRAGE:{self.kilometrage}\n"
            f"YAD:{self.amount_of_owners}\n"
            f"COLOR:{self.color}\n"
            f"LISTED PRICE:{self.listed_price}\n"
            f"ACTIVE:{self.active}\n"
        )


# Formats DD/MM/YYYY to the ISO YYYY-MM-DD format for sql
def format_date(date: str) -> str:
    date_elements = date[1:].split("/")
    formatted_date = f"20{date_elements[-1]}-{date_elements[1]}-{date_elements[0]}"
    return formatted_date


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


# Recieves string with the brand and model of a motorycle and exctracts model and brand
def exctract_brand_and_model(s: str) -> tuple[str]:
    brand = None
    model = ""
    if "KTM" in s:
        brand = "KTM"
        model = s.replace("KTM", "").strip()
    else:
        model = ""
        words = s.split()
        for word in words:
            clean_word = word.strip()
            if clean_word in hebrew_to_english_brand_map:
                brand = hebrew_to_english_brand_map[clean_word]
            else:
                model += f" {clean_word}"

        model = model.replace("אנפילד", "")
        model = model.replace("הרלי", "")
        model = model.strip()

    if brand is None:
        brand = "Other"

    return brand, model


# Recieves html of a listing page of a motorcycle and returns the yad2 mototrycle listing class
def scrape_listing(listing_html: BeautifulSoup) -> MotorcycleListing:

    # Exctracts listing id
    listing_id = listing_html.find_all('div', class_=re.compile("ad_adNumber"))[0].get_text(strip=True)

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
    model_year = moto_data[0].get_text(strip=True)
    amount_of_owners = moto_data[1].get_text(strip=True)

    # Exctracts listed price
    listed_price = listing_html.find("span", {"data-testid": "price"}).get_text(strip=True)

    # Exctracts model and brand
    model_and_brand = listing_html.find("h1", {"data-nagish": "upper-heading-title"}).get_text(strip=True)

    motorcycle_listing = MotorcycleListing(listing_id, creation_date, model_and_brand, model_year, engine_displacement, license_rank, kilometrage, amount_of_owners, color, listed_price)
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
    connection = sqlite3.connect("yad2_motorcycles_listings.db")
    cursor = connection.cursor()

    page = 4
    while True:
        print(f"Started Scraping Page:{page}")
        url = f"http://www.yad2.co.il/vehicles/motorcycles?page={page}"
        payload["url"] = url
        response = requests.request("POST", proxy_url, json=payload, headers=headers)

        if response.status_code != 200:
            print(f"Failed to retrieve page number {page}")
            break

        soup = BeautifulSoup(response.text, "html.parser")

        # Gathers links for each individual listing page in the original page
        links = exctract_individual_listings_links(soup, url)

        # Loops over every listing in the current page
        for link in links:
            print(f"Started Scraping listing:{link}\n")
            # Retrieves page HTML
            payload["url"] = link
            response = requests.request("POST", proxy_url, json=payload, headers=headers)
            soup = BeautifulSoup(response.text, "html.parser")
            moto_listing = scrape_listing(soup)
            print(moto_listing)
            query = """
                INSERT INTO motorcycle_listings (
                listing_id,
                creation_date,
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
            SET last_seen = date('now')
            """
            cursor.execute(query, vars(moto_listing))
            connection.commit()
        page += 1

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
