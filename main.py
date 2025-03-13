from bs4 import BeautifulSoup
import nodriver as nd
import sqlite3
from datetime import datetime
import json
from dataclasses import dataclass
import logging
import sys
from pathlib import Path
import asyncio


@dataclass
class MotorcycleListing:
    """Data class representing a motorcycle listing."""
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
        """
        Post-initialization to adjust default values.

        - If license_rank is None, it is determined based on engine displacement.
        - If model_name is None, it is set to "N/A".
        """
        if self.license_rank is None:
            self.license_rank = 'A' if self.engine_displacement > 500 else 'A1'
        else:
            self.license_rank = exctract_license_rank(self.license_rank)

        if self.model_name is None:
            self.model_name = "N/A"


@dataclass
class ExctracedPage:
    """Data class representing the data extracted from a single page."""
    page_num: int
    max_page_available: int
    listings: list[MotorcycleListing]

    def is_last(self) -> bool:
        if self.max_page_available == self.page_num:
            return True
        else:
            return False


def exctract_license_rank(s: str) -> str:
    """Determine the simplified license rank from the provided string."""
    if "47" in s:
        return "A1"
    elif "A2" in s:
        return "A2"
    else:
        return "A"


def exctract_english_variant(listing: dict) -> str:
    """Extracts the English variant name from a listing dictionary"""
    if listing:
        s = listing.get('textEng')
        if s is None:
            s = listing.get('text')
        return s
    else:
        return None


async def exctract_page_data(page_num: int, browser: nd.Browser) -> ExctracedPage:
    """
    Scrape and extract data from a given Yad2 page.

    The function performs the following steps:
      1. Construct the URL for the desired page.
      2. Navigate to the URL using the provided browser.
      3. Handle potential captchas by waiting if needed.
      4. Extract the JSON data embedded in the page.
      5. Parse and combine commercial and private listings.
      6. Convert each raw listing into a MotorcycleListing instance.
      7. Retrieve pagination info.

        Returns:
        ExctracedPage: An object containing the page number, maximum pages available,
                      and the list of motorcycle listings extracted from the page.
    """
    logger = logging.getLogger(__name__)
    url = f"http://www.yad2.co.il/vehicles/motorcycles?page={page_num}"
    logger.info(f"started scraping page number {page_num}")

    # Open the specified URL in a new browser tab.
    tab = await browser.get(url)
    await tab.sleep(0.5)

    # Attempt to locate the script element containing JSON data.
    script_element = await tab.query_selector("#__NEXT_DATA__")

    # Likely due to a CAPTCHA.
    while script_element is None:
        logger.info("Encountered Captcha")
        await tab.sleep(2)
        try:
            logger.info("Waiting for captcha to be solved...")
            await tab.wait_for(
                selector='iframe[data-hcaptcha-widget-id]:not([data-hcaptcha-response=""])',
                timeout=60000
            )
            await tab.sleep(7)
            logger.info("CAPTCHA Solved - continuing scraping")

        except asyncio.TimeoutError:
            logger.warning("Waiting for solving timedout")

        script_element = await tab.query_selector("#__NEXT_DATA__")

    await tab.sleep(0.1)

    # Extract the HTML from the script element.
    json_text = await script_element.get_html()
    logger.info(f"succesfully exctracted json data from {page_num}")

    # Parse the JSON text using BeautifulSoup and json.loads.
    soup = BeautifulSoup(json_text, 'html.parser')
    json_text = soup.find('script', id='__NEXT_DATA__', type='application/json').get_text(strip=True)
    data = json.loads(json_text)

    # Combine commercial and private listings.
    commercial_listings = data["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["commercial"]
    private_listings = data["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["private"]
    all_listings = commercial_listings + private_listings

    # Convert each listing to a MotorcycleListing instance.
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

    # Get the maximum number of pages available from pagination data.
    max_page = data["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["pagination"]["pages"]
    exctracted_page = ExctracedPage(page_num=page_num, max_page_available=max_page, listings=all_moto_listings)
    logger.info(f"Succesfully scraped page number {page_num}")
    return exctracted_page


def insert_page_into_db(exctracted_page: ExctracedPage, connection: sqlite3.Connection):
    """
    Insert the listings from an extracted page into the SQLite database.

    This function uses an UPSERT query:
      - Inserts a new row if the listing does not exist.
      - Updates the last_seen field if the listing_id already exists.
    """
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

    # Convert the list of MotorcycleListing objects into a list of dictionaries
    data = [vars(listing) for listing in exctracted_page.listings]
    try:
        cursor.executemany(query, data)
        logger.info(f"Succesfully inserted listings of page number {exctracted_page.page_num} into db\n")
    except Exception:
        logger.exception(f"\nError inserting data into db for page {exctracted_page.page_num}\n")
    connection.commit()
    cursor.close()


def update_last_scrape_date():
    with open("last_scrape_date.txt", "w") as f:
        current_date = datetime.now().date().isoformat()
        f.write(current_date)


def set_up_logging():
    """
    Configure logging to output to both the console and a log file.

    Logs are output with a timestamp, log level, and logger name.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Create a console handler for output to stdout.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Create a file handler for logging to a file
    file_handler = logging.FileHandler("scraper.log", mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Add both handlers to the logger.
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.info("\n\n\nNEW SCRAPING BATCH")


async def main():
    """
    Main function to drive the scraping process.

    This function initializes the browser and database connection,
    then iterates through pages to scrape listings until the last page is reached.
    """
    logger = logging.getLogger(__name__)
    page = 1

    captcha_solver_extension_path = r"D:\Programming\Captcha Solver Extension\solver"
    captcha_solver_pop_up_path = r"chrome-extension://hlifkpholllijblknnmbfagnkjneagid/popup/popup.html"

    config = nd.Config()
    config.add_extension(captcha_solver_extension_path)

    browser = await nd.start(config=config)
    await browser.sleep(2)
    # temp_tab = await browser.get(captcha_solver_pop_up_path)
    # await browser.sleep(2)

    connection = sqlite3.connect("yad2_motorcycles_listings.db")

    # Loop through pages until all pages are scraped.
    while True:

        exctracted_page = await exctract_page_data(page, browser)
        insert_page_into_db(exctracted_page, connection)

        # Warn if the number of listings is not as expected.
        if exctracted_page.is_last() and len(exctracted_page.listings) < 40:
            logger.warning(f"Page number {page} or {exctracted_page.page_num} contained less then 40 entries but {len(exctracted_page.listings)}")

        # Stop the loop if the current page is the last page.
        if exctracted_page.max_page_available == exctracted_page.page_num:
            logger.info("Finished scraping all pages")
            update_last_scrape_date()
            logger.info("Updated last_scrape_date file succesfully.")
            break

        page += 1


if __name__ == "__main__":
    set_up_logging()  # Initialize logging for the application.
    nd.loop().run_until_complete(main())  # Run the main asynchronous function.
