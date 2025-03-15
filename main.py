from bs4 import BeautifulSoup
import nodriver as nd
import curl_cffi
import sqlite3
import json
import logging
import sys
import asyncio
import time
import os
import random
from datetime import datetime
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()
PROXY_IL = os.getenv("PROXY_IL")


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


@dataclass
class ScrapeMetadata:
    """
    Manages scraping metadata by loading from and updating a JSON file.

    Attributes:
        last_scrape_date: Date when the scrape was initiated (ISO format).
        last_successful_scrape_date: Date of the last successful scrape.
        build_id: Build identifier from the previous scrape.
        json_path: File path for the metadata JSON (not shown in repr).
    """
    last_scrape_date: str = datetime.now().date().isoformat()
    last_successful_scrape_date: str = ""
    build_id: str = ""
    json_path: str = field(default="scrape_metadata.json", repr=False)

    def __post_init__(self):
        """
        Loads existing metadata from the JSON file, updates the last_scrape_date,
        and initializes instance variables with loaded values.
        """
        with open(self.json_path, "r+") as json_file:
            data = json.load(json_file)
            # Update the last scrape date to the current value
            data["last_scrape_date"] = self.last_scrape_date
            # Rewind file pointer and overwrite with updated data
            json_file.seek(0)
            json.dump(data, json_file, indent=4)
            json_file.truncate()

        # Set instance variables from the file
        self.last_successful_scrape_date = data["last_successful_scrape_date"]
        self.build_id = data["last_exctracted_build_id"]

    def update(self):
        """ Writes the current metadata back to the JSON file. """
        data = {
            "last_scrape_date": self.last_scrape_date,
            "last_successful_scrape_date": self.last_successful_scrape_date,
            "last_exctracted_build_id": self.build_id
        }
        with open(self.json_path, "w") as json_file:
            json.dump(data, json_file, indent=4)


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


async def exctract_initial_data(scrape_metadata: ScrapeMetadata) -> tuple[str, int]:
    """
    Launches a browser session to extract the build ID and the maximum number of pages from the target site.

    The function performs the following steps:
      1. Opens the specified URL.
      2. Waits for the page to load.
      3. Retrieves the script element containing JSON data.
      4. Parses the JSON to extract the build ID and pagination information.
      5. Closes the browser session.

    Returns:
        tuple[str, int]: A tuple containing the build ID (str) and the maximum number of pages (int).
    """

    logger = logging.getLogger(__name__)
    url = "https://www.yad2.co.il/vehicles/motorcycles"
    logger.info(f"Trying to exctract build id")

    # Start the browser (headless by default or as configured)
    browser = await nd.start()
    await browser.sleep(5)

    # Open the URL in a new tab
    tab = await browser.get(url)
    await tab.sleep(5)

    # Locate the script element that holds the JSON data
    script_element = await tab.query_selector("#__NEXT_DATA__")
    await tab.sleep(2)
    json_text = await script_element.get_html()

    # Stop the browser after extraction
    browser.stop()

    # Parse the HTML containing the JSON data using BeautifulSoup
    soup = BeautifulSoup(json_text, "html.parser")
    json_text = soup.find('script', id="__NEXT_DATA__", type="application/json").get_text(strip=True)

    data = json.loads(json_text)

    # If new build_id was detected
    build_id = data["buildId"]
    if scrape_metadata.build_id != build_id:
        logger.critical(f"New build id detected and updated. previus build id: {scrape_metadata.build_id} new build id: {build_id}")
        scrape_metadata.build_id = build_id

    max_page = data["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["pagination"]["pages"]

    logger.info(f"Successfully finished exctracting initial data. Build ID: {build_id} Amount of pages to scrape: {max_page}\n")
    return build_id, max_page


def create_referer_header(page_num: int, max_page: int) -> dict[str:str]:
    """
    Generates a Referer header to simulate natural user navigation.

    Although not strictly necessary for a successful request, adding a randomized
    Referer header can help the requests appear more natural upon close inspection.

    For page number 1, the referer page is randomly chosen from 1 to 27.
    For other pages, it is chosen from a range of values near the current page, but not equal to it.

    Args:
        page_num (int): The current page number.
        max_page (int): The maximum available page number.

    Returns:
        dict[str, str]: A dictionary containing the Referer header.
    """
    if page_num == 1:
        # For page 1, choose any number between 1 and 27 (inclusive)
        candidates = range(2, 28)
    else:
        # For other pages, select a range near the current page, ensuring values are within bounds and not equal to page_num.
        lower = max(1, page_num-3)
        upper = min(max_page, page_num+3)
        candidates = [number for number in range(lower, upper+1) if number != page_num]

    # Build and return the Referer header
    return {"Referer": f"https://www.yad2.co.il/vehicles/motorcycles?page={random.choice(candidates)}"}


def is_json(s: str) -> bool:
    """Check if the given string is valid JSON."""
    try:
        json.loads(s)
    except ValueError:
        return False
    return True


def exctract_page_data(page_num: int, build_id: str, max_page: int) -> ExctracedPage:
    """
    Scrapes data from a given page number using the build id and extracts motorcycle listings.

    The function makes repeated GET requests (up to a maximum number of attempts) to retrieve JSON data.
    It logs the status code and a snippet of the response, waits between attempts if the request fails,
    and ultimately parses the response JSON to extract both commercial and private listings.

    Args:
        page_num (int): The page number to scrape.
        build_id (str): The build ID required to construct the URL.
        max_page (int): The maximum number of pages available (used for header generation).

    Returns:
        ExctracedPage: An object containing the scraped page data.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Started scraping page number {page_num}")

    # Construct the URL for the JSON data endpoint
    url = f"https://www.yad2.co.il/vehicles/_next/data/{build_id}/motorcycles.json?page={page_num}"

    attempts = 0
    max_attempts = 10
    proxies = {
        "http": PROXY_IL,
        "https": PROXY_IL
    }

    # Try up to max_attempts times to get a valid response (HTTP 200)
    while attempts < max_attempts:
        try:
            response = curl_cffi.get(url=url, impersonate="chrome", headers=create_referer_header(page_num, max_page), proxies=proxies, timeout=60)
        except:
            logger.exception(f"Couldn't complete GET reuquest from target.")
            logger.info("Trying again")
            attempts += 1
            time.sleep(5)
            continue

        logger.debug(f"Recieved response. Status code is: {response.status_code}")
        if response.status_code != 200:
            logger.warning("Didn't recieve 200 response from server; Sleeping for 5-10 minutes and trying again.")
            time.sleep(random.uniform(300, 600))
            attempts += 1
            continue
        elif not is_json(response.text):
            logger.warning("Recieved status code 200 but the data wasn't a JSON file.")
            logger.info(f"First 100 characters of the data recieved:{response.text[0:100]}")
            time.sleep(random.uniform(30, 60))
            attempts += 1
            continue
        else:
            logger.debug("Proper JSON file recieved")
            break

    # Exit if max attempts reached without success
    if attempts == max_attempts:
        logger.critical(f"Scraping failed at page: {page_num} after {attempts} attempts.")
        sys.exit(1)

    # Parse JSON response data
    data = response.json()
    response.close()  # Close the response to free resources

    # Combine commercial and private listings.
    commercial_listings = data["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["commercial"]
    private_listings = data["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["private"]
    all_listings = commercial_listings + private_listings

    # Process each listing and convert to a MotorcycleListing instance
    all_moto_listings = []
    for listing in all_listings:
        moto_listing = MotorcycleListing(
            listing_id=listing['adNumber'],
            creation_date=str(datetime.fromisoformat(listing['dates']['createdAt']).date()),
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
        # logger.debug(moto_listing)
        all_moto_listings.append(moto_listing)

    # Update max_page in case it changed in the response data
    max_page = data["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]["pagination"]["pages"]

    # Construct the ExctracedPage object with all listings
    exctracted_page = ExctracedPage(
        page_num=page_num,
        max_page_available=max_page,
        listings=all_moto_listings
    )

    logger.info(f"Successfully finished scraping page number {page_num}")
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
        logger.info(f"Successfully inserted listings of page number {exctracted_page.page_num} into db\n")
    except Exception:
        logger.exception(f"\nError inserting data into db for page {exctracted_page.page_num}\n")
    connection.commit()
    cursor.close()


def set_up_logging(set_level=logging.DEBUG):
    """
    Configure logging to output to both the console and a log file.

    Logs are output with a timestamp, log level, and logger name.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(set_level)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Create a console handler for output to stdout.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # Create a file handler for logging to a file
    file_handler = logging.FileHandler("scraper.log", mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Add both handlers to the logger.
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.info(f"\n\n\nStarted Scraping: {datetime.now()}")


def main():
    """
    Main function to drive the scraping process.

    This function initializes the browser and database connection,
    then iterates through pages to scrape listings until the last page is reached.
    """
    logger = logging.getLogger(__name__)
    scrape_metadata = ScrapeMetadata()
    page_num = 1

    # Connect to the SQLite database to store the scraped listings.
    connection = sqlite3.connect("yad2_motorcycles_listings.db")

    # Extract initial data such as build_id and maximum page count from the target site.
    build_id, max_page = asyncio.run(exctract_initial_data(scrape_metadata))

    # Loop through pages until the scraping process is complete.
    while True:
        # Wait for a random period between 1 and 120 seconds before each request,
        wait_for = random.uniform(1, 120)
        logger.debug(f"Sleeping for: {wait_for} seconds")
        time.sleep(wait_for)

        # Extract page data for the current page.
        exctracted_page = exctract_page_data(page_num, build_id, max_page)

        # Insert the scraped data into the database.
        insert_page_into_db(exctracted_page, connection)

        # Warn if the number of listings is not as expected.
        if not exctracted_page.is_last() and len(exctracted_page.listings) < 40:
            logger.warning(f"Page number {page_num} or {exctracted_page.page_num} contained less then 40 entries but {len(exctracted_page.listings)}")

        # If the current page is the last one, log completion and break out of the loop.
        if exctracted_page.is_last():
            logger.info(f"Successfully finished scraping all {exctracted_page.page_num} pages")
            scrape_metadata.last_successful_scrape_date = scrape_metadata.last_scrape_date
            scrape_metadata.update()
            logger.info("Successfully updated the metadata file.")
            break

        page_num += 1


if __name__ == "__main__":
    set_up_logging()
    main()
    sys.exit(0)
