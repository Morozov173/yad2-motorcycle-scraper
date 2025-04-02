from camoufox.sync_api import Camoufox
import curl_cffi
import sqlite3
import json
import csv
import logging
import sys
import time
import os
import random
from datetime import datetime
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")
PROXY_LINK = os.getenv("PROXY_LINK")


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
    _logger: logging.Logger = field(default=logging.getLogger(__name__), init=False, repr=False)

    def __post_init__(self):
        """
        Post-initialization method to adjust default values for fields.

        If license_rank is not provided, it is determined based on engine displacement.
        If model_name is not provided, it is set to "N/A".
        """
        self.brand = "other" if self.brand == "אחר" else self.brand

        # Automatically determine license rank based on engine displacement (cc)
        if self.engine_displacement <= 125:
            license_rank_based_on_cc = "A2"
        elif self.engine_displacement <= 500:
            license_rank_based_on_cc = "A1"
        else:
            license_rank_based_on_cc = "A"

        # If license_rank is not provided, set it based on engine displacement
        if self.license_rank is None:
            self.license_rank = license_rank_based_on_cc
        else:
            self.license_rank = exctract_license_rank(self.license_rank)

            # Handle mismatch between engine displacement and provided license rank
            if license_rank_based_on_cc != self.license_rank and self.license_rank != 'A1':
                self._logger.debug(f"Listings for {self.brand} {self.model_name} has mismatch between CC of {self.engine_displacement} and license rank {self.license_rank}")
                self.license_rank = license_rank_based_on_cc

        # Default model_name to "N/A" if it is not provided
        if self.model_name is None:
            self.model_name = "N/A"


@dataclass
class ExctracedPage:
    """Data class representing the data extracted from a single page."""
    page_num: int
    max_page_available: int
    listings: list[MotorcycleListing]

    def is_last(self) -> bool:
        """Checks if the current page is the last page based on available pages."""
        if self.max_page_available == self.page_num:
            return True
        else:
            return False


@dataclass
class ScrapeMetadata:
    """
    Class to manage and update metadata related to the scraping process.

    Attributes:
        last_scrape_date: Date when the scraping was initiated (in ISO format).
        last_successful_scrape_date: Date of the last successful scrape.
        build_id: Build identifier from the previous scrape.
        amount_listings_added: Number of new listings added during the scrape.
        amount_listings_removed: Number of listings removed during the scrape.
        json_path: File path for the metadata JSON file.
    """
    last_scrape_date: str = datetime.now().date().isoformat()
    last_successful_scrape_date: str = ""
    build_id: str = ""
    amount_listings_added: int = 0
    amount_listings_removed: int = 0
    json_path: str = field(default="metadata.json", repr=False)

    def __post_init__(self):
        """Load existing metadata from the JSON file and update the last scrape date."""
        with open(self.json_path, "r+") as json_file:
            data = json.load(json_file)
            # Update the last scrape date to the current value
            data["last_scrape_date"] = self.last_scrape_date
            json_file.seek(0)
            json.dump(data, json_file, indent=4)
            json_file.truncate()

        # Set instance variables from the loaded file data
        self.last_successful_scrape_date = data["last_successful_scrape_date"]
        self.build_id = data["last_exctracted_build_id"]
        self.amount_listings_added = data["amount_listings_added"]
        self.amount_listings_removed = data["amount_listings_removed"]

    def update(self):
        """ Write the current metadata back to the JSON file. """
        data = {
            "last_scrape_date": self.last_scrape_date,
            "last_successful_scrape_date": self.last_successful_scrape_date,
            "last_exctracted_build_id": self.build_id,
            "amount_listings_added": self.amount_listings_added,
            "amount_listings_removed": self.amount_listings_removed
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


def exctract_build_id(metadata: ScrapeMetadata) -> str:
    """
    Extracts the build ID from the target website by launching a browser session."""

    logger = logging.getLogger(__name__)
    url = "https://www.yad2.co.il/vehicles/motorcycles"
    proxy = {
        "server": PROXY_SERVER,
        "username": PROXY_USERNAME,
        "password": PROXY_PASSWORD
    }

    logger.info("Trying to exctract the Build ID")
    # Launch browser and extract the build ID
    with Camoufox(headless=True, geoip=True, proxy=proxy) as browser:
        page = browser.new_page()
        page.goto(url, timeout=60000)
        time.sleep(2)
        json_text = page.locator("script#__NEXT_DATA__").text_content()
        time.sleep(2)

    data = json.loads(json_text)

    build_id = data["buildId"]
    logger.info(f"Successfuly exctracted the Build ID: {build_id}")

    # Update metadata if the build ID has changed
    if metadata.build_id != build_id:
        logger.critical(f"New build id detected and updated. previus build id: {metadata.build_id} new build id: {build_id}")
        metadata.build_id = build_id
        metadata.update()

    return build_id


def is_json(s: str) -> bool:
    """Check if the given string is valid JSON."""
    try:
        json.loads(s)
    except ValueError:
        return False
    return True


def request_json(url: str, max_attempts: int = 10) -> dict:
    """
    Performs a GET request to the provided URL and attempts to retrieve a valid JSON response.

    Retries up to max_attempts if a valid response is not received.
    """
    logger = logging.getLogger(__name__)
    proxies = {
        "http": PROXY_LINK,
        "https": PROXY_LINK
    }

    attempts = 0
    # Try up to max_attempts times to get a valid response (HTTP 200)
    while attempts < max_attempts:

        # Try to performn GET request to the target url
        try:
            response = curl_cffi.get(url=url, impersonate="chrome", proxies=proxies, timeout=60)
        except Exception as e:
            logger.warning(f"Couldn't complete GET reuquest from target. Exception raised: {e}")
            logger.info("Trying again")
            attempts += 1
            time.sleep(5)
            continue

        # If recieved response check that it's the desired one (200)
        logger.debug(f"Recieved response. Status code is: {response.status_code}")
        if response.status_code != 200:
            logger.warning("Didn't recieve 200 response from server; Sleeping for 1-10 minutes and trying again.")
            time.sleep(random.uniform(60, 600))
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
        logger.critical(f"Scraping failed at page {url}:  after {attempts} attempts.")
        sys.exit(1)

    json_dict = response.json()
    response.close()
    return json_dict


def exctract_page_data(page_num: int, build_id: str) -> ExctracedPage:
    """
    Scrapes data from a given page number using the provided build ID.
    Returns an object containing the listings from that page.
    """
    logger = logging.getLogger(__name__)

    # Construct the URL for the JSON data endpoint
    url = f"https://www.yad2.co.il/vehicles/_next/data/{build_id}/motorcycles.json?page={page_num}"

    # Parse JSON response data
    data = request_json(url)

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
            model_name=exctract_english_variant(listing.get('model')),
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

    return exctracted_page


def insert_page_into_db(exctracted_page: ExctracedPage, connection: sqlite3.Connection):
    """
    Insert the listings from an extracted page into the SQLite database.
    Uses UPSERT to insert new rows or update the `last_seen` column for existing rows.
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
        connection.rollback()
        logger.exception(f"\nError inserting data into db for page {exctracted_page.page_num}\n")

    connection.commit()
    cursor.close()


def update_inactive_listings(connection: sqlite3.Connection, metadata: ScrapeMetadata):
    """Updates listings that were not seen during the scrape by setting their status to inactive."""
    logger = logging.getLogger(__name__)
    logger.debug(f"Started updating listing status in database")

    cursor = connection.cursor()
    query = """
    UPDATE motorcycle_listings
    SET active = False
    WHERE last_seen < :last_successful_scrape_date AND active = True;
    """
    try:
        cursor.execute(query, {"last_successful_scrape_date": metadata.last_successful_scrape_date})
        metadata.amount_listings_removed = cursor.rowcount
        logger.debug(f"Amount of listings set to Inactive: {metadata.amount_listings_removed}")
    except:
        connection.rollback()
        logger.warning(f"Couldn't update listing status in database.")

    connection.commit()
    cursor.close()
    logger.info(f"Successfuly updated listing status in database. Listings removed since last scrape: {metadata.amount_listings_removed}")


def create_active_listings_csv(connection: sqlite3.Connection):
    """
    Creates a CSV file containing the active listings from the database.
    The file includes a model rank based on the number of listings per model.
    """
    logger = logging.getLogger(__name__)
    cursor = connection.cursor()
    query = """   
        WITH motorcycle_listings_formatted AS (
            SELECT 
                listing_id,
                creation_date,
                REPLACE(location_of_seller, '_', ' ') AS location_of_seller,
                REPLACE(UPPER(SUBSTR(brand, 1, 1)) || SUBSTR(brand, 2), '_', ' ') AS brand,
                model_name,
                model_year,
                engine_displacement,
                license_rank,
                kilometrage,
                amount_of_owners,
                color,
                listed_price,
                active,
                last_seen,
                UPPER(SUBSTR(brand, 1, 1)) || SUBSTR(brand, 2) || ' ' || model_name AS brand_model_name,
                COUNT(*) OVER(PARTITION BY brand, model_name) AS model_count
            FROM motorcycle_listings
            WHERE active = True
        )

        SELECT 
            *,
            DENSE_RANK() OVER(ORDER BY model_count DESC) AS model_rank
        FROM motorcycle_listings_formatted;
    """
    try:
        active_listings = cursor.execute(query).fetchall()
        column_names = [column_data[0] for column_data in cursor.description]
    except Exception as e:
        logger.error(f"Couldn't fetch active listings from database. Exception: {e}")
        return

    with open("active_listings.csv", "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(column_names)
        writer.writerows(active_listings)

    cursor.close()
    logger.info("Succesfully created active_listings csv file.")


def set_up_logging(set_level=logging.DEBUG):
    """Configure logging to output logs to both the console and a log file."""
    logger = logging.getLogger(__name__)
    logger.setLevel(set_level)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler for output to stdout.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # File handler for logging to a file
    file_handler = logging.FileHandler("scraper.log", mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Add both handlers to the logger.
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def main():
    """
    Main scraping function that initiates the scraping process,
    processes data, and stores results in the database.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Scrape started on: {datetime.now()}")

    # Connect to the SQLite database to store listings
    connection = sqlite3.connect("yad2_motorcycles_listings.db")
    cursor = connection.cursor()

    metadata = ScrapeMetadata()
    page_num = 1

    build_id = exctract_build_id(metadata)

    # Fetch initial listings count from the database
    amount_of_listings,  = cursor.execute("SELECT COUNT(*) FROM motorcycle_listings;").fetchone()
    logger.debug(f"Amount of listings at the start of the scrape: {amount_of_listings}")

    # Scrape data across pages
    while True:
        # Wait for a random period between requests to avoid rate limiting
        wait_for = random.uniform(1, 60)
        logger.debug(f"Sleeping for: {wait_for} seconds")
        time.sleep(wait_for)

        # Scrape data from the current page
        logger.info(f"Started scraping page number {page_num}")
        exctracted_page = exctract_page_data(page_num, build_id)
        logger.info(f"Successfully finished scraping page number {page_num}")

        # Insert the scraped data into the database
        insert_page_into_db(exctracted_page, connection)

        # Warn if the number of listings is not as expected.
        if not exctracted_page.is_last() and len(exctracted_page.listings) < 40:
            logger.warning(f"Page number {page_num} or {exctracted_page.page_num} contained less then 40 entries but {len(exctracted_page.listings)}")

         # Check if this is the last page.
        if exctracted_page.is_last():
            logger.info(f"Successfully finished scraping all {exctracted_page.page_num} pages")
            break

        page_num += 1

    # Mark inactive listings in the database
    update_inactive_listings(connection, metadata)

    # Update metadata after scraping
    new_amount_of_listings, = cursor.execute("SELECT COUNT(*) FROM motorcycle_listings;").fetchone()
    metadata.amount_listings_added = new_amount_of_listings - amount_of_listings
    logger.debug(f"New amount of listings is: {new_amount_of_listings}. Meaning amount of new listings adedd is:{new_amount_of_listings - amount_of_listings}")

    # Update last succesfull scrape date to today
    metadata.last_successful_scrape_date = metadata.last_scrape_date

    # Loadd all new metadata into the metadata json file
    logger.debug("Trying to dump new metadata info into JSON file.")
    metadata.update()
    logger.info("Successfully updated the metadata file.")

    # Create the CSV of active listings
    create_active_listings_csv(connection)

    connection.close()


if __name__ == "__main__":
    set_up_logging()
    main()
    sys.exit(0)
