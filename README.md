# Motorcycle Listings Scraper

A Python scraper for retrieving motorcycle listings from [yad2](https://www.yad2.co.il/vehicles/motorcycles) and storing them in an SQLite database. The scraper also generates a CSV file of active listings and maintains metadata about the scrape.


## Demo
You can acess a a live powerBI dashboard of the data here: https://app.powerbi.com/view?r=eyJrIjoiNmMyNWUxYmUtNGQ2MS00ZjFlLWI1ZDEtZmY5YTlmYjY5NTg2IiwidCI6Ijg0NTdhZDk1LTBkM2YtNDcwNC1iNWMwLTI3MTYzNzJlY2IxZCJ9

## Features

- Scrapes motorcycle listings from [yad2](https://www.yad2.co.il/vehicles/motorcycles)
- Stores listings in an SQLite database
- Marks inactive listings based on scrape status
- Exports active listings to a CSV file
- Tracks metadata (e.g., build ID, listings added/removed)

## Getting Started

Follow these steps to set up and run the scraper locally:

### 1. Clone the Repository

```bash
git clone https://github.com/Morozov173/yad2-motorcycle-scraper
```

### 2. Install Dependencies
This repositry uses [uv](https://github.com/astral-sh/uv) for dependency management 
```bash
uv sync
```
### 3. Set Up Environment Variables
Create a .env file based on the provided .env.example file if you wish to use proxies.
| Name            | Optional | Description                                                         |
|-----------------|----------|---------------------------------------------------------------------|
| PROXY_USERNAME  | - [ ]    | The username used to authenticate with the proxy server.            |
| PROXY_PASSWORD  | - [ ]    | The password that pairs with `PROXY_USERNAME` for proxy access.     |
| PROXY_SERVER    | - [ ]    | The hostname (or IP and port) of the proxy server to route through. |


### 4. Run
```bash
uv run main.py
```
