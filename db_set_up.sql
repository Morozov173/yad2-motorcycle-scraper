CREATE TABLE IF NOT EXISTS motorcycle_listings(
    listing_id INTEGER PRIMARY KEY,
    creation_date DATE NOT NULL,
    brand TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_year INTEGER NOT NULL,
    engine_displacement INTEGER NOT NULL,
    license_rank TEXT NOT NULL,
    kilometrage INTEGER NOT NULL,
    amount_of_owners INTEGER NOT NULL DEFAULT 1,
    color TEXT,
    listed_price REAL,
    active BOOLEAN NOT NULL,
    last_seen DATE DEFAULT (date('now'))
)