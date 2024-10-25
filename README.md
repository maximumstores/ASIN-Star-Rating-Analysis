# Check-ASIN-Star-Rating-Analysis

Check ASIN: Star Rating Analysis
A tool designed to automate the process of collecting, analyzing, and monitoring Amazon product ratings and reviews by ASIN. This script gathers star-level data, tracks ratings over time, updates Google Sheets, and sends alerts for low-rated products to a specified Telegram chat.

Features
Data Collection: Retrieves product information such as ratings, review counts, and star-level breakdowns (1-5 stars) from Amazon.
Regional Adaptability: Supports multiple Amazon locales (e.g., com, co.uk, de, etc.) with locale-specific parsing and formatting.
Google Sheets Integration: Updates data in Google Sheets with structured rows for each ASIN, including daily rating and review metrics.
Alerting System: Sends Telegram notifications if a product’s rating falls below an acceptable threshold over consecutive days.
Rate Limiting and Error Handling: Implements exponential backoff for API requests, ensuring reliability and compliance with rate limits.
Getting Started
Prerequisites
Python 3.7+
Required libraries: requests, BeautifulSoup4, gspread, oauth2client, telebot, babel, pytz, logging
Install dependencies:

bash
Копировать код
pip install requests beautifulsoup4 gspread oauth2client telebot babel pytz
Setup
Google Sheets API Credentials:
Place your Google Sheets JSON credentials file (e.g., maximumstores53-24d4ef8c1298.json) in the project folder. Update the path in the config section as needed.

Configuring Google Sheets:
Ensure your Google Sheets document has the following structure:

Config Sheet with Key and Value columns for configuration settings (e.g., product_urls, min_acceptable_rating).
Data Sheet titled "Customer reviews" to store ASIN details and daily metrics.
Telegram Bot:

Set up a bot and get your telegram_bot_token and telegram_chat_id.
Add these details to the configuration sheet.
Oxylabs API Credentials:

Obtain credentials from Oxylabs for Amazon data scraping.
Add oxylabs_username and oxylabs_password to the config.
Configuration
Ensure that Config sheet in Google Sheets includes:

product_urls: List of Amazon URLs to monitor.
update_time_hour and update_time_minute: Time (24-hour format) for daily updates.
min_acceptable_rating: The minimum acceptable rating for triggering alerts.
timezone: Timezone for scheduling (e.g., Europe/Kiev).
Running the Script
Execute the script:

bash
Копировать код
python check_asin_star_rating_analysis.py
This will:

Fetch data for each ASIN from Amazon using Oxylabs.
Update Google Sheets with the latest rating and review counts.
Send a Telegram message with any ASINs that have fallen below the acceptable rating threshold.
Key Functions
scrape_amazon_product: Main function for scraping product details and ratings from Amazon.
update_google_sheets: Writes or updates ASIN data in Google Sheets.
send_telegram_message: Sends alert to Telegram for low-rated products.
check_low_rating_for_multiple_days: Tracks ratings history and flags ASINs with consecutive low ratings.
Logging and Debugging
Logs provide detailed information on request status, Google Sheets updates, and Telegram notifications. Errors are logged to assist with debugging.

License
This project is open-source and free to use under the MIT License.
