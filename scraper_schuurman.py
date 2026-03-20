# omdat prijzen nog steeds niet goed zijn op de site van schuurman, deze scraper gebruiken om prijzen te verkrijgen

import configparser
from datetime import datetime
import os
import platform
import time
import random
import httpx
import pandas as pd
import re
from lxml import html
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from pathlib import Path


date_now = datetime.now().strftime("%c").replace(":", "-")
ini_config = configparser.ConfigParser(interpolation=None)
ini_config.read(Path.home() / "bol_export_files.ini")
dropbox_key = os.environ.get("DROPBOX")
delay = 30

def browser_chrome_lin():
    options = webdriver.ChromeOptions()
    prefs = {
        "profile.default_content_settings": 2,
        "profile.managed_default_content_settings.images": 2,
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--start-maximized")
    return webdriver.Chrome(options=options)

def browser_chrome_webdriver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--enable-managed-downloads = True")

    prefs = {
        "profile.default_content_settings": 2,
        "profile.managed_default_content_settings.images": 2,
    }
    options.add_experimental_option("prefs", prefs)
    
    return webdriver.Remote(
        command_executor="http://toop.nl:44440",
        options=options,
    )
if platform.node() in ("fedora-vangils", "fedora", "fedora-work","school-pc","fedora-thuis"):
    driver = browser_chrome_lin()
else:
    driver = browser_chrome_webdriver()

def login():

    driver.get("https://www.schuurman-ce.nl/shop")
    # wait for email field and enter email
    WebDriverWait(driver, delay).until(
        EC.element_to_be_clickable((By.XPATH, "//input[@id='login']"))
    ).send_keys(ini_config.get("schuurman website", "email"), Keys.ENTER)

    # wait for password field and enter password
    WebDriverWait(driver, delay).until(
        EC.element_to_be_clickable((By.XPATH, "//input[@id='password']"))
    ).send_keys(ini_config.get("schuurman website", "passwd"), Keys.ENTER)

    # Click to stay logged in

login()

print("Waiting 10 seconds for cookies to be set...")
time.sleep(10)

# Convert Selenium list of dicts cookies to a standard dict {name: value}
cookies = {c['name']: c['value'] for c in driver.get_cookies()}
user_agent = driver.execute_script("return navigator.userAgent")

driver.quit()

url = "https://www.schuurman-ce.nl/shop"

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'nl,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.schuurman-ce.nl/my',
    'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': user_agent
}

def scrape_page(client, url):
    response = None
    for attempt in range(3):
        try:
            response = client.get(url, headers=headers)
            break
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError) as e:
            print(f"Network error {e} on {url}, attempt {attempt+1}. Waiting 20s...")
            time.sleep(20)
            
    if response is None:
        print(f"Could not fetch {url} after 3 attempts. Stopping.")
        return [], None

    if response.status_code == 403 or "403: Verboden" in response.text:
        print(f"Encountered 403 Forbidden on {url}. Trying to force next page...")
        match = re.search(r'(/page/)(\d+)', url)
        if match:
            current_page = int(match.group(2))
            new_url = re.sub(r'/page/\d+', f'/page/{current_page + 1}', url)
            print(f"Generated next URL: {new_url}")
            return [], new_url
        print("Could not generate next URL from 403 page.")
        return [], None

    tree = html.fromstring(response.text)
    hoofdindeling_page = tree.xpath('//*[@id="products_grid"]/div/table//div/form')
    
    products = []
    for product in hoofdindeling_page:
        try:
            product_name = product.xpath('.//div/h6/a/text()')[0].strip()
            product_price = product.xpath('.//div[contains(@class, "product_price")]//span[@class="oe_currency_value"]/text()')[0].strip().replace(",", "")
            ean = product.xpath('.//div/h6/span[2]/text()')[0].strip()
            products.append({"name": product_name, "price": product_price, "ean": ean})
        except IndexError:
            continue

    # Prepare for next page
    next_page_xpath = '//ul[contains(@class, "pagination")]//li/a[.//span[contains(@class, "fa-chevron-right")]]/@href'
    next_page = tree.xpath(next_page_xpath)
    
    next_url = None
    if next_page:
        next_url = next_page[0]
        if not next_url.startswith("http"):
            next_url = "https://www.schuurman-ce.nl" + next_url
            
    return products, next_url

all_products = []

with httpx.Client(cookies=cookies, timeout=120.0, follow_redirects=True) as client:
    current_url = url
    while current_url:
        print(f"Scraping: {current_url}")
        products, current_url = scrape_page(client, current_url)
        all_products.extend(products)
        if current_url:
            sleep_time = random.uniform(5, 10)
            print(f"Waiting {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

df = pd.DataFrame(all_products)
filename = f"Schuurman_scrape_{date_now}.csv"
df.to_csv(filename, index=False)
print(f"Saved {len(all_products)} products to {filename}")
