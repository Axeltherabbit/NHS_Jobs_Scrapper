from contextlib import asynccontextmanager
from requests import get
from bs4 import BeautifulSoup
import openrouteservice
from dotenv import load_dotenv, dotenv_values
import sqlite3
from telegram import Bot
from telegram.error import RetryAfter
import asyncio
import logging

logging.basicConfig(
    filename='app.log',
    level=logging.ERROR,  # Log only ERROR and higher severity messages
    format='%(asctime)s - %(levelname)s - %(message)s',
)

if not dotenv_values()["OSM_TOKEN"]:
    raise Exception("Missing Token for openrouteservice")
if not dotenv_values()["TELEGRAM_TOKEN"]:
    raise Exception("Missing Token for telegram")
if not dotenv_values()["CHANNEL_ID"]:
    raise Exception("Missing Channel id for telegram")
if not dotenv_values()["KEYWORDS"]:
    raise Exception("Missing keywords list to search [comma separated]")
if not dotenv_values()["PAY_RANGES"]:
    raise Exception("Missing pay ranges list to search [comma separated] ie 30-40,40-50")
if not dotenv_values()["DOMAIN"]:
    raise Exception("Missing domain, usually: https://www.jobs.nhs.uk [include protocol]")
if not dotenv_values()["ORIGIN_ADDRESS"]:
    raise Exception("Missing origin address, this can be any address in the UK")



OSMClient = openrouteservice.Client(key=dotenv_values()["OSM_TOKEN"])
origin = dotenv_values()["ORIGIN_ADDRESS"]
origin_coords = OSMClient.pelias_search(origin)['features'][0]['geometry']['coordinates']

channel_id = dotenv_values()["CHANNEL_ID"]

conn = sqlite3.connect('jobs.db')
cursor = conn.cursor()

keywords = dotenv_values()["KEYWORDS"].split(",")
pay_ranges = dotenv_values()["PAY_RANGES"].split(",")
domain = dotenv_values()["DOMAIN"]

def get_and_handle_err(request):
    res = get(request)

    if not res.ok or not (200 <= res.status_code <= 299):
        raise Exception("Response not ok", res.status_code, request, res.text)
    return res
     
def search_query():
    for keyword in keywords:
        for pay_range in pay_ranges:
            page = 1
            while True:
                request = f"{domain}/candidate/search/results?keyword={keyword}&page={page}&payRange={pay_range}" 
                res = get_and_handle_err(request)

                print(keyword, pay_range, "page", page)
                soup = BeautifulSoup(res.text, "html.parser")
                links = soup.find_all("a", {"data-test" : "search-result-job-title"})
                
                for job in links:
                    if does_record_exist(job["href"]): continue
                    parse_job(job["href"])

                try:
                    m_page = int(soup.find("span", {"class" : "nhsuk-pagination__page"}).get_text().split("of")[1]) 
                except AttributeError:
                    m_page = 9999

                if page <= m_page: break
                else: page +=1 


def TryGetSalary(soup):
    try:
        return soup.find("p", {"id": "fixed_salary"}).get_text().replace("\n\n"," ").strip()
    except AttributeError: pass
    try:
        return soup.find("p", {"id": "range_salary"}).get_text().replace("\n\n"," ").strip()
    except AttributeError: pass
    try:
        return soup.find("p", {"id": "negotiable_salary"}).get_text().replace("\n\n"," ").strip()
    except AttributeError: pass

    raise Exception("Salary not found")


def TryGetTitle(soup):
    try:
        return soup.find("h1", {"class": "nhsuk-heading-xl nhsuk-u-margin-bottom-2 word-wrap"}).get_text()
    except AttributeError: pass
    raise Exception("Title not found")


def TryGetAddress(soup):
    try:
        adr_1 = soup.find("p", {"id": "employer_address_line_1_a"}).get_text().strip()
        adr_2 = soup.find("p", {"id": "employer_address_line_2_b"}).get_text().strip()
        town = soup.find("p", {"id": "employer_town_c"}).get_text().strip()
        county = soup.find("p", {"id": "employer_county_c"}).get_text().strip()
        postcode = soup.find("p", {"id": "employer_postcode_e"}).get_text().strip()

        return "\n".join([adr_1, adr_2, town, county, postcode])
    except AttributeError: pass
    try:
        adr_1 = soup.find("p", {"id": "employer_address_line_1"}).get_text().strip()
        adr_2 = soup.find("p", {"id": "employer_address_line_2"}).get_text().strip()
        town = soup.find("p", {"id": "employer_town"}).get_text().strip()
        county = soup.find("p", {"id": "employer_county"}).get_text().strip()
        postcode = soup.find("p", {"id": "employer_postcode"}).get_text().strip()

        return "\n".join([adr_1, adr_2, town, county, postcode])
    except AttributeError: pass
    
    raise Exception("Address not found")

def CalcDistanceByCar(address):
    try:
        destination_coords = OSMClient.pelias_search(address)['features'][0]['geometry']['coordinates']
        
        # Get directions between the two locations
        routes = OSMClient.directions(
            coordinates=[origin_coords, destination_coords],
            profile='driving-car',
            format='geojson'
        )
    except (openrouteservice.exceptions.ApiError, IndexError):
        return "Api is fucked up :)"

    distance = routes['features'][0]['properties']['segments'][0]['distance'] / 1000  # in kilometers
    duration = routes['features'][0]['properties']['segments'][0]['duration'] / 3600  # in hours
    hours = int(divmod(duration, 1)[0])
    minutes = int(divmod(duration, 1)[1] * 60)
    return f"Distance: {distance} KM\nDuration: {hours} hour{"s" if hours > 1 else ""} {minutes} minutes"


def does_record_exist(path):
    path = path.split("?", 1)[0] # rem params
    cursor.execute("SELECT * FROM records WHERE path = ?", (path,))
    return cursor.fetchone()


def insert_record(path, salary, title, address, distance):
    path = path.split("?", 1)[0] # rem params
    # Check if the record with the given name already exists
    cursor.execute("SELECT * FROM records WHERE path = ?", (path,))
    existing_record = cursor.fetchone()

    if existing_record:
        raise Exception(f"Record with path '{path}' already exists.")
    else:
        # Insert the new record if it doesn't exist
        cursor.execute("INSERT INTO records (path, salary, title, address, distance) VALUES (?, ?, ?, ?, ?)", (path, salary, title, address, distance))
        conn.commit()
        print(f"Record with path '{path}' inserted.")

async def telegram_send_message(message):
    telegram_bot = Bot(token=dotenv_values()["TELEGRAM_TOKEN"])
    try:
        await telegram_bot.send_message(chat_id=channel_id, text=message)
    except RetryAfter:
        await asyncio.sleep(60)

def parse_job(path):
    path = path.split("?", 1)[0] # rem params
    res = get_and_handle_err(domain+path)
    soup = BeautifulSoup(res.text, "html.parser")
    salary = TryGetSalary(soup)
    title = TryGetTitle(soup)
    address = TryGetAddress(soup)
    distance = CalcDistanceByCar(address)
    print(title)
    print(f"salary\n{salary}")
    print(address)
    print(distance)
    message = f"{domain+path}\n\n{title}\n{salary}\n{address}\n{distance}"
    asyncio.run(telegram_send_message(message))
    insert_record(path, salary, title, address, distance)

             
def main():
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS records (
        path TEXT UNIQUE,
        salary varchar(1000),
        title varchar(1000),
        address varchar(1000),
        distance varchar(1000)
    )
    ''')
    search_query()

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logging.error(f"An error occurred: {err}", exc_info=True)
