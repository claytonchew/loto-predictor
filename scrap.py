import csv
import requests
import datetime

from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

LOTOS = [
    {"name": "TOTO50", "max_page": 15},
    {"name": "TOTO55", "max_page": 62},
    {"name": "TOTO58", "max_page": 60},
    {"name": "TOTO63", "max_page": 16},
]


def generate_url(toto, page):
    return f"https://www.lotto-8.com/Malaysia/listlto{toto}.asp?indexpage={str(page)}"


def parse(url):
    html = requests.get(url)
    soup = BeautifulSoup(html.text, "lxml")
    table = soup.find("table")

    output_rows = []
    for row in table.findAll("tr"):
        columns = row.findAll("td")
        output_row = []
        for col in columns:
            output_row.append(col.get_text(strip=True).replace("\xa0", " "))
        output_rows.append(output_row)

    return output_rows[1:]


def append_to_csv(rows, filename):
    with open(filename, "a") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(rows)


if __name__ == "__main__":

    start_time = datetime.datetime.now()
    print("Starting scrapper...")

    for loto in LOTOS:

        print(f"Scraping {loto['name']}...")

        with ThreadPoolExecutor() as e:
            urls = [
                generate_url(loto["name"], page)
                for page in range(1, loto["max_page"] + 1)
            ]
            results = e.map(parse, urls)

            for result in results:
                append_to_csv(result, f"{loto['name']}.csv")

        print("Done!")

    print(f"Scrap finished. ({datetime.datetime.now() - start_time})")

