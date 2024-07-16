import re
from sickle import Sickle
from typing import List, Tuple
import requests
from bs4 import BeautifulSoup
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

num_dict = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
    "hundred": 100,
    "thousand": 1000,
}


def text_to_int(text):
    if text.lower() in num_dict:
        return num_dict[text.lower()]
    return None


def parse_textual_numbers(s):
    s = s.lower().replace("-", " ")
    parts = s.split()
    total = 0
    current = 0

    for part in parts:
        number = text_to_int(part)
        if number:
            if number >= 100:
                current *= number
            else:
                current += number
        else:
            if current != 0:
                total += current
                current = 0

    total += current
    return total


def extract_number_of_pages(format_string):
    wanted = re.search(r"(\d+|[\w\s-]+)\s*pages", format_string, re.IGNORECASE)
    if wanted:
        number_text = wanted.group(1)
        return parse_textual_numbers(number_text)
    return None


def get_record_data() -> List[Tuple[str, str, int]]:
    sickle = Sickle("https://texashistory.unt.edu/explore/collections/THRSH/oai/")
    records = sickle.ListRecords(metadataPrefix="oai_dc")
    data = []
    for record in records:
        metadata = record.metadata
        date = metadata.get("date", [""])[0]
        url = metadata.get("identifier", [])[1]
        format_description = metadata.get("format", [""])[0]
        pages = extract_number_of_pages(format_description)
        data.append((date, url, pages))
    return data


def get_ocr_text(session, url):
    response = session.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    ocr_text = soup.find(id="ocr-data").pre.get_text(strip=True)
    return ocr_text


def process_record(session, date, url, pages):
    path = f"data/{date}"
    os.makedirs(path, exist_ok=True)
    for i in range(1, pages + 1):
        ocr_text = get_ocr_text(session, f"{url}/m1/{i}")
        with open(f"{path}/{i}.txt", "w") as f:
            f.write(ocr_text)


if __name__ == "__main__":
    print("Scraping data...")
    data = get_record_data()
    os.makedirs("data", exist_ok=True)
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(process_record, session, date, url, pages)
                for date, url, pages in data
            ]
            for future in as_completed(futures):
                future.result()
    print("Data scraped successfully.")
