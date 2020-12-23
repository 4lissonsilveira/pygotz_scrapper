import sys
import json

from concurrent import futures

import requests

from bs4 import BeautifulSoup


def extract_elements(soup):
    return soup.find_all('span', {'class':'LabelV'})


def convert_to_json(elements):
    elem_values = {}
    for element in elements:
        elem_values[element.text] = element.next_sibling.text

    return json.dumps(elem_values)

def open_url_and_parse_content(url):
    r = requests.get(url)
    return BeautifulSoup(r.content, 'html.parser')


def scrap(url):
    soup = open_url_and_parse_content(url)
    elements = extract_elements(soup)
    if elements:
        json_data = convert_to_json(elements)
        return json_data


def main(start_id, end_id, filename):
    print('Retrieving data')
    url = 'https://www.tibia.com/charactertrade/?subtopic=pastcharactertrades&page=details&auctionid={}&source=overview'
    ids = range(start_id, end_id + 1)
    all_urls = [url.format(_ids) for _ids in ids]

    with futures.ThreadPoolExecutor(max_workers=50) as executor:
        results = executor.map(scrap, all_urls)

    print('All the data have been retrieved')
    print('Exporting data to the file {}'.format(filename))

    with open(filename, 'a') as file:
        row_count = 0
        for item in results:
            row_count += 1
            if item:
                file.write(item)
                file.write('\n')

    print('The scrapping has finished.')

if __name__ == '__main__':
    print('The scraping has started')
    start_id, end_id, filename = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
    main(start_id, end_id, filename)
