import os
import argparse
import asyncio
import requests
import threading
import peewee
import wget
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from database_manager import DatabaseManager
import local_settings


parser = argparse.ArgumentParser()
parser.add_argument(
    'phrase',
    type=str,
    help='the phrase to search',
)

parser.add_argument(
    '--format',
    type=str,
    choices=['xls', 'csv', 'json'],
    default='csv',
    help='Specify the type of output file'
)

args = parser.parse_args()

# TODO: adding argparse
# TODO: modifying database -> removing repeated data, adding save location
# TODO: adding zipfile save
# TODO: fixing problems with download files to separate locations
# TODO: add Threading

database_manager = DatabaseManager(
    database_name=local_settings.DATABASE['name'],
    user=local_settings.DATABASE['user'],
    password=local_settings.DATABASE['password'],
    host=local_settings.DATABASE['host'],
    port=local_settings.DATABASE['port'],
)

df = pd.DataFrame(columns=[
    'ID',
    'author',
    'title',
    'publisher',
    'year',
    'page',
    'language',
    'size',
    'type',
])


class ScrapedData(peewee.Model):
    phrase = peewee.CharField(
        max_length=200, null=False, verbose_name='Phrase')
    ID = peewee.CharField(max_length=20, null=False, verbose_name='ID')
    author = peewee.CharField(null=False, verbose_name='Author')
    title = peewee.CharField(null=False, verbose_name='Title')
    publisher = peewee.CharField(max_length=50, verbose_name='Publisher')
    year = peewee.CharField(max_length=20, verbose_name='Year')
    page = peewee.CharField(max_length=20, verbose_name='Page')
    language = peewee.TextField(null=False, verbose_name='language')
    size = peewee.CharField(max_length=20, null=False, verbose_name='Size')
    type = peewee.CharField(max_length=20, null=False, verbose_name='Type')

    class Meta:
        database = database_manager.db


async def main():
    try:

        counter = 1
        while True:
            url = f'https://libgen.is/search.php?req=/{args.phrase.replace(" ", "+")}&page={counter}'
            text = requests.get(url, timeout=10)
            print('connected to site')
            if text.status_code == requests.codes.ok:
                content = BeautifulSoup(text.text, 'html.parser')
                table = content.find('table', class_='c')
                tr = table.find_all('tr')
                if len(tr) > 1:
                    main_path = (os.getcwd() +
                                 f'\\{args.phrase}_' +
                                 f'{datetime.now().date()}'
                                 )
                    if not os.path.exists(main_path):
                        os.makedirs(main_path, exist_ok=True)
                    else:
                        pass
                    await scrapper(tr, main_path)
                else:
                    print(f"there are no result for your search: {args.phrase}")
                    break
            else:
                print(f"The site isn't responding {requests.status_codes}")
                break
            counter += 1
    except requests.exceptions.ConnectionError as error:
        print(f'Connection Error {error}')
    except EOFError as error:
        print(f'EOFError {error}')
    except AttributeError as error:
        print(f'Attribute Error {error}')


async def scrapper(soup, temp_dir):
    print('Scrapper started')
    global df
    data_scrape = []
    try:
        for i in range(1, len(soup)):
            for m, j in enumerate(soup[i].find_all('td')):
                if m == 1:
                    data_scrape.append(','.join([element.text for element in j.find_all('a')]))
                    continue

                if m == 2:
                    title = [element.text for element in j.find('a').contents][0]
                    data_scrape.append(title)
                    link = j.find('a').get('href')
                    temp_url = f'https://libgen.is/{link}'

                    image_downloader(temp_url,temp_dir)
                    continue

                if m == 9:
                    temp_url = j.find('a').get('href')
                    file_downloader(temp_url, temp_dir)
                data_scrape.append(j.text)

            df = df._append({
                'ID': data_scrape[0],
                'author': data_scrape[1],
                'title': data_scrape[2],
                'publisher': data_scrape[3],
                'year': data_scrape[4],
                'page': data_scrape[5],
                'language': data_scrape[6],
                'size': data_scrape[7],
                'type': data_scrape[8],
            }, ignore_index=True)
            data_scrape.clear()

    except AttributeError as error:
        print(f'AttributeError {error}')
    except ValueError as error:
        print(f'Value error: {error}')
    except TypeError as error:
        print(f'Type error {error}')


def image_downloader(link, base_dir):
    try:
        text = requests.get(link, timeout=10).text
        soup = BeautifulSoup(text, 'html.parser')
        tr = soup.find('table').find_all('tr')
        for i, j in enumerate(tr):
            if i == 1:
                image_url = f"https://libgen.is/{j.find('a').find('img').get('src')}"
                wget.download(
                    url=image_url,
                    out=base_dir,
                    bar=wget.bar_adaptive
                )
    except requests.exceptions.ConnectionError as error:
        print('error', error)
    except AttributeError as error:
        print(f'Attribute Error {error}')


def file_downloader(link, base_dir):
    try:
        text = requests.get(link, timeout=10).text
        link_file = BeautifulSoup(text, 'html.parser').find('h2').find('a').get('href')
        wget.download(link_file,
                      out=base_dir,
                      bar=wget.bar_adaptive
                      )
    except requests.exceptions.ConnectionError as error:
        print(f'Connection Error {error}')


def database_creator():
    global df
    df = df.drop_duplicates(subset=['title'])
    for k in range(len(df)):
        if not ScrapedData.select().where(ScrapedData.title == df.loc[k, 'title']).exists():
            ScrapedData.create(
                phrase=args.phrase,
                ID=df.loc[k, 'ID'],
                author=df.loc[k, 'author'],
                title=df.loc[k, 'title'],
                publisher=df.loc[k, 'publisher'],
                year=df.loc[k, 'year'],
                page=df.loc[k, 'page'],
                language=df.loc[k, 'language'],
                size=df.loc[k, 'size'],
                type=df.loc[k, 'type'],
            )


def export_data(model):
    if model == 'csv':
        df.to_csv(str(os.getcwd()) + '.csv', index=False)
    elif model == 'json':
        df.to_json(str(os.getcwd()) + '.json', index=False)
    elif model == 'xls':
        df.to_excel(str(os.getcwd()) + '.xlsx', index=False)


if __name__ == '__main__':
    try:
        database_manager.create_tables(models=[ScrapedData])
        asyncio.run(main())
        database_creator()
        export_data(args.format)
    except peewee.OperationalError as e:
        print('Error', e)
    finally:
        if database_manager.db:
            database_manager.db.close()
            print('Database connection is closed')
