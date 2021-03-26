# HTML scrape lib
import requests
from bs4 import BeautifulSoup
from time import sleep
from random import randint
# ETL lib
import re
import numpy as np
import pandas as pd
# Postgres DB
from sqlalchemy import create_engine
import psycopg2


def immobiliare_scraper(url):
    """
    Scraper for Immobiliare search, working as of 25 March 2021
    It takes the page url and identifies the number of pages to scrape and creates the object of the HTML
    """
    # create a bs4 object read in html
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    # takes the page number at the end of the HTML
    n_pages = int(soup.find_all('li', class_='disabled')[-1].text)

    # create variable to store page numbers to iterate over
    # create list of pages to iterate over
    pages = list(np.arange(1, n_pages, 1))
    tmp_results = []
    complete_results = []
    counter = 0

    # iterate over multiple pages and get the request
    while counter <= len(pages):
        page = requests.get(re.sub('pag=(\d+)', r'pag='+str(counter), url))
        # create a bs4 object read in html
        soup = BeautifulSoup(page.content, 'html.parser')
        sleep(randint(2, 10))
        tmp_results.append(soup)
        counter += 1

    # iterate over pages and extract text
    for ix, soup in enumerate(tmp_results):
        # here we get the content of each ads. We inspected the HTML structure here: https://webformatter.com/html
        complete_results.append(soup.find_all(
            'div', class_='listing-item_body--content'))

    return complete_results


def immobiliare_html_to_df(html_scraped):
    # initialize the structure of the dictionary
    dict_ = {
        'name': [],
        'summary': [],
        'url': [],
        'price': [],
        'rooms': [],
        'sqm': [],
        'baths': [],
        'floors': []
    }

    for ix, page in enumerate(html_scraped):
        for elem in page:
            # extract name of listing
            name = elem.find_all('p', class_="titolo text-primary")

            for i, val in enumerate(name):
                if val.find('a')['title'] is not None:
                    dict_['name'].append(str(val.find('a')['title']))
                else:
                    None

            # extract short summary
            summary = elem.find_all('p', class_="descrizione__truncate")
            for i, val in enumerate(summary):
                if val.text is not None:
                    dict_['summary'].append(val.text)
                else:
                    None

             # extract the ad link
            if elem.find('a')['href'] is not None:
                dict_['url'].append(elem.find('a')['href'])
            else:
                None

            # extract the apt.price and trasnfrom to int. when not available we default ot 0
            if elem.find('li', class_="lif__item lif__pricing") is not None:
                dict_['price'].append(
                    int(re.sub(r"[^a-zA-Z0-9]+", ' ',
                               elem.find('li', class_="lif__item lif__pricing").text.strip()).replace(" ", "")
                        )
                )
            else:
                if elem.find('li', class_="lif__item lif__pricing--wrapped").text.strip().replace(" ", "") == "PREZZOSURICHIESTA":
                    dict_['price'].append('private_treaty')

            # extract floor value
            if elem.find('abbr', class_="text-bold im-abbr") is not None:
                dict_['floors'].append(elem.find(
                    'abbr', class_="text-bold im-abbr").get('title').strip().replace(" ", ""))
            else:
                dict_['floors'].append('na')

            # extract apt features. the structure is rooms, sqm, baths, floor level
            for i in range(0, len(elem.find_all('span', class_="text-bold"))):
                if i == 0:
                    if elem.find_all('span', class_="text-bold") is not None:
                        dict_['rooms'].append(elem.find_all(
                            'span', class_="text-bold")[i].text.strip().replace(" ", ""))
                    else:
                        dict_['rooms'].append("null"+str(elem))

                if i == 1:
                    if elem.find_all('span', class_="text-bold") is not None:
                        dict_['sqm'].append(elem.find_all(
                            'span', class_="text-bold")[i].text.strip().replace(" ", ""))
                    else:
                        dict_['sqm'].append("null"+str(elem))
                if i == 2:
                    if elem.find_all('span', class_="text-bold") is not None:
                        dict_['baths'].append(elem.find_all(
                            'span', class_="text-bold")[i].text.strip().replace(" ", ""))
                    else:
                        dict_['baths'].append("null"+str(elem))

    # initialize the df and manipulate the data
    tmp = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in dict_.items()]))
    tmp['id'] = tmp['name'].replace(" ", "", regex=True).replace(
        ",", "", regex=True)+'_'+tmp['price'].astype(str)+'_'+tmp['sqm'].astype(str)+'_'+tmp['floors'].astype(str)+'_'+tmp['rooms'].astype(str)
    tmp = tmp[['id', 'name', 'summary', 'price',
               'sqm', 'rooms', 'baths', 'floors', 'url']]

    tmp.drop_duplicates(subset='id', keep="last")

    return tmp


def initialize_db_extract_immobiliare(html_scraped, table_name):
    """
    This function extract the information based on a fixed HTML structure (valid as of 22Mar2021) and import it to a Postgres DB.
    This function is called only the first time, when the DB is not present. If the DB is present, the function shouldn't be called
    to avoid duplicate insertion.
    """

    # create a connection with the database, we use psycopg2 to create the table
    try:
        conn = psycopg2.connect(database="postgres", user="manfredi",
                                password="manfredi", host="localhost", port="5432")
    except:
        print("I am unable to connect to the database")

    cur = conn.cursor()

    # check wether the table "immobiliare" is already present and initialized.
    # If not, we run the script to initialize and populate for the first time
    cur.execute(
        "select * from information_schema.tables where table_name=%s", (table_name,))
    check = bool(cur.rowcount)

    if check == False:  # table doesn't exist yet

        if table_name == 'immobiliare_rent':

            # we initialize the table with the format we need
            try:
                cur.execute("""
                    DROP TABLE IF EXISTS immobiliare_rent;
                    CREATE TABLE IF NOT EXISTS immobiliare_rent
                    (
                        id text, 
                        name text,
                        summary text,
                        price text,
                        sqm text, 
                        rooms text,
                        baths text, 
                        floors text,
                        url text,
                        created_at timestamp without time zone NOT NULL DEFAULT NOW(),
                        updated_at timestamp without time zone DEFAULT NULL,
                        --CONSTRAINT immobiliare_rent_key PRIMARY KEY (id)
                    )
                    WITH (
                        OIDS = FALSE
                    )
                    TABLESPACE pg_default;
                    ALTER TABLE immobiliare_rent
                        OWNER to manfredi;
                    CREATE INDEX immobiliare_rent_id ON immobiliare_rent(id, name, sqm, price);""")
                print('Table inisitalized')
            except:
                print("Something wrong happened!")

            conn.commit()  # <--- makes sure the change is shown in the database
            conn.close()
            cur.close()

        elif table_name == "immobiliare_sales":
            # we initialize the table with the format we need
            try:
                cur.execute("""
                    DROP TABLE IF EXISTS immobiliare_sales;
                    CREATE TABLE IF NOT EXISTS immobiliare_sales
                    (
                        id varchar(255), 
                        name varchar(255),
                        summary varchar(255),
                        price varchar(255),
                        sqm varchar(255), 
                        rooms varchar(255),
                        baths varchar(255), 
                        floors varchar(255),
                        url varchar(255),
                        created_at timestamp without time zone NOT NULL DEFAULT NOW(),
                        updated_at timestamp without time zone DEFAULT NULL,
                        --CONSTRAINT immobiliare_sales_key PRIMARY KEY (id)
                    )
                    WITH (
                        OIDS = FALSE
                    )
                    TABLESPACE pg_default;
                    ALTER TABLE immobiliare_sales
                        OWNER to manfredi;
                    CREATE INDEX immobiliare_sales_id ON immobiliare_sales(id, name, sqm, price);""")
                print("table initialized")
            except:
                print("Something wrong happened!")

            conn.commit()  # <--- makes sure the change is shown in the database
            conn.close()
            cur.close()

        tmp = immobiliare_html_to_df(html_scraped)
        # we use sqlalchemy to load the data to Postgres
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/postgres'
                               .format('manfredi',  # username
                                       'manfredi',  # password
                                       'localhost',  # host
                                       '5432'  # local port
                                       ), echo=False)

        if table_name == "immobiliare_rent":
            # we load into Postgres table created
            tmp.to_sql('immobiliare_rent', engine, if_exists='append', index=False,
                       chunksize=1000, method='multi')
            print('The table immobiliare_rent has been initialized with ' +
                  str(len(tmp))+' rows')
        elif table_name == "immobiliare_sales":
            # we load into Postgres table created
            tmp.to_sql('immobiliare_sales', engine, if_exists='append', index=False,
                       chunksize=1000, method='multi')
            print('The table immobiliare_rent has been initialized with ' +
                  str(len(tmp))+' rows')
    else:
        print('The database is already initialized')


def insert_immobiliare(html_scraped, table_name):
    """
    Function to insert successive scrape after the DB it has been initialized for the first time. 
    """
    # we use sqlalchemy to connect to the DB
    engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/postgres'
                           .format('manfredi',  # username
                                   'manfredi',  # password
                                   'localhost',  # host
                                   '5432'  # local port
                                   ), echo=False)

    if table_name == 'immobiliare_rent':
        # we pull the ids that are already in the table
        ids = pd.read_sql(
            """select id from public.immobiliare_rent""", con=engine)
    elif table_name == "immobiliare_sales":
        ids = pd.read_sql(
            """select id from public.immobiliare_sales""", con=engine)

    tmp = immobiliare_html_to_df(html_scraped)

    # yields the elements in `list_2` that are NOT in `list_1`
    diff_list = list(np.setdiff1d(
        list(tmp.id.astype(str)), list(ids['id'].astype(str))))

    # select rows based on diff list items. We will import only rows that are not yet present
    tmp = tmp[tmp['id'].isin(diff_list)]

    if table_name == 'immobiliare_rent':
        # we pull the ids that are already in the table
        tmp.to_sql('immobiliare_rent', engine, if_exists='append', index=False,
                   chunksize=1000, method='multi')
    elif table_name == "immobiliare_sales":
        tmp.to_sql('immobiliare_sales', engine, if_exists='append', index=False,
                   chunksize=1000, method='multi')
    # we load into Postgres table created

    print('Inserted '+str(len(tmp))+' rows')
