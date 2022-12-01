# Imports
import os
import re
import requests
import logging

import pandas as pd
import numpy  as np

from datetime   import datetime
from bs4        import BeautifulSoup
from sqlalchemy import create_engine


# data collection
def data_collection(url, headers):

    # requesto to URL
    page = requests.get(url, headers=headers)

    # BeautifuylSoup object
    soup = BeautifulSoup(page.text, 'html.parser')

    # getting number of products
    total_item = soup.find_all('h2', class_='load-more-heading')[0].get('data-total')
    total_item

    # calculating number of pages
    page_number = np.ceil(int(total_item)/36)
    page_number

    # generating url
    url02 = url + '?page-size=' + str(int(page_number)*36)
    url02

    # request to new URL
    page = requests.get(url02, headers=headers)

    # BeautifulSoup object
    soup = BeautifulSoup(page.text, 'html.parser')

    # product details
    products = soup.find('ul', class_='products-listing small')
    product_list = products.find_all('article', class_='hm-product-item')

    # product id
    product_id = [p.get('data-articlecode') for p in product_list]

    # product category
    product_category = [p.get('data-category') for p in product_list]

    # product name
    product_list = products.find_all('a', class_='link')
    product_name = [p.get_text() for p in product_list]

    # price
    product_list = products.find_all('span', class_='price regular')
    product_price = [p.get_text() for p in product_list]

    # creating dataset
    data = pd.DataFrame([product_id, product_category, product_name, product_price]).T
    data.columns = ['product_id', 'product_category', 'product_name', 'product_price']

    # scrapy datetime
    data['scrapy_datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return data


def data_collection_by_product(data, headers):
    # empty dataframe for append
    df_color = pd.DataFrame()
    df_composition = pd.DataFrame()

    # iteration for each id product
    for code in data['product_id']:
        url02 = 'https://www2.hm.com/en_us/productpage.' + str(code) + '.html'
        logger.debug('Product: %s', url02)

        page = requests.get(url02, headers=headers)
        soup = BeautifulSoup(page.text, 'html.parser')
        
        # Product Color
        color_name = soup.find('a', class_='filter-option miniature active').get('data-color')

        # product id
        product_code = soup.find('a', class_='filter-option miniature active').get('data-articlecode')

        aux1 = pd.DataFrame({'product_id': product_code, 'color_name': color_name}, index=[0])
        df_color = pd.concat([df_color, aux1])

        # Product Composition 
        product_composition_list = soup.find_all('div', class_='details-attributes-list-item')

        product_composition = [list(filter(None, p.get_text().split('\n'))) for p in product_composition_list]
        
        composition = pd.DataFrame(product_composition).T

        # rename dataframe
        composition.columns = composition.iloc[0]

        # delete first row
        composition['Art. No.'] = composition['Art. No.'].fillna(method='ffill')
        composition = composition.iloc[1:]

        composition_aux = composition.fillna('').groupby(['Art. No.'], as_index=False).sum()

        df_composition = pd.concat([df_composition, composition_aux], axis=0)
        
    df_composition = df_composition[['Art. No.', 'Fit', 'Size', 'Composition', 'Additional material information']]
    df_composition.rename(columns={'Art. No.': 'product_id', 'Fit': 'fit', 'Size': 'size', 'Composition':
                                'composition', 'Additional material information': 'additional_material'},
                                inplace=True)

    # merging the dataframes
    df_details = pd.merge(df_color, df_composition, how='left', on='product_id')

    data = pd.merge(data, df_details, how='left', on='product_id')

    return data


def data_cleaning(data):
    # Data cleaning
    # product_name
    data['product_name'] = data['product_name'].apply(lambda x: x.replace(' ', '_').lower())

    # product_price
    data['product_price'] = data['product_price'].apply(lambda x: x.replace ('$ ', '')).astype(float)

    # scrapy_datetime
    data['scrapy_datetime'] = pd.to_datetime(data['scrapy_datetime'], format='%Y-%m-%d %H:%M:%S')

    # color_name
    data['color_name'] = data['color_name'].apply(lambda x: x.replace(' ', '_').lower())

    # Fit
    data['fit'] = data['fit'].apply(lambda x: x.replace(' ', '_').replace('/', '_').lower())

    # Composition - Shell
    data['shell_composition'] = np.NaN

    for index, line in data.iterrows():
        # since there is no pattern, different methods are implemented
        if re.match('Shell:.(.+%)\w', line['composition']):
            data.loc[index, 'shell_composition'] = re.match('Shell:.(.+%)\w', line['composition']).group(1)
        elif re.match('(.+)%\w', line['composition']):
            data.loc[index, 'shell_composition'] = re.match('(.+)%\w', line['composition']).group(1)
        else:
            data.loc[index, 'shell_composition'] = line['composition']

    # Composition - Lining
    for index, line in data.iterrows():
        if re.search('Pock.+: (.+)', line['composition']):
            data.loc[index, 'pocket_lining_composition'] = re.search('Pock.+: (.+)', line['composition']).group(1)

    # size
    data['size_number'] = np.NaN
    data['leg_lenght'] = np.NaN
    data['circumference'] = np.NaN

    for index, line in data.iterrows():
        # look for lines with text for extraction
        if pd.notnull(line['size']):
            if re.search('Length: (.{1,4}) cm', line['size']):
                data.loc[index, 'leg_lenght'] = re.search('Length: (.{1,4}) cm', line['size']).group(1)
            if re.search('Circumference: (.{1,4}) cm', line['size']):
                data.loc[index, 'circumference'] = re.search('Circumference: (.{1,4}) cm', line['size']).group(1)

            if re.search('\(Size (.+)\)\w', line['size']):
                data.loc[index, 'size_number'] = re.search('\(Size (.+)\)\w', line['size']).group(1)
            elif re.search('\(Size (.+)\)', line['size']):
                data.loc[index, 'size_number'] = re.search('\(Size (.+)\)', line['size']).group(1)

    # Spliting shell composition
    # Cotton, Spandex, Polyester, Elastomultiester, Rayon, Lyocell
    data['cotton'] = data['shell_composition'].apply(lambda x: int(re.search('Cotton (\d{1,3})', x).group(1))/100
                                                    if re.search('Cotton (\d{1,3})', x) else np.NaN)

    data['spandex'] = data['shell_composition'].apply(lambda x: int(re.search('Spandex (\d{1,3})',x).group(1))/100
                                                    if re.search('Spandex (\d{1,3})', x) else np.NaN)

    data['polyester'] = data['shell_composition'].apply(lambda x:
                                                        int(re.search('Polyester (\d{1,3})',x).group(1))/100
                                                        if re.search('Polyester (\d{1,3})', x) else np.NaN)

    data['elastomultiester'] = data['shell_composition'].apply(lambda x:
                                                            int(re.search('Elastomultiester (\d{1,3})',
                                                                            x).group(1))/100 
                                                            if re.search('Elastomultiester (\d{1,3})', x) else
                                                            np.NaN)

    data['rayon'] = data['shell_composition'].apply(lambda x: int(re.search('Rayon (\d{1,3})', x).group(1))/100
                                                    if re.search('Rayon (\d{1,3})', x) else np.NaN)

    data['lyocell'] = data['shell_composition'].apply(lambda x: int(re.search('Lyocell (\d{1,3})',x).group(1))/100
                                                    if re.search('Lyocell (\d{1,3})', x) else np.NaN)

    data = data.drop(['size', 'composition', 'shell_composition'], axis=1)

    for index, line in data.iterrows():
        # check for null values
        if pd.notnull(line['pocket_lining_composition']):
            if re.search('Cotton (\d{1,3})', line['pocket_lining_composition']):
                data.loc[index, 'cotton_pocket'] = int(re.search('Cotton (\d{1,3})',
                                                            line['pocket_lining_composition']).group(1))/100
            
            if re.search('Polyester (\d{1,3})', line['pocket_lining_composition']):
                data.loc[index, 'polyester_pocket'] = int(re.search('Polyester (\d{1,3})',
                                                            line['pocket_lining_composition']).group(1))/100
        if pd.notnull(line['additional_material']):
            if re.search('cotton (\d{1,3})', line['additional_material']):
                data.loc[index, 'recycled_cotton'] = int(re.search('cotton (\d{1,3})',
                                                            line['additional_material']).group(1))/100
            
            if re.search('polyester (\d{1,3})', line['additional_material']):
                data.loc[index, 'recycled_polyester'] = int(re.search('cotton (\d{1,3})',
                                                            line['additional_material']).group(1))/100
                
    data = data.drop(['pocket_lining_composition', 'additional_material'], axis=1)

    return data


def data_insert(data):
    # Data Insert
    # connecting to database
    engine = create_engine('sqlite:////home/matheus/repos/webscraping-for-clothing-business/database_hm.sqlite', echo=False)

    # inserting data into database
    with engine.connect() as connection:
        data.to_sql('showroom', con=connection, if_exists='append', index=False)

    return None


if __name__ == '__main__':

    #logging
    path = '/home/matheus/repos/webscraping-for-clothing-business/'

    if not os.path.exists(path + 'Logs'):
        os.makedirs(path + 'Logs')
    
    logging.basicConfig(
        filename=path + 'Logs/webscraping_hm.log',
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y%m%d %H%M%S',
    )

    logger = logging.getLogger('webscraping_hm')

    # Parameters and constants
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:106.0) Gecko/20100101 Firefox/106.0'}

    # URL
    url = 'https://www2.hm.com/en_us/men/products/jeans.html'

    # Data collection
    data = data_collection(url, headers)
    logger.info('data collection done')

    # Data collection by product
    data_product = data_collection_by_product(data, headers)
    logger.info('data collectioon by product done')

    # Data cleaning
    data_product_cleaned = data_cleaning(data_product)
    logger.info('data product cleaning done')

    # Data insertion
    data_insert(data_product_cleaned)
    logger.info('data insertion done')