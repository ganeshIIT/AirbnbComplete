import numpy as np
import pandas as pd

from datetime import datetime

from bs4 import BeautifulSoup
from urllib.request import Request, urlopen

req = Request("http://insideairbnb.com/get-the-data/")
html_page = urlopen(req)

soup = BeautifulSoup(html_page, "lxml")

links = []
for link in soup.findAll('a'):
    links.append(link.get('href'))
    
    
def getdetailsfromurl(urltext):
    components = urltext.split('/')
    country = components[3]
    if len(components) == 9:
        province = components[4]
        city = components[5]
    else:
        province = components[3]
        city = components[3]
    return {'country': country, 'province':province, 'city': city}


def getdatalinks(datasets = ['listings', 'calendar', 'reviews'], 
                 country = 'united-states', 
                 province = None, 
                 city = None):
    
    url_dict = {name:list() for name in datasets}
    for link in links:
        if link:
            if (not province) and (not city):
                for dataset in datasets:
                    if f'/{dataset}.csv.gz' in link and f'/{country}/' in link:
                        url_dict[dataset].append(link)
          
            if (province) and (not city):
                for dataset in datasets:
                    if f'/{dataset}.csv.gz' in link and f'/{country}/{province}/' in link:
                        url_dict[dataset].append(link)

            if province and city:
                for dataset in datasets:
                    if f'/{dataset}.csv.gz' in link and f'/{country}/{province}/{city}/' in link:
                        url_dict[dataset].append(link)
    return(url_dict)



listing_columns = ['id','country', 'province', 'city', 'listing_url', 
                #    'scrape_id', 
                #    'last_scraped', 
                   'source', 'name',
    #    'neighborhood_overview', 'picture_url', 
       'host_id',
    #    'host_url', 
       'host_name', 'host_since', 'host_location', 
       #'host_about',
       'host_response_time', 'host_response_rate', 'host_acceptance_rate',
       'host_is_superhost', 
    #    'host_thumbnail_url', 'host_picture_url',
    #    'host_neighbourhood', 
       'host_listings_count',
       'host_total_listings_count', 'host_verifications',
       'host_has_profile_pic', 'host_identity_verified', 
    #    'neighbourhood',
    #    'neighbourhood_cleansed', 'neighbourhood_group_cleansed', 
       'latitude',
       'longitude', 'property_type', 'room_type', 'accommodates', 
    #    'bathrooms',
       'bathrooms_text', 'bedrooms', 'beds', 'amenities', 'price',
       'minimum_nights', 
    #    'calendar_updated', 
       'has_availability',
       'availability_30', 'availability_60', 'availability_90',
       'availability_365', 'calendar_last_scraped', 'number_of_reviews',
       'number_of_reviews_ltm', 'number_of_reviews_l30d', 'first_review',
       'last_review', 'review_scores_rating', 'review_scores_accuracy',
       'review_scores_cleanliness', 'review_scores_checkin',
       'review_scores_communication', 'review_scores_location',
       'review_scores_value', 'license', 'instant_bookable',
       'calculated_host_listings_count',
       'calculated_host_listings_count_entire_homes',
       'calculated_host_listings_count_private_rooms',
       'calculated_host_listings_count_shared_rooms', 'reviews_per_month']

calendar_columns = ['listing_id', 
                    'date', 
                    'available', 
                    'price', 
                     #'adjusted_price',
                     # 'minimum_nights', 'maximum_nights'
                     ]

reviews_columns = ['listing_id', 
                   'date', 
                   'reviewer_id', 
                   'reviewer_name', 
                   'comments']





country = 'united-states'
province = 'nc'
city = 'asheville'
datasets = ['listings', 'calendar', 'reviews']

# getdatalinks(datasets = datasets, country = country, province=province, city= city)


urls = getdatalinks(datasets = datasets, 
                    country = country, 
                    # province=province, 
                    # city= city
                    )



def getlistings():
    all_listings = pd.DataFrame()
    print('Extracting listings')
    for url in urls['listings']:
        # match = re.search('\d{4}-\d{2}-\d{2}', url)
        # print(datetime.strptime(match.group(), '%Y-%m-%d').date())
        #print(getdetailsfromurl(url))
        listings = (pd.read_csv(url)
                .assign(country = getdetailsfromurl(url)['country'],
                        province = getdetailsfromurl(url)['province'],
                        city = getdetailsfromurl(url)['city'])
        )
        all_listings = pd.concat([all_listings, listings])
    all_listings = all_listings[listing_columns]
    return all_listings

def getcalendar():
    all_calendar = pd.DataFrame()
    print('Extracting calendar')
    for url in urls['calendar']:
        calendar = pd.read_csv(url, parse_dates=['date'])
        calendar = calendar.loc[:,calendar_columns].loc[calendar['date'].isin(pd.date_range(start =datetime.now().date(), periods=10, freq='D'))]
        all_calendar = pd.concat([all_calendar, calendar])
    return all_calendar

def getreviews():
    all_reviews = pd.DataFrame()
    print('Extracting reviews')
    for url in urls['reviews']:
        reviews =pd.read_csv(url, parse_dates=['date'])
        reviews = reviews.sort_values(by = 'date', ascending=False).groupby('listing_id').head(10)
        all_reviews = pd.concat([all_reviews, reviews])
    all_reviews = all_reviews[reviews_columns]
    return all_reviews