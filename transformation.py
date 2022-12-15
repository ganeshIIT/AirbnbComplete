import numpy as np
import pandas as pd

from datetime import datetime

import extraction 
import helper


# Cleanse functions
def cleanselistings(df):
    return(df.drop_duplicates(subset = ['id'])
        .rename(columns = {'id':'listing_id'})
        .assign(listing_id = lambda x: x['listing_id'].astype('object'),
                host_id = lambda x: x['host_id'].astype('object'),
                #last_scraped = lambda x: pd.to_datetime(x['last_scraped']).dt.normalize(),
                host_since = lambda x: pd.to_datetime(x['host_since']).dt.normalize(),
                calendar_last_scraped = lambda x: pd.to_datetime(x['calendar_last_scraped']).dt.normalize(),
                first_review = lambda x: pd.to_datetime(x['first_review']).dt.normalize(),
                last_review = lambda x: pd.to_datetime(x['last_review']).dt.normalize(),
                price = lambda x: x['price'].str.replace('$', '').str.replace(',', '').astype('float'),
                has_availability = lambda x: x['has_availability'].apply(lambda x: True if x ==  't' else False)
                    .astype('bool'),
                instant_bookable = lambda x: x['instant_bookable'].apply(lambda x: True if x ==  't' else False)
                    .astype('bool'),
                host_has_profile_pic = lambda x: x['host_has_profile_pic'].apply(lambda x: True if x ==  't' else False)
                    .astype('bool'),
                host_identity_verified = lambda x: x['host_identity_verified'].apply(lambda x: True if x ==  't' else False)
                    .astype('bool'),
                host_is_superhost = lambda x: x['host_is_superhost'].apply(lambda x: True if x ==  't' else False)
                    .astype('bool'),
                amenities = lambda x: x['amenities'].str.replace('[', '').str.replace('"', '').str.replace(']', ''),
                host_verifications = lambda x: x['host_verifications'].str.replace('[', '').str.replace("'", '').str.replace(']', ''),
                
        )
    )

def cleansecalendar(df):
    return(df.drop_duplicates()
        .assign(listing_id = lambda x: x['listing_id'].astype('object'),
                date = lambda x: x['date'].dt.normalize(),
                available = lambda x: x['available'].apply(lambda x: True if x ==  't' else False)
                    .astype('bool'),    
                price = lambda x: x['price'].str.replace('$', '').str.replace(',', '').astype('float'),
        )
    )
    
def cleansereviews(df):
    return(df.drop_duplicates()
        .assign(listing_id = lambda x: x['listing_id'].astype('object'),
                date = lambda x: x['date'].dt.normalize(),
                reviewer_id = lambda x: x['reviewer_id'].astype('object'),
        )
    )



all_listings = None
all_calendar = None
all_reviews = None

def transformedlistings():
    all_listings = extraction.getlistings()
    print('Transforming listings')
    all_listings = cleanselistings(all_listings)
    return all_listings

def transformedcalendar():
    all_calendar = extraction.getcalendar()
    print('Transforming calendar')
    all_calendar = cleansecalendar(all_calendar)
    return all_calendar

def transformedreviews():
    all_reviews = extraction.getreviews()
    print('Transforming reviews')
    all_reviews = cleansereviews(all_reviews)
    return all_reviews





