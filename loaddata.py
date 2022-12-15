import numpy as np
import pandas as pd
import pyodbc

import dataloader
import helper

import transformation

print()
print('Requested: listings')
all_listings = transformation.transformedlistings()
print('Received: transformed listings')

print()
print('Load started: listings')
custom = {"id": "INT PRIMARY KEY",
          "listing_id": "VARCHAR(24) UNIQUE",
          "host_since": "DATE",
          "calendar_last_scraped": "DATE",
          "first_review": "DATE",
          "last_review": "DATE"
          }
dataloader.full_load(df=all_listings, tbl="listings",
                     hasindex=False, custom=custom)


print()
print('Requested: calendar')
all_calendar = transformation.transformedcalendar()
print('Received: transformed calendar')

print()
print('Load started: calendar')
custom = {"id": "INT PRIMARY KEY",
          "date": "DATE"
          }
dataloader.full_load(df=all_calendar, tbl="calendar",
                     hasindex=False, custom=custom)


print()
print('Requested: reviews')
all_reviews = transformation.transformedreviews()
print('Received: transformed reviews')

print()
print('Load started: reviews')
custom = {"id": "INT PRIMARY KEY",
          "date": "date"
          }
dataloader.full_load(df=all_reviews, tbl="reviews",
                     hasindex=False, custom=custom)
