
#!/usr/bin/env python

# Librairie Os pour les chemins de fichiers et variables d'environnement
import os
import requests
import logging
import pandas as pd
import time

# ------------------ CONFIGURATION ------------------------------- #

API_KEY = os.environ.get('Key_Google')

# Backoff time sets how many minutes to wait between google pings when your API limit is hit
BACKOFF_TIME = 1 #30minutes

output_filename = 'C:/Users/maell/DEV_DATA/Musees_App/Scripts/Data/Csv_Bdd/Table_Musees_3.csv'
input_filename = 'C:/Users/maell/DEV_DATA/Musees_App/Scripts/Data/Csv_Bdd/Table_Musees.csv'

# Specify the column name in your input data that contains addresses here
address_column_name = "adrl1_m"

# Return Full Google Results? If True, full JSON results from Google are included in output
RETURN_FULL_RESULTS = False

# ------------------ DATA LOADING -------------------------------- #

# Read the data to a Pandas Dataframe
data = pd.read_csv(input_filename, encoding='utf-8', sep=";")

if address_column_name not in data.columns:
	raise ValueError("Missing Address column in input data")

# Form a list of addresses for geocoding:
# Make a big list of all of the addresses to be processed.
addresses = data[address_column_name].tolist()

addresses = (data[address_column_name] + ',' + data['ville_m'] + ',France').tolist()


#------------------	FUNCTION DEFINITIONS ------------------------

def get_google_results(address, api_key=None, return_full_response=False):
  
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)
    
    if api_key is not None:
        geocode_url = geocode_url + "&key={}".format(api_key)
        
    # Ping google for the reuslts:
    results = requests.get(geocode_url)
    # Results will be in JSON format - convert to dict using requests functionality
    results = results.json()
    
    # if there's no results or an error, return empty results.
    if len(results['results']) == 0:
        output = {
            "formatted_address" : None,
            "latitude": None,
            "longitude": None,
            "accuracy": None,
            "google_place_id": None,
            "type": None,
            "postcode": None
        }
    else:    
        answer = results['results'][0]
        output = {
            "formatted_address" : answer.get('formatted_address'),
            "latitude": answer.get('geometry').get('location').get('lat'),
            "longitude": answer.get('geometry').get('location').get('lng'),
            "accuracy": answer.get('geometry').get('location_type'),
            "google_place_id": answer.get("place_id"),
            "type": ",".join(answer.get('types')),
            "postcode": ",".join([x['long_name'] for x in answer.get('address_components') 
                                  if 'postal_code' in x.get('types')])
        }
        
    # Append some other details:    
    output['input_string'] = address
    output['number_of_results'] = len(results['results'])
    output['status'] = results.get('status')
    if return_full_response is True:
        output['response'] = results
    
    return output

# ------------------ PROCESSING LOOP ----------------------------- #

#create a logger
logger = logging.getLogger('mylogger')
#set logger level
logger.setLevel(logging.WARNING)

# Ensure, before we start, that the API key is ok/valid, and internet access is ok
test_result = get_google_results("London, England", API_KEY, RETURN_FULL_RESULTS)
if (test_result['status'] != 'OK') or (test_result['formatted_address'] != 'London, UK'):
    logger.warning("There was an error when testing the Google Geocoder.")
    raise ConnectionError('Problem with test results from Google Geocode - check your API key and internet connection.')

# Create a list to hold results
results = []
# Go through each address in turn
for address in addresses:
    # While the address geocoding is not finished:
    geocoded = False
    while geocoded is not True:
        # Geocode the address with google
        try:
            geocode_result = get_google_results(address, API_KEY, return_full_response=RETURN_FULL_RESULTS)
        except Exception as e:
            logger.exception(e)
            logger.error("Major error with {}".format(address))
            logger.error("Skipping!")
            geocoded = True
            
        # If we're over the API limit, backoff for a while and try again later.
        if geocode_result['status'] == 'OVER_QUERY_LIMIT':
            logger.info("Hit Query Limit! Backing off for a bit.")
            time.sleep(BACKOFF_TIME * 1) # sleep for 30 minutes
            geocoded = False
        else:
            # If we're ok with API use, save the results
            # Note that the results might be empty / non-ok - log this
            if geocode_result['status'] != 'OK':
                logger.warning("Error geocoding {}: {}".format(address, geocode_result['status']))
            logger.debug("Geocoded: {}: {}".format(address, geocode_result['status']))
            results.append(geocode_result)           
            geocoded = True
            
      # Print 2 geocodes and sleep 5 second   
    if len(results) % 5 == 0:      
          time.sleep(3)        
    # Print status every 100 addresses
    if len(results) % 50 == 0:
    	logger.info("Completed {} of {} address".format(len(results), len(addresses)))
            
    # Every 500 addresses, save progress to file(in case of a failure so you have something!)
    if len(results) % 100 == 0:
        pd.DataFrame(results).to_csv("{}_bak".format(output_filename))

# All done
logger.info("Finished geocoding all addresses")

# Write the full results to csv using the pandas library.
pd.DataFrame(results).to_csv(output_filename, sep=";", encoding='utf-8')