# UK FOODS STANDARD AGENCY data scraper in Python 2.7
# Author M J Findlay
# Date Nov-2018
import requests
import urllib2
from bs4 import BeautifulSoup

# The search URL is injectable in the following terms:
# Main URL is http://ratings.food.gov.uk/authority-search/en-GB/
# The following fields are appended and separated by forward slash characters.  Unused values are %5E (caret)
# 1. Business Name
# 2. Post Code
# 3. sort option (alpha, relevance, ratings, distance)
# 4. Type of business (1=restaurants, 5=hospitals, 7846=mobile caterers, 7845=Schools, 7838=farmers, etc.)
# 5. Local Authority Area (e.g. 807=Belfast, 760=Aberdeen, 027=Cambridge)
# 6. Rating search limit (Equal5, Equal4, EqualAll, etc.) 
# 7. Display map (0=off, 1=on)
# 8. Page number to display (1...)
# 9. Number of results to display / page (does not appear to have a limit)
# In this case we are interested in restaurants in Belfast City
# The URL search parameters that we provide will perform the search to return the first 1000 restaurants on one page
# Note there are actually less than 1000 restaurants in Belfast

# We will use BeautifulSoup to extract the fields from the html records.



# Define a function to grab the image that we want to associate with the data.
def load_image (source_url):
    r = requests.get(source_url, stream = True )
    if r.status_code == 200 :
        aSplit = source_url.split('/')
        ruta = "./Pictures/"+aSplit[ len (aSplit)- 1 ]
        print (ruta)
        output = open (ruta,"wb")
        for chunk in r:
            output.write(chunk)
        output.close()
            
# Function to identify the user agent

def download(url, user_agent='uocmjf', num_retries=2):
    print 'Downloading:', url
    headers = {'User-agent': user_agent}
    
    request = urllib2.Request(url, headers=headers)
    try:
        html = urllib2.urlopen(request).read()
    except urllib2.URLError as e:
        print 'Download error:', e.reason
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                # retry 5XX HTTP errors
                return download(url, user_agent, num_retries-1)
    return html

# Function to get all the records that the search engine has displayed
def get_records(page):
    import re
    # Parse the page through beautiful soup
    soup = BeautifulSoup(page, 'html.parser')
    
    # Obtain the contents of the Class that contains all the results of the query.
    results_found=soup.find_all(class_="uxResults wshowDistance")

    # Find the id that contains the number of records reported by the search engine
    if soup.find(id="SearchResults_uxSearchResultsReturnCount")==None:
        number_of_hits = 0
    else:
        results_count = (soup.find(id="SearchResults_uxSearchResultsReturnCount").text.strip())
        number_of_hits = int(re.findall('[0-9]+', results_count)[0])
    print(number_of_hits)

    if number_of_hits == 0:
        print "Warning - no records found - please check the data parameters being passed."
    return soup, number_of_hits

# Define function to create the CSV file for the required fields found in the records set
def build_csv(outputfilename,records):
    import csv
    with open ( outputfilename , 'wb' ) as csvfile:
        foodwriter = csv.writer(csvfile, delimiter = ',' ,quotechar = '"' , quoting = csv.QUOTE_ALL)
        # loop over the list of records        
        for row1 in records.find_all("div", class_="ResultRow"):
            # Fetch the fields of interest
            # business name
            bus_name=row1.find("a").contents[0]
            # Post Code
            bus_post_code=row1.find(class_="ResultsBusinessPostcode").contents[0]
            # Foods Standard Agency inspection rating image title (field is an image file)
            rating_img=row1.find(class_="ResultsRatingValue").find("img")
            # Get image title text from rating_img
            rating_desc=rating_img['title']
            # Fetch the date of the inspection
            rating_date=row1.find("span").contents[0]
            # Use regular expressions to remove concatenated blank spaces and newline and carriage return special characters from the fields for name and post code
            bus_name = re.sub('\ {2,}', '', bus_name)
            bus_name = re.sub('\n*\r*', '', bus_name)
            bus_post_code = re.sub('\ {2,}', '', bus_post_code)
            bus_post_code = re.sub('\n*\r*', '', bus_post_code)
            # Convert the rating_desc string into a number.  Handle the case where we don't have a numerical rating available in the data
            # and replace it with the value "NA".
            if rating_desc.upper()[:22] == "FOOD HYGIENE RATING IS":
                rating_no = str(int(re.findall('[0-9]',rating_desc)[0]))
            else:
                rating_no = "NA"
            # We need to turn the strings into a list to make use of writerow()
            rowtoprint = [bus_name,bus_post_code,rating_desc,rating_date,rating_no]
            print rowtoprint
            # Handle UTF-8 characters
            foodwriter.writerow([s.encode('utf-8') for s in rowtoprint])

# The URL of the logo that we want to retrieve.
logo_url="http://ratings.food.gov.uk/Images/fhrs_100x100.jpg"

# Grab the image
load_image(logo_url)

# First get the page with a user-agent set for restaurants in Belfast
# i.e. Leave fields 1 and 2 not set (caret); Set field 3 to Relevance,
# Set field 4 to 1, set field 5 to 807, set field 6 to "EqualAll", set field 7 to 0, set field 8 to 1 and set field 9 to 1000

# The URL of the htnl page with the data that we want to retrieve.
url="http://ratings.food.gov.uk/authority-search/en-GB/%5E/%5E/Relevance/1/807/EqualAll/0/1/1000"

# Download the URL
page_extract=download(url)

# Parse the page through beautiful soup
(resultsSet,number_of_records)=get_records(page_extract)

# If we didn't find any records then abort the process.
if number_of_records == 0:
    print "We did not find any records, aborting the CSV creation process."
else:
    print "Creating CSV file for "+str(number_of_records)+" records."
# Set the output csv file name
import time
timestring = time.strftime("%Y%m%d-%H%M%S")
output_file="FSA_Restaurants_Belfast"+timestring+".csv"
build_csv(output_file,resultsSet)

