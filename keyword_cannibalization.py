import os
import os.path
import pandas as pd
pd.options.mode.chained_assignment = None
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from ssl import SSLEOFError
from googleapiclient.errors import HttpError
import concurrent.futures
import time
import pandas as pd
import googleapiclient
import google_auth_httplib2
import httplib2
import requests
from bs4 import BeautifulSoup, Comment
import multiprocessing

#______________________________________________________________________________
# Authentication of Search Console API

# Delete existing token file
if os.path.isfile('C:/your_path/token_gsc.json'):
  os.remove('C:/your_path/token_gsc.json')

# Function to create token for Search Console
def gsc_auth(scopes):
    creds = None
    if os.path.exists('token_gsc.json'): #It does not have the same name as the first token, to differentiate between the APIs
        creds = Credentials.from_authorized_user_file('token_gsc.json', scopes)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'C:/your_path/credentials_V2.json', scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token_gsc.json', 'w') as token:
            token.write(creds.to_json())

    service = build('searchconsole', 'v1', credentials=creds)

    return service

# Scope is defined
scopes_gsc = ['https://www.googleapis.com/auth/webmasters']

# Function is called
gsc_auth(scopes_gsc)


###############################################################################
############################# FUNCTION CREATION ###############################
###############################################################################

# Function for scraping of URLs
def fetch_url(url):
    # Counter to print progress
    with lock:
        url_counter.value += 1
        print(f"Fetched HTML from {url_counter.value}/{len(kw_df)} URLs")
    
    return requests.get(url).text


# Function to extract visible text from html
def extract_visible_text(html_page):
    soup = BeautifulSoup(html_page, 'html.parser')
    
    # Irrelevant html tags are excluded
    for script in soup(["script", "noscript", "nav", "style", "input", "meta", "label", "header", "footer", "aside", "table", "button", "head"]):
        script.decompose()
        
    # Removes the "Read also" part of the page    
    for div in soup.find_all('div', class_='container d-print-none mt-7'):
        div.decompose()

    # Various comments are removed
    for element in soup.find_all(text=lambda text: isinstance(text, Comment)):
        element.extract()
    
    return str(soup)

# Function to extract /guides/-links
def extract_links(html_page):
    soup = BeautifulSoup(html_page, 'html.parser')
    links = []

    for link in soup.find_all('a', href=True):
        href = link['href']
        links.append(href)

    return links



###############################################################################
################################ URL IMPORT ###################################
###############################################################################

# URLs are imported
url_df = pd.read_excel(r'C:/your_path/url_file.xlsx')   

# URLs are saved in list
urls = list(url_df['URL'])

# All URLs containing "lang=" are removed
urls = [x for x in urls if "lang=" not in x ]

###############################################################################
############################ URL FILTER CREATION ##############################
###############################################################################

# List of filter dicts created - one for each url
url_filters = []
for url in urls:
    filter_dict = {
        "dimension": "page",
        "operator": "equals",
        "expression": [url]
    }
    url_filters.append(filter_dict)


###############################################################################
############################## KEYWORD IMPORT #################################
###############################################################################

# Credentials object is created
creds = Credentials.from_authorized_user_file('C:/your_path/token_gsc.json', scopes_gsc)

# Function for keyword import is created
def fetch_url_data(i):
    # New connection and authorization created for the API for each thread 
    http = google_auth_httplib2.AuthorizedHttp(creds, http=httplib2.Http())
    local_service = googleapiclient.discovery.build('searchconsole', 'v1', http=http)
    # Request is defined 
    urls_request = {
        'startDate': "2022-10-18",
        'endDate': "2024-02-17",
        'dimensions': ["query", "page"],  
        'rowLimit': 10000,
        'startRow': 0,
        "dimensionFilterGroups": [
        {
          "groupType": "and",
          "filters": url_filters[i]
        }
      ]
    }

    # SSL-error protection
    attempts = 0
    max_attempts = 5
    while attempts < max_attempts:
        try:
            # API call
            urls_response = local_service.searchanalytics().query(siteUrl='https://www.website.dk/', body=urls_request).execute()
            return urls_response
        except HttpError as e: # Exception for quota error
            if e.resp.status == 403 and "quotaExceeded" in str(e):
                print("Quota exceeded. Waiting 1 minute before retrying...")
                time.sleep(60)  # Wait 1 minute
                attempts += 1
            else:
                print(f"An error occurred: {e}. Retrying...")
                attempts += 1
        except SSLEOFError as e: # Exception for SSL error
            print(f"SSL error occurred: {e}. Retrying...")
            attempts += 1
    else:
        print(f"Max attempts reached for index {i}. Continuing to next index.")
        return None

# Start timer
time_start = time.time()

# ThreadPoolExecutor is used to import data simultaneously
keywords_dataframes = []
with concurrent.futures.ThreadPoolExecutor() as executor:
    for i, urls_response in enumerate(executor.map(fetch_url_data, range(len(url_filters)))):
        time.sleep(0.2)
        print(f"Keyword import progress: {i + 1}/{len(url_filters)} URLs")

        if urls_response and 'rows' in urls_response:
            # Response is converted to dataframe
            urls_df = pd.DataFrame(urls_response['rows'])
            urls_df[['keyword', 'url']] = pd.DataFrame(urls_df['keys'].to_list(), columns=['keyword', 'url'])
            urls_df.drop(columns=['keys'], inplace=True)
            keywords_dataframes.append(urls_df)

# End timer
time_end = time.time()
diff_time = time_end - time_start
time_result = diff_time / 60
print('Execution time for keyword import:', round(time_result, 2), 'minutes')


# Keyword dataframes are combined
kw_df = pd.concat(keywords_dataframes)


###############################################################################
########################## INTERNAL LINKS PROCESSING ##########################
###############################################################################

# Counter is created
url_counter = multiprocessing.Value('i', 0)
lock = multiprocessing.Lock()

# html from URLs is imported simultaneously
with concurrent.futures.ThreadPoolExecutor() as executor:
    html_pages = list(executor.map(fetch_url, kw_df['url']))

# Visible text is extracted from html
text_content_list = []
counter = 1 
for i in html_pages:
    text_content = extract_visible_text(i)
    text_content_list.append(text_content)
    print(f"Extracted visible text from {counter}/{len(html_pages)} URLs")
    counter += 1  # Add 1 to counter

    
# Links in the text on the pages are extracted
links_list = []
counter = 1
for page_content in text_content_list:
    links = extract_links(page_content)
    links_list.append(links)
    print(f"Extracted links from {counter}/{len(html_pages)} HTML texts")
    counter += 1  # Add 1 to counter

# Links are extracted from the lists and converted to string and added to the dataframe
kw_df['internal_urls'] = ['; '.join(links) if isinstance(links, list) else '' for links in links_list]
        
###############################################################################
########################## KEYWORD DATA PROCESSING ############################
###############################################################################
    
# Function to concatenate URLs together
def concatenate_all_urls(urls):
    # Filter out empty strings or None values before joining
    filtered_urls = [url for url in urls if url]
    return ','.join(filtered_urls)

# Group by for keywords
kw_df_grouped = kw_df.groupby('keyword').agg( # The keyword checked for cannibalization 
    unique_pages=('url', 'nunique'), # Number of unique URLs where keywords rank on
    total_clicks=('clicks', 'sum'), # Total number of clicks per keyword
    total_impressions=('impressions', 'sum'), # Total number of impressions per keyword
    avg_ctr=('ctr', 'mean'), # Average CTR per keyword
    avg_position=('position', 'mean'), # Average position per keyword
    urls=('url', concatenate_all_urls), # Unique URLs where keywords rank on
    internal_urls=('internal_urls', concatenate_all_urls) # All internal URLs on the ranked URLs
).reset_index() 

# Sort the resulting DataFrame by 'unique_pages' in descending order
cannibalized_kw_df = kw_df_grouped.sort_values(by='unique_pages', ascending=False)

# DataFrame is exported to excel
cannibalized_kw_df.to_excel('C:/your_path/kw_cannibalization.xlsx')

