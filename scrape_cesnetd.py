# Script to scrape CESNET-D's job postings
# Emilio Lehoucq

##################################### Importing libraries #####################################
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from random import uniform
from time import sleep
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
import re
import random
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
from shared_scripts.scraper import get_selenium_response
from shared_scripts.text_extractor import extract_text
from shared_scripts.url_extractor import extract_urls

##################################### Setting parameters #####################################

# URL of the job category postings
URL_CESNETD_JOB_CATEGORY = "https://cesnet.discourse.group/c/job-posting/5"

# URL of the job tags postings
URL_JOBS_TAGS = "https://cesnet.discourse.group/tag/jobs"

# Define CESNET-D's username and password
load_dotenv()
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

# Minimum and maximum time to sleep
SLEEP_MIN_TIME = 2
SLEEP_MAX_TIME = 5

# Timestamp
TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Number of retries
RETRIES = 5

##################################### Configure the logging settings #####################################
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f"Logging configured. Current timestamp: {TS}")

# # TODO: comment for GitHub Actions
# # Add a FileHandler to log to a file in the current working directory
# file_handler = logging.FileHandler('scrape_cesnetd.log')
# file_handler.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)

##################################### Define functions for this script #####################################
def scroll_to_bottom(driver, timeout_min, timeout_max):
    """
    Function to scroll to the bottom of the page.
    Draft taken from: https://chatgpt.com/share/671a8ab4-a1bc-8004-8567-3be2d472dc49
    """

    # Re-try block
    logger.info("Re-try block in scroll_to_bottom function about to start.")

    # Iterate over the number of retries
    for attempt in range(RETRIES):
        logger.info(f"Attempt number: {attempt + 1}.")

        try:
            last_height = driver.execute_script("return document.body.scrollHeight")

            while True:
                # Scroll down to the bottom
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

                # Wait for the page to load more content
                sleep(uniform(timeout_min, timeout_max))

                # Get the new scroll height
                new_height = driver.execute_script("return document.body.scrollHeight")

                # Check if the scroll height has changed
                if new_height == last_height:
                    # If heights are the same, break the loop
                    break

                # Update last_height to new height
                last_height = new_height

            # Break the loop if the re-try block was successful
            logger.info("Re-try block successful. About to break the re-try loop.")
            break

        except Exception as e:
            logger.info(f"Attempt {attempt + 1} failed. Error: {e}.")

            # Check if we have retries left
            if attempt < RETRIES - 1:
                logger.info("Sleeping before retrying.")
                sleep(uniform(SLEEP_MIN_TIME, SLEEP_MAX_TIME))
            else:
                logger.error("No more retries left. Exiting the function.")
                break

def get_main_post_url(url):
    """
    Function to extract the main post URL from a Discourse thread URL.
    Otherwise I would get URLs to responses in the thread, which I don't want.

    Draft taken from ChatGPT.
    """
    try:
        # Split the URL by slashes to check the last segment
        parts = url.rstrip('/').split('/')
        # If the last two segments are numeric, remove the last one
        if len(parts) > 1 and parts[-1].isdigit() and parts[-2].isdigit():
            return '/'.join(parts[:-1])
        # Otherwise, return the URL as is
        return url
    except Exception as e:
        print(f"Error in get_main_post_url: {e}. Returning the URL as is.")
        return url

# Code to test the get_main_post_url function
# def test_get_main_post_url():
#     assert get_main_post_url("https://cesnet.discourse.group/t/now-hiring-fgcu-assistant-professor-school-counseling/2148") == "https://cesnet.discourse.group/t/now-hiring-fgcu-assistant-professor-school-counseling/2148"
#     assert get_main_post_url("https://cesnet.discourse.group/t/now-hiring-fgcu-assistant-professor-school-counseling/2148/1") == "https://cesnet.discourse.group/t/now-hiring-fgcu-assistant-professor-school-counseling/2148"
#     assert get_main_post_url("https://cesnet.discourse.group/t/lecturer-in-higher-education-student-affairs-at-butler-univ-indianapolis/727") == "https://cesnet.discourse.group/t/lecturer-in-higher-education-student-affairs-at-butler-univ-indianapolis/727"
#     assert get_main_post_url("https://cesnet.discourse.group/t/lecturer-in-higher-education-student-affairs-at-butler-univ-indianapolis/727/1") == "https://cesnet.discourse.group/t/lecturer-in-higher-education-student-affairs-at-butler-univ-indianapolis/727"
#     assert get_main_post_url("https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/91") == "https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/91"
#     assert get_main_post_url("https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/91/2") == "https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/91"
#     # The ones below are hypothetical cases
#     assert get_main_post_url("https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/9") == "https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/9"
#     assert get_main_post_url("https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/9/2") == "https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/9"
#     assert get_main_post_url("https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/999999999999999") == "https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/999999999999999"
#     assert get_main_post_url("https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/999999999999999/2") == "https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/999999999999999"
#     print("All tests passed.")

# test_get_main_post_url()

def get_url_id(url):
    """
    Function to get the id of a post from its URL.
    """
    try:
        # Split the URL by slashes to check the last segment
        parts = url.rstrip('/').split('/')
        # Check that the last segment is a number
        if parts[-1].isdigit():
            # Return the last segment
            return parts[-1]
        # If the last segment is not a number, raise an error
        else:
            raise ValueError(f"Last segment of URL is not a number.")
    except Exception as e:
        random_number = random.randint(1000000000000, 10000000000000)
        print(f"Error in get_url_id for URL {url}: {e}. Returning this random number: {random_number}.")
        return random_number

# # Code to test the get_url_id function
# def test_get_url_id():
#     assert get_url_id("https://cesnet.discourse.group/t/now-hiring-fgcu-assistant-professor-school-counseling/2148") == "2148"
#     assert get_url_id("https://cesnet.discourse.group/t/lecturer-in-higher-education-student-affairs-at-butler-univ-indianapolis/727") == "727"
#     assert get_url_id("https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/91") == "91"
#     # The ones below are hypothetical cases
#     assert get_url_id("https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/9") == "9"
#     assert get_url_id("https://cesnet.discourse.group/t/visiting-assistant-professor-cmhc-valparaiso-university/999999999999999") == "999999999999999"
#     get_url_id("https://cesnet.discourse.group")
#     print("All tests passed.")

# test_get_url_id()

def upload_file(element_id, file_suffix, content, folder_id, service, logger):
    """
    Function to upload a file to Google Drive.

    Inputs:
    - element_id: ID of the job post
    - file_suffix: suffix of the file name
    - content: content of the file
    - folder_id: ID of the folder in Google Drive
    - service: service for Google Drive
    - logger: logger

    Outputs: None

    Dependencies: from googleapiclient.http import MediaFileUpload, os
    """
    
    logger.info(f"Inside upload_file: uploading ID {element_id} to Google Drive.")

    try:
        # Prepare the file name
        file_name = f"{element_id}_{file_suffix}.txt"
        logger.info(f"Inside upload_file: prepared the name of the file for the {file_suffix}")

        # Write the content to a temporary file
        with open(file_name, 'w') as temp_file:
            temp_file.write(content)
        logger.info(f"Inside upload_file: wrote the {file_suffix} string to a temporary file")

        # Prepare the file metadata
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        logger.info(f"Inside upload_file: prepared the file metadata for the {file_suffix}")

        # Prepare the file media
        media = MediaFileUpload(file_name, mimetype='text/plain')
        logger.info(f"Inside upload_file: prepared the file media for the {file_suffix}")

        # Upload the file to the Drive folder
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        logger.info(f"Inside upload_file: uploaded the file to the shared folder for the {file_suffix}")

        # Remove the temporary file after uploading
        os.remove(file_name)
        logger.info(f"Inside upload_file: removed the temporary file after uploading for the {file_suffix}")
    
    except Exception as e:
        logger.info(f"Inside upload_file: something went wrong. Error: {e}")

    return None

logger.info("Functions defined.")

##################################### SETTING UP GOOGLE APIS AND GET THE POSTINGS THAT I ALREADY SCRAPED #####################################

# LOCAL MACHINE -- Set the environment variable for the service account credentials 
#TODO: comment for GH Actions (and add to the secrets)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

# Iterate over the number of retries
logger.info("Re-try block for Google Sheets about to start.")
for attempt in range(RETRIES):
    logger.info(f"Attempt {attempt + 1}.")

    try:
        # Authenticate using the service account
        # LOCAL MACHINE
        #TODO: comment for GH Actions
        # credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
        # GITHUB ACTIONS
        # TODO: uncomment for GH Actions
        credentials = service_account.Credentials.from_service_account_info(json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')))
        logger.info("Authenticated with Google Sheets")

        # Create service
        service = build("sheets", "v4", credentials=credentials)
        logger.info("Created service for Google Sheets")

        # Get the values from the Google Sheet with the postings
        # https://docs.google.com/spreadsheets/d/1a3AH-zvYYca58CWWlszVEBi-AeyDDWKV_WhW90o9GK0/edit?gid=0#gid=0
        spreadsheet_postings_id = "1a3AH-zvYYca58CWWlszVEBi-AeyDDWKV_WhW90o9GK0"
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_postings_id, range='A:A').execute()
        existing_postings = result.get("values", []) # Example output: [['test1'], ['abc'], ['123']]
        logger.info("Got data from Google Sheets with the postings.")

        # Get number of existing postings
        n_postings = len(existing_postings)
        logger.info(f"Number of existing postings obtained: {n_postings}.")

        # Convert the list of lists to a set
        existing_postings = set([posting for posting_list in existing_postings for posting in posting_list])
        logger.info("Converted the list of lists to a set.")

        # Get the values from the Google Sheets with the URLs in the postings
        # https://docs.google.com/spreadsheets/d/13Z3XZEDo2BsFb9kRdvbV-Qio7iB_acsbOf-6iDC7OzA/edit?gid=0#gid=0
        spreadsheet_urls_in_postings_id = "13Z3XZEDo2BsFb9kRdvbV-Qio7iB_acsbOf-6iDC7OzA"
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_urls_in_postings_id, range='A:A').execute()
        existing_urls_in_postings = result.get("values", []) # Example output: [['test1'], ['abc'], ['123']]
        logger.info("Got data from Google Sheets with the URLs in the postings.")

        # Get number of existing URLs in postings
        n_urls_in_postings = len(existing_urls_in_postings)
        logger.info(f"Number of existing URLs in postings obtained: {n_urls_in_postings}.")

        # Break the re-try loop if successful
        logger.info("Re-try block successful. About to break the re-try loop.")
        break

    except Exception as e:
        logger.info(f"Attempt {attempt + 1} failed. Error: {e}")

        # Check if we have retries left
        if attempt < RETRIES - 1: 
            logger.info("Sleeping before retry.")
            sleep(uniform(SLEEP_MIN_TIME, SLEEP_MAX_TIME)) 
        else:
            logger.info("All retries exhausted.")
            # Re-raise the last exception if all retries are exhausted
            raise

##################################### Initialize the driver #####################################
# Re-try block
logger.info("Re-try block in initializing the driver about to start.")

# Iterate over the number of retries
for attempt in range(RETRIES):
    logger.info(f"Attempt number: {attempt + 1}.")

    try:
        # Initialize the driver
        # TODO: uncomment for GH Actions
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)
        # # TODO: comment for GH Actions
        # driver = webdriver.Chrome()
        logger.info("Driver initialized.")

        # Break the loop if the re-try block was successful
        logger.info("Re-try block successful. About to break the re-try loop.")
        break

    except Exception as e:
        logger.error(f"Error in initializing the driver: {e}.")

        # Check if we have retries left
        if attempt < RETRIES - 1:
            logger.info("Sleeping before retrying.")
            sleep(uniform(SLEEP_MIN_TIME, SLEEP_MAX_TIME))
        else:
            logger.error("No more retries left. Exiting the script.")
            # Raise the last exception if all retries failed
            raise

##################################### Scrape all the job category postings #####################################
# The Discourse Group has both a job category and a jobs tag. Although there's an overlap,
# some posts are in the category, but not in the tag, and vice versa
# I was planning to scrape the job category and the jobs tag postings separately, but it doesn't make sense
# So, in general, all references to job category postings should also be understood to include jobs tags postings

logger.info("Starting to scrape all the job category postings.")

# Re-try block
logger.info("Re-try block in scraping all the job category postings about to start.")

# Iterate over the number of retries
for attempt in range(RETRIES):
    logger.info(f"Attempt number: {attempt + 1}.")

    try:
        ################# Job category #################

        # Go to the job category postings page
        driver.get(URL_CESNETD_JOB_CATEGORY)
        logger.info(f"Driver went to URL: {URL_CESNETD_JOB_CATEGORY}.")

        # Sleep some time
        sleep(uniform(SLEEP_MIN_TIME, SLEEP_MAX_TIME))
        logger.info(f"Driver slept for a bit.")

        # Find the login button
        login_button = driver.find_element(By.CLASS_NAME, 'body-page-button-container')
        logger.info("Driver found the login button.")

        # Click the login button
        login_button.click()
        logger.info("Driver clicked the login button.")

        # Sleep some time
        sleep(uniform(SLEEP_MIN_TIME, SLEEP_MAX_TIME))
        logger.info("Driver slept for a bit.")

        # Find the username and password fields
        username_field = driver.find_element(By.ID, "login-account-name") 
        password_field = driver.find_element(By.ID, "login-account-password") 
        logger.info("Driver found the username and password fields.")

        # Enter the username and password
        username_field.send_keys(USERNAME)  
        password_field.send_keys(PASSWORD)  
        logger.info("Driver entered the username and password.")

        # Find the login button
        login_button = driver.find_element(By.ID, "login-button")
        logger.info("Driver found the login button.")

        # Click the login button
        login_button.click()
        logger.info("Driver clicked the login button.")

        # Sleep some time
        sleep(uniform(SLEEP_MIN_TIME, SLEEP_MAX_TIME))
        logger.info("Driver slept for a bit.")

        # Scroll to the bottom of the page
        scroll_to_bottom(driver, SLEEP_MIN_TIME, SLEEP_MAX_TIME)
        logger.info("Driver scrolled to the bottom of the page.")

        # Get the URLs of the job postings
        urls = [get_main_post_url(url) for url in [hyperlink.get_attribute('href') for hyperlink in driver.find_elements(By.TAG_NAME, 'a')] if url is not None and 'https://cesnet.discourse.group/t/' in url and 'about-the-job-posting-category' not in url]
        logger.info("Driver got the URLs of the job postings for the job category.")
        logger.info(f"Number of URLs found: {len(urls)}.")
        urls = list(set(urls))
        logger.info(f"Number of URLs after removing duplicates: {len(urls)}.")

        ################# jobs tag postings #################

        # Go to the jobs tag postings page
        driver.get(URL_JOBS_TAGS)

        # Sleep some time
        sleep(uniform(SLEEP_MIN_TIME, SLEEP_MAX_TIME))

        # Scroll to the bottom of the page
        scroll_to_bottom(driver, SLEEP_MIN_TIME, SLEEP_MAX_TIME) 
        logger.info("Driver scrolled to the bottom of the page.")

        # Get the URLs of the job postings
        urls_jobs_tags = [get_main_post_url(url) for url in [hyperlink.get_attribute('href') for hyperlink in driver.find_elements(By.TAG_NAME, 'a')] if url is not None and 'https://cesnet.discourse.group/t/' in url]
        logger.info("Driver got the URLs of the job postings for the jobs tags.")
        logger.info(f"Number of URLs found: {len(urls_jobs_tags)}.")
        urls_jobs_tags = list(set(urls_jobs_tags))
        logger.info(f"Number of URLs after removing duplicates: {len(urls_jobs_tags)}.")

        # Putting the two lists together
        # Although it's inefficient what I'm doing with duplicates,
        # it makes it easier to check that the script is working correctly
        # And there's not a major efficiency problem with the number of URLs
        urls += urls_jobs_tags
        logger.info("Merged the URLs of the job postings for the job category and the jobs tags.")
        logger.info(f"Total number of URLs: {len(urls)}.")
        urls = list(set(urls))
        logger.info(f"Total number of URLs after removing duplicates: {len(urls)}.")

        # Break the loop if the re-try block was successful
        logger.info("Re-try block successful. About to break the re-try loop.")
        break

    except Exception as e:
        logger.error(f"Error in scraping all the job category postings: {e}.")

        # Check if we have retries left
        if attempt < RETRIES - 1:
            logger.info("Sleeping before retrying.")
            sleep(uniform(SLEEP_MIN_TIME, SLEEP_MAX_TIME))
        else:
            logger.error("No more retries left. Exiting the script.")
            # Raise the last exception if all retries failed
            raise

##################################### Scrape individual job category postings #####################################

# Create a list to store the data of all the postings for the job category
data_all_postings_job_category = []
logger.info("List created to store the data of all the postings for the job category.")

# Loop over the URLs for the job postings for the job category
logger.info("Starting to loop over the URLs for the job postings for the job category.")
for url in urls[:10]: # TODO: remove slicing

    # Create a list to store the data of the posting
    data_given_posting = []
    logger.info("List created to store the data of the posting.")

    # Get the URL unique ID of the posting and append it to the list of data for the posting
    url_id = get_url_id(url)
    data_given_posting.append(url_id)
    logger.info(f"URL unique ID of the posting appended to the list of data for the posting: {url_id}.")

    # TODO: think: can the same job be posted several times with different URL ids?
    # Check whether I already have that posting
    if url_id in existing_postings:
        logger.info(f"Posting {url_id} already scraped. URL: {url} Skipping.")
        # Skip to the next posting
        continue

    # Append the URL of the posting to the list of data for the posting
    data_given_posting.append(url)
    logger.info(f"URL of the posting appended to the list of data for the posting: {url}.")

    # Get current timestamp and append it to the list of data for the posting
    data_given_posting.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("Appended current timestamp to the list of data for the posting.")

    # Re-try block
    logger.info("Re-try block in scraping individual job category postings about to start.")

    # Iterate over the number of retries
    for attempt in range(RETRIES):
        logger.info(f"Attempt number: {attempt + 1}.")

        try:
            # Go to the URL of the posting
            driver.get(url)
            logger.info(f"Driver went to URL: {url}.")

            # Sleep some time
            sleep(uniform(SLEEP_MIN_TIME, SLEEP_MAX_TIME))
            logger.info("Driver slept for a bit.")

            # Get the post
            post = driver.find_element(By.CLASS_NAME, 'cooked')
            logger.info("Driver got the posting.")

            # Get the URLs in the post
            # Using extract_urls() too to focus only on the URLs that I care about
            urls_in_post = [url for link in post.find_elements(By.TAG_NAME, 'a') if link.get_attribute('href') is not None and "http" in link.get_attribute('href') for url in extract_urls(link.get_attribute('href')) if url is not None]
            logger.info("Driver got the URLs in the posting.")
            logger.info(f"len urls_in_post (just hyperlinks in the post): {len(urls_in_post)}")
            logger.info(f"urls_in_post (just hyperlinks in the post): {urls_in_post}") # TODO: remove

            # Get the text of the posting
            text_post = post.text
            logger.info("Driver got the text of the posting.")

            # Extract the URLs from the text of the posting and add them to urls_in_post
            urls_in_post += extract_urls(text_post)
            logger.info("URLs extracted from the text of the posting.")
            logger.info(f"len urls_in_post (adding URLs from the text): {len(urls_in_post)}")
            logger.info(f"urls_in_post (adding URLs from the text): {urls_in_post}") # TODO: remove

            # Remove duplicates from urls_in_post
            urls_in_post = list(set(urls_in_post))
            logger.info("Removed duplicates from urls_in_post.")
            logger.info(f"len urls_in_post after removing duplicates: {len(urls_in_post)}")

            # Append the URLs in the posting to the list of data for the posting
            data_given_posting.append(urls_in_post)
            logger.info("URLs in the posting appended to the list of data for the posting.")

            # Append the text of the posting to the list of data for the posting
            data_given_posting.append(text_post)
            logger.info("Text of the posting appended to the list of data for the posting.")

            # Break the loop if the re-try block was successful
            logger.info("Re-try block successful. About to break the re-try loop.")
            break

        except Exception as e:
            logger.error(f"Error in scraping individual job category postings: {e}.")

            # Check if we have retries left
            if attempt < RETRIES - 1:
                logger.info("Sleeping before retrying.")
                sleep(uniform(SLEEP_MIN_TIME, SLEEP_MAX_TIME))
            else:
                logger.info("No more retries left. Couldn't scrape {url}. Error: {e}.")

                # Append FAILURE to the data of the posting
                data_given_posting.append("FAILURE")
                data_given_posting.append("FAILURE")
                logger.info("FAILURE appended to the data of the posting.")

    # Append the data of the posting to the list of data for all the postings for the job category
    data_all_postings_job_category.append(data_given_posting)
    logger.info("Data of the posting appended to the list of data for all the postings for the job category.")

# Quit the driver
driver.quit()
logger.info("Driver quit.")

##################################### Scrape URLs in postings #####################################

# Create variable to store the data of all the URLs found in the postings
data_all_urls_in_postings = []
logger.info("Variable created to store the data of all the URLs found in the postings.")

# Iterate over data_all_postings_job_category
for data_posting in data_all_postings_job_category:
    logger.info("Starting to iterate over data_all_postings_job_category to scrape URLs in postings.")

    # Get the URLs in the posting
    urls_in_posting = data_posting[3]

    # If there are URLs in the posting
    if len(urls_in_posting) > 0:

        # Iterate over the URLs in the posting
        for url in urls_in_posting:
            logger.info(f"About to try to scrape this URL in the posting: {url}.")

            # Create a list to store the data of the URL
            data_given_url = []

            # Store the ID for the URL
            data_given_url.append(n_urls_in_postings + len(data_all_urls_in_postings) + 1)

            # Store the unique URL id
            data_given_url.append(data_posting[0])

            # Store the URL of the posting 
            data_given_url.append(data_posting[1])
            
            # Store the URL
            data_given_url.append(url)

            # Store the current timestamp
            data_given_url.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            logger.info("Data for the URL initialized.")

            # Re-try block
            logger.info("Re-try block in scraping URLs in postings about to start.")

            # Iterate over the number of retries
            for attempt in range(RETRIES):
                logger.info(f"Attempt number: {attempt + 1}.")

                try:
                    # Scrape the URL 
                    source_code_url = get_selenium_response(url)
                    logger.info("Scraped the URL.")

                    # Store the source code for the URL
                    data_given_url.append(source_code_url)
                    logger.info("Source code for the URL stored.")

                    # Extract the text from the source code
                    text_url = extract_text(source_code_url)
                    logger.info("Extracted the text from the source code.")

                    # Store the text for the URL
                    data_given_url.append(text_url)

                    # Break the loop if the re-try block was successful
                    logger.info("Re-try block successful. About to break the re-try loop.")
                    break
            
                except Exception as e:
                    logger.error(f"Error in scraping URL {url} in posting: {e}.")

                    # Check if we have retries left
                    if attempt < RETRIES - 1:
                        logger.info("Sleeping before retrying.")
                        sleep(uniform(SLEEP_MIN_TIME, SLEEP_MAX_TIME))
                    else:
                        logger.info(f"No more retries left. Couldn't scrape {url}. Error: {e}.")

                        # Append FAILURE to the data of the URL
                        data_given_url.append("FAILURE")
                        data_given_url.append("FAILURE")
                        logger.info("FAILURE appended to the data of the URL.")

            # Append the data for the URL in the list of data for all the URLs found in the postings
            data_all_urls_in_postings.append(data_given_url)

####################################### WRITE NEW DATA TO GOOGLE SHEETS #######################################

# Data for the postings

# Retry block in case of failure
logger.info("Re-try block for data for postings (Google Sheets) about to start.")

# Iterate over the number of retries
for attempt in range(RETRIES):

    try:
        logger.info(f"Re-try block for data for postings (Google Sheets). Attempt {attempt + 1}.")

        # Range to write the data
        range_sheet="A"+str(n_postings+1)+":C10000000"
        logger.info("Prepared range to write the data for the postings.")

        # Body of the request
        # id, url, ts
        body={"values": [element[0:3] for element in data_all_postings_job_category]} 
        logger.info("Prepared body of the request for the postings.")

        # Execute the request
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_postings_id,
            range=range_sheet,
            valueInputOption="USER_ENTERED",
            body=body
            ).execute()
        logger.info("Wrote new data to Google Sheets for the postings.")

        # Break the loop if successful
        logger.info("Re-try block for data for postings successful. About to break the loop.")
        break
    
    except Exception as e:
        logger.info(f"Re-try block for data for postings. Attempt {attempt + 1} failed. Error: {e}")

        if attempt < RETRIES - 1:
            logger.info("Re-try block for data for postings. Sleeping before retry.")
            sleep(5)
        else:
            logger.info("Re-try block for data for postings. All retries exhausted.")
            raise

logger.info("Wrote new data to Google Sheets for the postings.")

# Data for the URLs in the postings

# Retry block in case of failure
logger.info("Re-try block for data for URLs in postings (Google Sheets) about to start.")

# Iterate over the number of retries
for attempt in range(RETRIES):

    try:
        logger.info(f"Re-try block for data for URLs postings (Google Sheets). Attempt {attempt + 1}.")

        # Range to write the data
        range_sheet="A"+str(n_urls_in_postings+1)+":E10000000" 
        logger.info("Prepared range to write the data for the URLs in postings.")

        # Body of the request
        # id, id, url, url, ts
        body={"values": [element[:5] for element in data_all_urls_in_postings]} 
        logger.info("Prepared body of the request for the URLs in postings.")

        # Execute the request
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_urls_in_postings_id,
            range=range_sheet,
            valueInputOption="USER_ENTERED",
            body=body
            ).execute()
        logger.info("Wrote new data to Google Sheets for the URLs in postings.")

        # Break the loop if successful
        logger.info("Re-try block for data for URLs in postings successful. About to break the loop.")
        break
    
    except Exception as e:
        logger.info(f"Re-try block for data for URLs in postings. Attempt {attempt + 1} failed. Error: {e}")

        if attempt < RETRIES - 1:
            logger.info("Re-try block for data for URLs in postings. Sleeping before retry.")
            sleep(5)
        else:
            logger.info("Re-try block for data for URLs in postings. All retries exhausted.")
            raise

logger.info("Wrote new data to Google Sheets for the URLs in postings.") 

####################################### WRITE NEW DATA TO GOOGLE DRIVE #######################################

# Note: if there's already a file with the same name in the folder, this code will add another with the same name

# Data for the postings

# Folder ID
# https://drive.google.com/drive/u/4/folders/1zW3WhBG-bX4gYWfRR8d_vOoOmMGiC8mE
folder_id = "1zW3WhBG-bX4gYWfRR8d_vOoOmMGiC8mE" 

# Retry block in case of failure
for attempt in range(RETRIES):

    try:
        logger.info(f"Re-try block for data for the postings (Google Drive). Attempt {attempt + 1}.")

        # Authenticate using the service account (for Google Drive, not Sheets)
        service = build('drive', 'v3', credentials=credentials)
        logger.info("Created service for Google Drive.")
                    
        # Iterate over each of the job posts (list)
        for element in data_all_postings_job_category:
            logger.info("Iterating over each of the postings.")
            # Get the text of the job post
            text = element[-1]
            logger.info("Got the text of the post.")
            # Upload the text to Google Drive
            upload_file(element[0], "text", text, folder_id, service, logger)
        logger.info("Wrote new data for the postings (if available) to Google Drive.")

        # Break the loop if successful
        logger.info("Re-try block for data for the postings successful. About to break the loop.")
        break

    except Exception as e:
        logger.info(f"Re-try block for data for the postings. Attempt {attempt + 1} failed. Error: {e}.")

        if attempt < RETRIES - 1:
            logger.info("Re-try block for data for the postings. Sleeping before retry.")
            sleep(5)
        else:
            logger.info("Re-try block for data for the postings. All retries exhausted.")
            raise

logger.info("Wrote new data for the postings (if available) to Google Drive.")

# Data for the URLs in postings

# Folder ID
# https://drive.google.com/drive/u/4/folders/1pyhL4yqRnvGX3MkpsZAuQnlhqddvYXb7
folder_id = "1pyhL4yqRnvGX3MkpsZAuQnlhqddvYXb7" 

# Retry block in case of failure
for attempt in range(RETRIES):

    try:
        logger.info(f"Re-try block for data for the URLs in postings (Google Drive). Attempt {attempt + 1}.")

        # Authenticate using the service account (for Google Drive, not Sheets)
        service = build('drive', 'v3', credentials=credentials)
        logger.info("Created service for Google Drive.")
                    
        # Iterate over each of the job posts (list)
        for element in data_all_urls_in_postings:
            logger.info("Iterating over each of the URLs in postings.")
            # Get the source code of the job post
            source_code = element[-2]
            logger.info("Got the source code of the post.")
            # Get the text of the job post
            text = element[-1]
            logger.info("Got the text of the post.")
            # Upload the source code to Google Drive
            upload_file(element[0], "source_code", source_code, folder_id, service, logger)
            # Upload the text to Google Drive
            upload_file(element[0], "text", text, folder_id, service, logger)
        logger.info("Wrote new data for the URLs in postings (if available) to Google Drive.")

        # Break the loop if successful
        logger.info("Re-try block for data for the URLs in postings successful. About to break the loop.")
        break

    except Exception as e:
        logger.info(f"Re-try block for data for the URLs in postings. Attempt {attempt + 1} failed. Error: {e}.")

        if attempt < RETRIES - 1:
            logger.info("Re-try block for data for the URLs in postings. Sleeping before retry.")
            sleep(5)
        else:
            logger.info("Re-try block for data for the URLs in postings. All retries exhausted.")
            raise

logger.info("Wrote new data for the URLs in postings (if available) to Google Drive.")

logger.info("Script finished.")
