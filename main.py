import openai
from firecrawl import FirecrawlApp
from openai import AzureOpenAI
from dotenv import load_dotenv
import os 
import json 
import pandas as pd 
from datetime import datetime

def scrape_data(url):

    load_dotenv()
    # Initialize the FirecrawlApp with your API key
    app = FirecrawlApp(api_key=os.getenv('FIRECRAWL_API_KEY'))
    
    # Scrape a single URL with options to get only the main content
    scraped_data = app.scrape_url(url, {'pageOptions': {'onlyMainContent': True}})
    
    # Check if 'markdown' key exists in the scraped data
    if isinstance(scraped_data, dict) and 'markdown' in scraped_data:
        return scraped_data['markdown']
    else:
        raise KeyError("The key 'markdown' does not exist in the scraped data or the response is not a dictionary.")

def save_raw_data(raw_data, timestamp, output_folder='output'):
     # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Split the raw data into chunks using "[!" as the separator
    chunks = raw_data.split('[!')
    
    # Save the raw markdown data with timestamp in filename
    raw_output_path = os.path.join(output_folder, f'rawData_{timestamp}.md')
    with open(raw_output_path, 'w', encoding='utf-8') as f:
        for i, chunk in enumerate(chunks[1:7]):  # write only the first 7 chunks
            f.write('[!' + chunk)
        if len(chunks) > 7:  # if there are more than 7 chunks
            lines = chunks[7].split('\n')
            f.write('[!' + '\n'.join(lines[:28]) + '\n')  # write the first 27 lines of the last chunk
    print(f"Raw data saved to {raw_output_path}")

def format_data(data, fields=None):
    load_dotenv()

    openai.api_key = os.getenv('OPENAI_API_KEY')
    openai.api_base = os.getenv('AZURE_OPENAI_ENDPOINT')
    openai.api_version = os.getenv('OPENAI_API_VERSION')

    client = AzureOpenAI(
        api_key=openai.api_key,
        api_version=openai.api_version,
        azure_endpoint=openai.api_base
    )

    # Assign default fields if not provided
    if fields is None:
        fields = ["Address", "Real Estate Agency", "Price", "Beds", "Baths", "Sqft", "Home Type", "Listing Age", "Picture of home URL", "Listing URL"]

    # Define system message content
    system_message = "Extract structured information and convert it into pure JSON format."

    # Split data into chunks based on the delimiter "[!["
    chunks = data.split('[![')
    chunks = ['[![' + chunk for chunk in chunks if chunk.strip()]  # Reassemble chunks with delimiter and filter out empty chunks

    all_formatted_data = []

    for chunk in chunks:
        user_message = f"Extract the following information from the provided text:\nPage content:\n\n{chunk}\n\nInformation to extract: {fields}"

        response = client.chat.completions.create(
            model="gpt-35-turbo",
            response_format="json",
            messages=[
                {
                    "role": "system",
                    "content": system_message
                },
                {
                    "role": "user",
                    "content": user_message
                }
            ]
        )
        # Check if the response contains the expected data
        if response and response.choices:
            formatted_data = response.choices[0].message.content.strip()
            print(f"Formatted data received from API: {formatted_data}")

            try:
                parsed_json = json.loads(formatted_data)
                # Ensure parsed JSON is a dictionary and not empty
                if isinstance(parsed_json, dict) and parsed_json:
                    all_formatted_data.append(parsed_json)
            except json.JSONDecodeError as e:
                print(f"JSON decoding error: {e}")
                print(f"Formatted data that caused the error: {formatted_data}")
                continue  # Skip to the next chunk

    final_result = {'listings': all_formatted_data}
    return final_result

def save_formatted_data(formatted_data, timestamp, output_folder='output'):
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Save the formatted data as JSON with timestamp in filename
    output_path = os.path.join(output_folder, f'sorted_data_{timestamp}.json')

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(formatted_data, f, indent=4)
    print(f"Formatted data saved to {output_path}")

    # Check if data is a dictionary and contains exactly one key
    if isinstance(formatted_data, dict) and len(formatted_data) == 1:
        key = next(iter(formatted_data))  # Get the single key
        formatted_data = formatted_data[key]

     # Convert the formatted data to a pandas DataFrame
    df = pd.DataFrame(formatted_data)

    # Convert the formatted data to a pandas DataFrame
    if isinstance(formatted_data, dict):
        formatted_data = [formatted_data]

    df = pd.DataFrame(formatted_data)

    # Save the DataFrame to an Excel file
    excel_output_path = os.path.join(output_folder, f'sorted_data_{timestamp}.xlsx')
    df.to_excel(excel_output_path, index=False)
    print(f"Formatted data saved to Excel at {excel_output_path}")

if __name__ == "__main__":
    # Scrape a single URL
    #url = 'https://www.zillow.com/salt-lake-city-ut/'
    url = 'https://www.trulia.com/for_sale/San_Francisco,CA/37.43214,38.06195,-122.58398,-122.0841_xy/10_zm/'
    #url = 'https://www.seloger.com/immobilier/achat/immo-lyon-69/'
    
    try:
        # Generate timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Scrape data
        raw_data = scrape_data(url)
        
        # Save raw data
        save_raw_data(raw_data, timestamp)

        # Format data
        formatted_data = format_data(raw_data)
        
        # Save formatted data
        save_formatted_data(formatted_data, timestamp)
        
    except Exception as e:
        print(f"An error occurred: {e}")
