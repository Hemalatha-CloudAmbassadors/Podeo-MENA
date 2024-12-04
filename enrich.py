import base64
import vertexai
from vertexai.generative_models import GenerativeModel, SafetySetting
from google.cloud import bigquery
from google.auth import credentials

# Function to authenticate and fetch data from BigQuery
def fetch_trending_keywords(country_code, region_name):
    # Authenticate with Google Cloud
    client = bigquery.Client()

    # Query to fetch trending terms based on the given country and region
    query = f"""
        SELECT
            term,
            rank,
            ROUND(AVG(score), 2) AS score,
            refresh_date
        FROM
            `bigquery-public-data.google_trends.international_top_terms`
        WHERE
            refresh_date = DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
            AND country_code = '{country_code}'
            AND region_name = '{region_name}'
        GROUP BY
            term, rank, refresh_date
        ORDER BY
            rank ASC;
    """
    query_job = client.query(query)  # Make API request to BigQuery

    # Fetch results and extract keywords (terms)
    results = query_job.result()
    keywords = [row.term for row in results]

    return keywords

# Function to send a request to Gemini API for content generation
def generate_content_with_gemini(keywords, country, region):
    vertexai.init(project="internal-sandbox-434410", location="us-central1")
    model = GenerativeModel("gemini-1.5-flash-002")

    # Create the prompt using the fetched keywords
    keywords_text = "\n".join(keywords)
    text1_1 = f"""For the trending keywords given below, which I extracted from Google for the location "{region}, {country}". Keep this as a starting point for my podcasts, enhance the keywords and give me a list of ideas so that I can create a podcast with those contents. Additionally, add a brief description on each idea.

    {keywords_text}

    Give the output as a CSV format with idea and the respective description."""

    generation_config = {
        "max_output_tokens": 8192,
        "temperature": 1,
        "top_p": 0.95,
    }

    safety_settings = [
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold=SafetySetting.HarmBlockThreshold.OFF
        ),
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=SafetySetting.HarmBlockThreshold.OFF
        ),
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold=SafetySetting.HarmBlockThreshold.OFF
        ),
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=SafetySetting.HarmBlockThreshold.OFF
        ),
    ]

    # Start the chat with Gemini and generate the response
    chat = model.start_chat()
    response = chat.send_message(
        [text1_1],
        generation_config=generation_config,
        safety_settings=safety_settings
    )

    # Extract and return only the CSV part from the response
    csv_output = response.candidates[0].content.parts[0].text.strip("```csv\n").strip("```")
    return csv_output

# Main function to fetch trending data and generate content
def main():
    # Input the country and region
    country = input("Enter the Country_name: ")
    region = input("Enter the region_name of the country: ")

    # Fetch trending keywords based on the country and region
    keywords = fetch_trending_keywords(country, region)
    print(f"Fetched trending keywords for {country}, {region}: {keywords}")

    # Send the keywords to Gemini API for content generation
    csv_response = generate_content_with_gemini(keywords, country, region)

    # Print the CSV response
    print("Generated Podcast Ideas (CSV format):")
    print(csv_response)

# Run the main function
if __name__ == "__main__":
    main()