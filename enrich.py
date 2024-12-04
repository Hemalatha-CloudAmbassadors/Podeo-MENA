import vertexai
from vertexai.generative_models import GenerativeModel
from google.cloud import bigquery
import re

# Function to fetch trending keywords from BigQuery based on user inputs
def fetch_trending_keywords(country_code, region_name):
    """Fetch trending keywords from BigQuery."""
    # Initialize BigQuery client
    client = bigquery.Client()

    # Construct the query
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

    # Execute the query
    query_job = client.query(query)
    results = query_job.result()

    # Extract terms into a list
    keywords = [{"term": row.term, "rank": row.rank, "score": row.score} for row in results]
    return keywords

# Function to generate content ideas with Gemini API
def generate_content_with_gemini(keywords, country, region):
    """Generate podcast ideas based on trending keywords using Gemini API."""
    # Initialize Vertex AI
    vertexai.init(project="internal-sandbox-434410", location="us-central1")

    # Instantiate the model
    model = GenerativeModel("gemini-1.5-flash-002")

    # Prepare the keywords text for the prompt
    keywords_text = "\n".join([kw["term"] for kw in keywords])
    prompt = f"""
    For the following trending keywords extracted from Google for the location "{region}, {country}":
    {keywords_text}

    Please provide podcast ideas for each keyword. For each idea, add a brief description of the podcast content.
    Provide the output in CSV format: "Idea, Description".
    """

    # Generation configuration
    generation_config = {
        "max_output_tokens": 8192,
        "temperature": 1,
        "top_p": 0.95,
    }

    # Start the chat with Gemini and generate the response
    chat = model.start_chat()
    response = chat.send_message(
        [prompt],
        generation_config=generation_config
    )

    # Debugging: Print the entire response object to understand its structure
    print("Response from Gemini API:", response)

    # Check if the response has candidates and extract the correct content
    if response.candidates:
        content = response.candidates[0].content

        # Ensure the content is a string and clean the CSV format
        if isinstance(content, str):
            # Use regex to remove the code block markers and clean the CSV content
            csv_output = re.sub(r"```csv\n", "", content)  # Remove the opening code block
            csv_output = re.sub(r"\n```", "", csv_output)  # Remove the closing code block
        else:
            raise ValueError("Content is not in expected string format.")

        return csv_output
    else:
        raise ValueError("No response generated from the model.")

# Main function to integrate everything
def main():
    # User inputs for country code and region name
    country_code = input("Enter country code (e.g., IN): ")
    region_name = input("Enter region name (e.g., Tamil Nadu): ")

    # Fetch trending keywords from BigQuery
    keywords = fetch_trending_keywords(country_code, region_name)
    print(f"Fetched Keywords for {region_name}, {country_code}:")
    for kw in keywords:
        print(f"Term: {kw['term']}, Rank: {kw['rank']}, Score: {kw['score']}")

    # Generate content ideas using Gemini API
    csv_data = generate_content_with_gemini(keywords, country_code, region_name)
    print("\nGenerated Podcast Ideas (CSV format):")
    print(csv_data)

if __name__ == "__main__":
    main()