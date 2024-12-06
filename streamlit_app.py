import streamlit as st
import openai
from google.cloud import bigquery
import os
from google.oauth2 import service_account



# Prompt the user for OpenAI API key at the very beginning
openai_api_key = st.text_input("Enter your OpenAI API Key:", type="password")

if not openai_api_key:
    st.warning("Please enter your OpenAI API Key to enable the chatbot.")
    st.stop()  # Stop execution until the API key is entered
else:
    openai.api_key = openai_api_key
    st.success("OpenAI API Key has been set.")

# Initialize BigQuery client
try:
    credentials = service_account.Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]))
    bq_client = bigquery.Client(credentials=credentials)
    st.success("Connected to BigQuery successfully!")
except Exception as e:
    st.error(f"Failed to connect to BigQuery: {e}")
    st.stop()  # Stop execution if BigQuery client can't be initialized

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "keywords" not in st.session_state:
    st.session_state.keywords = set()

# Function to fetch keywords
def fetch_keywords():
    """Fetch distinct keywords from BigQuery for relevance checking."""
    keywords = set()
    try:
        # Define queries
        disease_query = "SELECT DISTINCT disease_name FROM `ba-882-group3.NNDSS_Dataset.Disease`"
        states_query = "SELECT DISTINCT states FROM `ba-882-group3.NNDSS_Dataset.Location`"
        year_query = "SELECT DISTINCT mmwr_year FROM `ba-882-group3.NNDSS_Dataset.Report`"

        # Execute queries
        diseases = [row["disease_name"].lower() for row in bq_client.query(disease_query).result() if row["disease_name"]]
        states = [row["states"].lower() for row in bq_client.query(states_query).result() if row["states"]]
        years = [str(row["mmwr_year"]) for row in bq_client.query(year_query).result() if row["mmwr_year"]]

        # Update keywords
        keywords.update(diseases)
        keywords.update(states)
        keywords.update(years)
        keywords.update(["bigquery", "disease", "location", "report", "weekly_data"])

        st.success("Keywords fetched successfully!")
    except Exception as e:
        st.error(f"Error fetching keywords from BigQuery: {e}")

    return keywords

# Load keywords into session state
if not st.session_state.keywords:
    st.session_state.keywords = fetch_keywords()


# Show title and description after the API key is entered
st.title("Disease Insights Chatbot")
st.write("Ask questions related to US disease data and get insights!")


# Function to check if the query is relevant
def is_relevant_query(prompt):
    """Check if the user query contains relevant keywords."""
    return any(keyword in prompt.lower() for keyword in st.session_state.keywords)

# Function to construct SQL query from prompt
def construct_query_from_prompt(prompt):
    """Generate a SQL query based on the user's input."""
    keywords_in_prompt = [k for k in st.session_state.keywords if k in prompt.lower()]
    if not keywords_in_prompt:
        return None

    keyword = keywords_in_prompt[0]
    if "disease" in prompt.lower():
        return f"SELECT * FROM `ba-882-group3.NNDSS_Dataset.Disease` WHERE LOWER(disease_name) LIKE '%{keyword}%' LIMIT 10"
    elif "state" in prompt.lower():
        return f"SELECT * FROM `ba-882-group3.NNDSS_Dataset.Location` WHERE LOWER(states) LIKE '%{keyword}%' LIMIT 10"
    elif "year" in prompt.lower() or "mmwr_year" in prompt.lower():
        return f"SELECT * FROM `ba-882-group3.NNDSS_Dataset.Report` WHERE mmwr_year = '{keyword}' LIMIT 10"
    return None

# Function to run BigQuery SQL queries
def run_bigquery(sql_query):
    """Executes a SQL query on BigQuery and returns the results."""
    try:
        query_job = bq_client.query(sql_query)
        results = query_job.result()
        return [dict(row) for row in results]
    except Exception as e:
        st.error(f"Error querying BigQuery: {e}")
        return []

# Function to interact with OpenAI API
def ask_openai(prompt):
    """Generate a response using OpenAI API."""
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a professional business analyst and epidemiologist providing insights based on disease data."},
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message['content']
    except Exception as e:
        return f"Error interacting with OpenAI: {e}"

def get_disease_mapping():
    """Fetch a mapping of disease_id to disease_name."""
    mapping_query = "SELECT disease_id, disease_name FROM `ba-882-group3.NNDSS_Dataset.Disease`"
    results = run_bigquery(mapping_query)
    return {row["disease_id"]: row["disease_name"] for row in results}

def get_location_mapping():
    """Fetch a mapping of location_id to location_name."""
    mapping_query = "SELECT location_id, location_name FROM `ba-882-group3.NNDSS_Dataset.Location`"
    results = run_bigquery(mapping_query)
    return {row["location_id"]: row["location_name"] for row in results}

# Function to process user-selected query
def handle_query(selected_query):
    """Fetch and execute a predefined query based on user's selection."""
    metadata_query = f"""
    SELECT QueryDescription, QuerySQL 
    FROM `ba-882-group3.NNDSS_Dataset.QueryMetadata` 
    WHERE QueryDescription = '{selected_query}'
    """
    metadata_results = run_bigquery(metadata_query)
    if not metadata_results:
        return f"Query '{selected_query}' not found in metadata."

    query_description = metadata_results[0]['QueryDescription']
    sql_query = metadata_results[0]['QuerySQL']

    query_results = run_bigquery(sql_query)
    if not query_results:
        return f"No results found for query '{selected_query}'."

    disease_mapping = get_disease_mapping()
    for row in query_results:
        if "disease_id" in row:
            disease_id = row["disease_id"]
            row["disease_id"] = disease_mapping.get(disease_id, f"Unknown Disease ID: {disease_id}")

    location_mapping = get_location_mapping()
    for row in query_results:
        if "location_id" in row:
            location_id = row["location_id"]
            row["location_id"] = location_mapping.get(location_id, f"Unknown Location ID: {location_id}")

    # Format results for display
    result_string = "\n".join([str(row) for row in query_results[:10]])  # Display top 10 rows
    openai_prompt = f"The user selected query: '{selected_query}'. Description: {query_description}. Results:\n{result_string}\nPlease summarize this information."

    # Generate analysis using OpenAI
    analysis = ask_openai(openai_prompt)
    return f"**Query Results:**\n{result_string}\n\n**Analysis:**\n{analysis}"

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])



# Fetch list of query names from metadata table
query_list_query = "SELECT QueryDescription FROM `ba-882-group3.NNDSS_Dataset.QueryMetadata` ORDER BY QueryDescription"
query_description = run_bigquery(query_list_query)
query_options = [row['QueryDescription'] for row in query_description]

if query_options:
    selected_query = st.radio("What are you interested in:", query_options)
    if st.button("Run Query"):
        with st.spinner("Running query and generating analysis..."):
            response = handle_query(selected_query)
        st.success("Query Complete!")
        st.write(response)
else:
    st.warning("No queries found in metadata. Please add queries to the QueryMetadata table.")


# Chat input
if prompt := st.chat_input("Ask a question:"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    if is_relevant_query(prompt):
        sql_query = construct_query_from_prompt(prompt)
        if sql_query:
            query_results = run_bigquery(sql_query)
            if query_results:
                # Format results and get analysis from OpenAI
                result_string = "\n".join([str(row) for row in query_results[:10]])
                assistant_response = f"**Query Results:**\n{result_string}"
                analysis = ask_openai(f"Please analyze the following data:\n{result_string}")
                assistant_response += f"\n\n**Analysis:**\n{analysis}"
            else:
                assistant_response = "No results found for your query."
        else:
            assistant_response = "Unable to construct a query based on your input."
    else:
        assistant_response = "Your question doesn't seem related to the database."
