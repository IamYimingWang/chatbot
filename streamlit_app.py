import streamlit as st
import openai
from google.cloud import bigquery
import os
from google.oauth2 import service_account
import pandas as pd
import json


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

# Queries for each table
queries = {
    "Disease": "SELECT * FROM `ba-882-group3.NNDSS_Dataset.Disease` LIMIT 5;",
    "Location": "SELECT * FROM `ba-882-group3.NNDSS_Dataset.Location` LIMIT 5;",
    "Report": "SELECT * FROM `ba-882-group3.NNDSS_Dataset.Report` LIMIT 5;",
    "Weekly_Data": "SELECT * FROM `ba-882-group3.NNDSS_Dataset.Weekly_Data` LIMIT 5;"
}

# Fetch rows and format as JSON
sample_data = {}
for table, query in queries.items():
    results = bq_client.query(query).result()
    rows = [dict(row) for row in results]
    sample_data[table] = rows 

# Save as JSON for the chatbot
with open("sample_data.json", "w") as f:
    json.dump(sample_data, f, indent=4)

print(json.dumps(sample_data, indent=4))
schema_description = {
    "DenormalizedTable": {
        "columns": [
            {"name": "disease_id", "description": "Unique identifier for a disease."},
            {"name": "disease_name", "description": "Name of the disease."},
            {"name": "location_id", "description": "Unique identifier for a location."},
            {"name": "location_name", "description": "Name of the location (e.g., state, city)."},
            {"name": "longitude", "description": "Longitude of the location."},
            {"name": "latitude", "description": "Latitude of the location."},
            {"name": "report_id", "description": "Unique identifier for a report."},
            {"name": "mmwr_year", "description": "Year of the report (based on the MMWR calendar)."},
            {"name": "mmwr_week", "description": "Week number of the report (based on the MMWR calendar)."},
            {"name": "current_week", "description": "Data value for the current week."},
            {"name": "cumulative_ytd", "description": "Cumulative year-to-date data for the current year."},
            {"name": "previous_52_week_max", "description": "Maximum value from the previous 52 weeks."},
            {"name": "previous_52_week_max_flag", "description": "Flag indicating any anomalies in the previous 52-week max."},
            {"name": "cumulative_ytd_flag", "description": "Flag indicating any anomalies in the cumulative YTD data."}
        ],
        "description": "A denormalized table combining diseases, locations, reports, and weekly data."
    }
}



# Function to construct SQL query from prompt
def construct_query_from_prompt(prompt):
    """
    Generate a SQL query dynamically based on the user's input using the denormalized table.
    """
    system_prompt = (
        "You are a SQL assistant. Based on the schema information below:\n"
        f"{schema_description}\n"
        "Generate a valid BigQuery SQL query using only the `ba-882-group3.NNDSS_Dataset.DenormalizedTable` table. "
        "Do not include any explanation or text before or after the query. If the request is unclear or unrelated "
        "to the schema, respond with 'null'."
        "When asked to 'summarize' or provide a general overview of diseases, generate a query that lists "
        "the unique diseases and their counts for the specified location and year if provided. "
        "If no year is provided, summarize for all available years."
        "When generating queries, ensure that any filter on 'location_name' uses the UPPER() function "
        "to match the uppercase format of the data."
    )

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Generate a SQL query for this request: {prompt}"},
            ],
        )
        query = response.choices[0].message["content"].strip()

        if not query.lower().startswith("select") or "null" in query.lower():
            st.warning("Generated query is invalid or unrelated to the schema.")
            return None

        # Display the SQL query in a collapsible expander
        with st.expander("Show Generated SQL Query"):
            st.code(query, language="sql")  # Display query with SQL syntax highlighting
        return query
    except Exception as e:
        st.warning(f"Error generating query: {e}")
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
    """Fetch disease_id to disease_name mapping."""
    try:
        query = "SELECT disease_id, disease_name FROM `ba-882-group3.NNDSS_Dataset.Disease`"
        results = run_bigquery(query)
        return {row["disease_id"]: row["disease_name"] for row in results}
    except Exception as e:
        st.error(f"Error fetching disease mapping: {e}")
        return {}

def get_location_mapping():
    """Fetch location_id to location_name mapping."""
    try:
        query = "SELECT location_id, location_name FROM `ba-882-group3.NNDSS_Dataset.Location`"
        results = run_bigquery(query)
        return {row["location_id"]: row["location_name"] for row in results}
    except Exception as e:
        st.error(f"Error fetching location mapping: {e}")
        return {}

def handle_query(selected_query):
    """Fetch and execute a predefined query based on the user's selection."""
    # Step 1: Fetch query metadata
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

    # Step 2: Execute the query
    query_results = run_bigquery(sql_query)
    if not query_results:
        return f"No results found for query '{selected_query}'."

    # Step 3: Map disease_id and location_id to their names
    disease_mapping = get_disease_mapping()
    location_mapping = get_location_mapping()
    for row in query_results:
        if "disease_id" in row:
            disease_id = row["disease_id"]
            row["disease_id"] = disease_mapping.get(disease_id, f"Unknown Disease ID: {disease_id}")
        if "location_id" in row:
            location_id = row["location_id"]
            row["location_id"] = location_mapping.get(location_id, f"Unknown Location ID: {location_id}")

    # Step 4: Format results for OpenAI analysis
    query_results_top10 = query_results[:10]
    openai_prompt = (
        f"The user selected query: '{selected_query}'.\n\n"
        f"Description: {query_description}.\n\n"
        f"Results:\n{query_results_top10}\n\n"
        "Please provide a brief summary and insights based on these results."
    )
    analysis = ask_openai(openai_prompt)

    # Step 5: Display the query results
    st.subheader("Query Results")
    st.dataframe(pd.DataFrame(query_results_top10))  # Show top 10 rows
    st.caption("Only the top 10 rows are displayed.")

    # Step 7: Construct assistant response
    assistant_response = f"Query executed successfully.\n\n**Analysis:**\n{analysis}"
    st.session_state.messages.append({"role": "assistant", "content": assistant_response})
    with st.chat_message("assistant"):
        st.markdown(assistant_response)

# Fetch list of query names from metadata table
query_list_query = "SELECT QueryDescription FROM `ba-882-group3.NNDSS_Dataset.QueryMetadata` ORDER BY QueryDescription"
query_description = run_bigquery(query_list_query)
query_options = [row['QueryDescription'] for row in query_description]

if query_options:
    selected_query = st.radio("What are you interested in:", query_options)
    if st.button("Run Query"):
        with st.spinner("Running query and generating analysis..."):
            response = handle_query(selected_query)

else:
    st.warning("No queries found in metadata. Please add queries to the QueryMetadata table.")


# Chat input
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
                # Format and display query results as a table
                st.subheader("Query Results")
                st.dataframe(pd.DataFrame(query_results[:10]))  # Show top 10 rows
                st.caption("Only the top 10 rows are displayed.")

                # Prepare results for OpenAI analysis
                analysis = ask_openai(f"Please analyze the following data:\n{query_results}")
                
                # Construct full assistant response
                assistant_response = f"Query executed successfully.\n\n**Analysis:**\n{analysis}"
            else:
                assistant_response = "No results found for your query."
        else:
            assistant_response = "Unable to construct a query based on your input."
    else:
        assistant_response = "Your question doesn't seem related to the database."

    # Display response as chat message
    with st.chat_message("assistant"):
        st.markdown(assistant_response)

    st.session_state.messages.append({"role": "assistant", "content": assistant_response})
