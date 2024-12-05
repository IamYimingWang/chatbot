import streamlit as st
import openai
from google.cloud import bigquery
import requests
import os


openai_api_key = st.secrets["OPENAI_API_KEY"]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = st.secrets["GOOGLE_APPLICATION_CREDENTIALS_PATH"]

try:
    bq_client = bigquery.Client()
    st.success("BigQuery client connected successfully!")
except Exception as e:
    st.error(f"Failed to connect to BigQuery: {e}")

    
# Show title and description.
st.title("Disease Insights Chatbot")
st.write("Select a query from the list below to get insights based on historical disease data!")


# OpenAI API Key
openai.api_key = openai_api_key

    # Google Cloud BigQuery setup
bq_client = bigquery.Client()

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])


def is_relevant_query(prompt):
    """Check if the user query contains relevant keywords."""
    if not prompt:  # Check if prompt is None or empty
        return False

    keywords = st.session_state.keywords
    for keyword in keywords:
        if keyword and keyword in prompt.lower():  # Ensure keyword is not None
            return True
    return False


# Chat input field
if prompt := st.chat_input("Ask a question:"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    if is_relevant_query(prompt):
        # Example: Determine query type from prompt
        if "disease" in prompt.lower():
            sql_query = "SELECT * FROM `ba-882-group3.NNDSS_Dataset.Disease` LIMIT 10"
        elif "state" in prompt.lower():
            sql_query = "SELECT * FROM `ba-882-group3.NNDSS_Dataset.Location` LIMIT 10"
        elif "mmwr_year" in prompt.lower():
            sql_query = "SELECT * FROM `ba-882-group3.NNDSS_Dataset.Report` LIMIT 10"
        

        try:
            query_job = bq_client.query(sql_query)
            results = query_job.result()
            response = f"BigQuery Results:\n{[dict(row) for row in results]}"
        except Exception as e:
            response = f"Error querying BigQuery: {e}"
    else:
        response = "Please ask again, I can only answer questions regarding the dataset."

    # Display response
    with st.chat_message("assistant"):
        st.markdown(response)
    st.session_state.messages.append({"role": "assistant", "content": response})



# Function to run BigQuery SQL queries
def run_bigquery(sql_query):
    """Executes a SQL query on BigQuery and returns the results."""
    query_job = bq_client.query(sql_query)  # Use bq_client instead of client
    results = query_job.result()
    return [dict(row) for row in results]


# Function to interact with OpenAI API
def ask_openai(prompt):
    """Uses OpenAI API to generate a response."""
    response = openai.ChatCompletion.create(
        model="gpt-4", 
        messages=[
            {"role": "system", "content": "You are a professional business analyst and epidemiologist providing insights based on disease data."},
            {"role": "user", "content": prompt}
        ]
    )
    return response.choices[0].message['content']

# Function to process user query
def handle_query(selected_query):
    # Fetch SQL and description from QueryMetadata table
    metadata_query = f"""
    SELECT QueryDescription, QuerySQL 
    FROM `ba-882-group3.NNDSS_Dataset.QueryMetadata` 
    WHERE QueryName = '{selected_query}'
    """
    metadata_results = run_bigquery(metadata_query)
    if not metadata_results:
        return f"Query '{selected_query}' not found in metadata."

    query_description = metadata_results[0]['QueryDescription']
    sql_query = metadata_results[0]['QuerySQL']

    # Run the query
    try:
        query_results = run_bigquery(sql_query)
        # Format results for display
        result_string = "\n".join([str(row) for row in query_results[:10]])  # Display top 10 rows
        openai_prompt = f"The user selected query: '{selected_query}'. Description: {query_description}. Results:\n{result_string}\nPlease summarize this information."
        # Generate analysis using OpenAI
        analysis = ask_openai(openai_prompt)
        return f"**Query Results:**\n{result_string}\n\n**Analysis:**\n{analysis}"
    except Exception as e:
        return f"Error running query '{selected_query}': {e}"

def fetch_keywords():
    """Fetch distinct keywords from BigQuery for relevance checking."""
    keywords = set()

    # Correct column names based on BigQuery schema
    disease_query = "SELECT DISTINCT disease_name FROM `ba-882-group3.NNDSS_Dataset.Disease`"
    states_query = "SELECT DISTINCT states FROM `ba-882-group3.NNDSS_Dataset.Location`"
    year_query = "SELECT DISTINCT mmwr_year FROM `ba-882-group3.NNDSS_Dataset.Report`"  # Updated to mmwr_year

    try:
        # Fetch distinct diseases
        disease_results = bq_client.query(disease_query).result()
        diseases = [row["disease_name"] for row in disease_results if row["disease_name"]]
        keywords.update(d.lower() for d in diseases)

        # Fetch distinct states
        states_results = bq_client.query(states_query).result()
        states = [row["states"] for row in states_results if row["states"]]
        keywords.update(s.lower() for s in states)

        # Fetch distinct years
        year_results = bq_client.query(year_query).result()
        years = [row["mmwr_year"] for row in year_results if row["mmwr_year"]]  # Corrected to mmwr_year
        keywords.update(str(y) for y in years)

        # Debugging output
        st.write("Fetched diseases:", diseases)
        st.write("Fetched states:", states)
        st.write("Fetched years:", years)

    except Exception as e:
        st.error(f"Error fetching keywords from BigQuery: {e}")

    # Add other important keywords manually
    important_keywords = ["bigquery", "disease", "location", "report", "weekly_data"]
    keywords.update(important_keywords)

    return keywords

# Cache the keywords to avoid querying BigQuery repeatedly
if "keywords" not in st.session_state:
    st.session_state.keywords = fetch_keywords()


def construct_query_from_prompt(prompt):
    """Generate a SQL query based on the user's input."""
    if "disease" in prompt.lower():
        keyword = [k for k in st.session_state.keywords if k in prompt.lower()][0]
        return f"SELECT * FROM `ba-882-group3.NNDSS_Dataset.Disease` WHERE disease_name LIKE '%{keyword}%' LIMIT 10"
    elif "state" in prompt.lower():
        keyword = [k for k in st.session_state.keywords if k in prompt.lower()][0]
        return f"SELECT * FROM `ba-882-group3.NNDSS_Dataset.Location` WHERE states LIKE '%{keyword}%' LIMIT 10"
    elif "year" in prompt.lower() or "mmwr_year" in prompt.lower():  # Handles both 'year' and 'mmwr_year'
        keyword = [k for k in st.session_state.keywords if k in prompt.lower()][0]
        return f"SELECT * FROM `ba-882-group3.NNDSS_Dataset.Report` WHERE mmwr_year = '{keyword}' LIMIT 10"
    return None


# Fetch list of query names from metadata table
query_list_query = "SELECT QueryName FROM `ba-882-group3.NNDSS_Dataset.QueryMetadata` ORDER BY QueryName"
query_names = run_bigquery(query_list_query)
query_options = [row['QueryName'] for row in query_names]

if query_options:
    selected_query = st.radio("Choose a query to run:", query_options)
    if st.button("Run Query"):
        with st.spinner("Running query and generating analysis..."):
            response = handle_query(selected_query)
        st.success("Query Complete!")
        st.write(response)
else:
    st.warning("No queries found in metadata. Please add queries to the QueryMetadata table.")

if is_relevant_query(prompt):
    sql_query = construct_query_from_prompt(prompt)
    if sql_query:
        try:
            query_job = bq_client.query(sql_query)
            results = query_job.result()
            response = f"BigQuery Results:\n{[dict(row) for row in results]}"
        except Exception as e:
            response = f"Error querying BigQuery: {e}"
    else:
        response = "Please specify your question more clearly."
else:
    response = "Please ask again, I can only answer questions regarding the dataset."
if "keywords" not in st.session_state:
    try:
        st.session_state.keywords = fetch_keywords()
    except Exception:
        st.session_state.keywords = {"bigquery", "disease", "location", "report", "weekly_data"}
