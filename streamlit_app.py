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

# Dropdown for table selection
selected_table = st.selectbox(
    "Select a table to query", 
    ["Disease", "Location", "Report", "Weekly_Data"]
)

# Chat input field
if prompt := st.chat_input("Ask a question:"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Determine type of question
    if "bigquery" in prompt.lower():
        # Construct dynamic SQL query based on selected table
        sql_query = f"SELECT * FROM `ba-882-group3.NNDSS_Dataset.{selected_table}` LIMIT 10"
        try:
            query_job = bq_client.query(sql_query)
            results = query_job.result()
            response = f"BigQuery Results from {selected_table}:\n{[dict(row) for row in results]}"
        except Exception as e:
            response = f"Error querying BigQuery: {e}"

    elif "function" in prompt.lower():
        # Example Cloud Function call
        function_url = "https://us-central1-ba-882-group3.cloudfunctions.net/weekly_data"
        payload = {"input": prompt}
        try:
            cf_response = requests.post(function_url, json=payload)
            if cf_response.status_code == 200:
                response = cf_response.json()
            else:
                response = f"Cloud Function Error: {cf_response.status_code}"
        except Exception as e:
            response = f"Error calling Cloud Function: {e}"

    else:
        # OpenAI GPT response
        stream = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": m["role"], "content": m["content"]}
                for m in st.session_state.messages
            ],
            stream=True,
        )
        response = ""
        for chunk in stream:
            response += chunk["choices"][0]["delta"].get("content", "")


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
