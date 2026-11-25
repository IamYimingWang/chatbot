# Disease Insights Chatbot

An academic prototype that connects natural-language questions with US public-health surveillance data. Developed as part of the Boston University MS in Business Analytics NNDSS pipeline project.

## What it demonstrates

- A Python and Streamlit interface for exploring disease data.
- BigQuery integration for retrieving disease, location and weekly reporting data.
- OpenAI-assisted SQL generation and summaries of query results.
- A choice of predefined analyses alongside a conversational interface.

## Project context

This application complements the [NNDSS Data Pipeline team project](https://github.com/IamYimingWang/NNDSS_Pipeline_Project). It is an educational prototype, not a clinical tool or a production deployment. The original Streamlit chatbot template provided the starting point; this repository extends it with BigQuery data access and domain-specific analysis.

## Running locally

1. Install dependencies: `pip install -r requirements.txt`.
2. Configure your own Google Cloud service-account credentials in Streamlit secrets under `gcp_service_account`, with access to the required BigQuery dataset. The current source references the original course dataset and may need adaptation for your environment.
3. Start the app: `streamlit run streamlit_app.py`.
4. Supply your own OpenAI API key through the app when prompted.

Never commit API keys or service-account credentials. Cloud and model usage may incur charges.

## Scope and limitations

The code reflects a course-project implementation. Live access to the original cloud resources is not guaranteed, and the application has not been revalidated here against current package or model versions. Generated SQL and AI summaries require independent review; use a read-only dataset and restricted permissions when adapting it.

## Related work

See the linked pipeline repository for the team contributors, dataset source, data preparation, scheduling and Tableau analysis.
