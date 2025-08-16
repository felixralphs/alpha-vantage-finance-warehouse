# alpha-vantage-finance-warehouse
End-to-end finance data pipeline using Alpha Vantage → GCS → BigQuery → Tableau.
# Alpha Vantage Finance Data Warehouse

An end-to-end data engineering project to fetch financial data from the [Alpha Vantage API](https://www.alphavantage.co/), store it in a local "raw zone" folder structure, and prepare for ingestion into Google Cloud Platform (GCP) BigQuery.

## 📌 Project Status
**Stage 1 – Local Ingestion Complete**  
- ✅ Fetch **Daily Adjusted Prices** for one or more stock symbols.  
- ✅ Save JSON data locally in a date-partitioned folder structure.  
- ✅ Configurable symbols and settings via `.env`.  
- 🔜 Next: Add fundamentals data, upload to Google Cloud Storage (GCS), and create BigQuery tables.

---

## 📂 Folder Structure

```plaintext
alpha-vantage-finance-warehouse/
│
├── ingestion/           # Python scripts for data ingestion
│   └── main.py           # Main ingestion entrypoint
│
├── sql/                 # SQL queries for BigQuery (future steps)
├── models/              # dbt or Dataform models (future steps)
├── tests/               # Data quality tests (future steps)
├── notebooks/           # Jupyter/Colab exploration (future steps)
├── assets/              # Images, diagrams, and visual assets
├── docs/                # Project documentation
├── .env                 # Environment variables (NOT committed)
├── .env.example         # Template for environment variables
├── .gitignore           # Files/folders to ignore in Git
├── README.md            # Project documentation
