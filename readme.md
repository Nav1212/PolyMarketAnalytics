# PolyMarketAnalytics

A project to create an efficient data warehouse for Polymarket using DuckDB.

## Features
- ** Data Warehousing**: Leverages DuckDB for creating and managing an optimized data warehouse. Currently bronze layer is implemented with Parquet files, silver layer is coming soon.
- **Polymarket Data Integration**: Designed to work with Polymarket data to facilitate analytics and insights. Sources are CLOB, Data, and Gamma APIs will expand soon.


Simple design of the current tool
<img width="779" height="358" alt="image" src="https://github.com/user-attachments/assets/adf1ea6e-15a8-4cb6-b50d-34ed2a1ad797" />
The fetchers all run in parallel and can have multiple workers depending on need. In the future I plan on allocating them dynamically depending on the speed of the API. 

## Prerequisites
- Python (primary programming language used in the project)
- DuckDB (for database interaction)
-alot of libraries, when im done ill make it more easily deployable.

## How to Use
1. Clone the repository:
   ```bash
   git clone https://github.com/Nav1212/PolyMarketAnalytics.git
