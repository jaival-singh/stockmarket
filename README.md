# 📈 NIFTY-100 Stock Market Data Engineering & Analytics Pipeline

An end-to-end **data engineering and analytics pipeline** for **NIFTY-100 stock market data**, built using **Python, Apache Cassandra (Astra DB), Pandas, PySpark, and multithreading**.

The pipeline ingests **latest available market data on-demand from Yahoo Finance (via `yfinance`)**, enabling **near real-time, continuously refreshable analytics** over Indian equity markets.

This project follows the **Bronze–Silver–Gold architecture** and extends into **business-focused analytics**, including:
- Top & worst performing stocks (quarterly performance ranking)
- Technical indicators (EMA, RSI)
- Volume spike detection for market activity signals
- Insight-driven data visualizations

## ⭐ Quick Highlights
- Near real-time **stock data ingestion** using Yahoo Finance  
- End-to-end **Bronze–Silver–Gold data architecture**  
- **Cassandra-based time-series modeling** with analytics-optimized schemas  
- **Multithreaded ingestion and processing** for scalable pipelines  
- **Technical indicator enrichment** (EMA-20, EMA-50, RSI-14) for trend and momentum analysis  
- **Quarterly ranking of top and worst performing NIFTY-100 stocks** based on returns  
- **Volume spike detection** to identify unusual trading activity  
- **Business-ready visual insights** through time-series analytics and plots  
- **Production-style data validation** at each pipeline layer


---

## 🏗️ Architecture Overview

Yahoo Finance (yfinance)
↓
Bronze Layer (Raw Data - Cassandra)
↓
Silver Layer (Cleaned & Standardized Data)
↓
Gold Layer (Technical Indicators)
↓
Analytics Layer (Performance & Visual Insights)


---

## 🧱 Tech Stack

- Python  
- Apache Cassandra / Astra DB  
- Yahoo Finance (`yfinance`)  
- Pandas  
- PySpark  
- Multithreading (`ThreadPoolExecutor`)  
- Matplotlib  

---

## 📂 Project Structure

├── insert_bronze.py
├── inspect_bronze.py
├── silver.py
├── inspect_silver.py
├── gold_one.py
├── inspect_gold_one.py
├── topperformersquaterly.py
├── inspect_topperformersquaterly.py
├── top_performers_visual.py
├── worstperformersquaterly.py
├── inspect_worst_performers.py
├── worst_performers_visual.py
├── volume_visual.py
├── credentials/
│ ├── secure-connect-database.zip
│ └── database-token.json
└── README.md


---

## 🟤 Bronze Layer – Raw Data Ingestion

### `insert_bronze.py`

#### Purpose
- Ingest **10 years of daily stock data** for NIFTY-100 companies from Yahoo Finance  
- Store raw **OHLCV** data into **Cassandra**

#### Key Features
- Multithreaded ingestion for faster processing  
- Optimized time-series Cassandra schema  
- Automatic table creation and truncation  
- Progress logging during ingestion  

#### Schema

```sql
stocks (
    ticker TEXT,
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    adj_close FLOAT,
    volume BIGINT,
    PRIMARY KEY (ticker, date)
)
inspect_bronze.py
Purpose

Validate Bronze layer data quality

Checks Performed

Total row count

Column count

Sample records

NULL value analysis using PySpark

Record count per ticker

## ⚪ Silver Layer – Cleaned & Standardized Data

### `silver.py`

#### Purpose
- Transform raw Bronze data into a **clean, analytics-ready format**

#### Transformations
- Convert `FLOAT` values to `DECIMAL`
- Normalize date formats
- Batch inserts with **QUORUM consistency**
- Multithreaded batch processing

#### Schema

```sql
stocks_silver (
    ticker TEXT,
    date DATE,
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    adj_close DECIMAL,
    volume BIGINT,
    PRIMARY KEY (ticker, date)
)


### `inspect_silver.py`

#### Purpose
- Validate Silver layer data
- Inspect sample records and confirm schema correctness

---

## 🟡 Gold Layer – Technical Indicators

### `gold.py`

#### Purpose
- Enrich stock data with commonly used technical indicators

#### Indicators Computed
- EMA (20-day)
- EMA (50-day)
- RSI (14-day)

#### Schema

```sql
gold_stocks (
    ticker TEXT,
    date DATE,
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    volume BIGINT,
    ema_20 DECIMAL,
    ema_50 DECIMAL,
    rsi DECIMAL,
    PRIMARY KEY (ticker, date)
)
# 📊 Analytics & Insights Layer

This layer converts curated data into **actionable financial insights** by validating outputs, ranking stock performance, and visualizing key market signals.

---

## 🟡 Gold Layer Validation

### `inspect_gold.py`
**Purpose**
- Validate Gold layer output  
- Verify indicator calculations  

---

## 🟢 Quarterly Top Performing Stocks

### `topperformersquaterly.py`
**Purpose**
- Identify top-performing **NIFTY-100** stocks over the last **3 months**

**Logic**
- Fetch last **90 days** of data from `stocks_silver`
- Calculate **percentage price change**
- Store top performers in a sorted Cassandra table

**Schema**
```sql
top_performing_stocks_by_percent (
    dummy_partition TEXT,
    percent_change DECIMAL,
    ticker TEXT,
    start_date DATE,
    end_date DATE,
    start_price DECIMAL,
    end_price DECIMAL,
    PRIMARY KEY (dummy_partition, percent_change)
) WITH CLUSTERING ORDER BY (percent_change DESC);

# 🟢 Quarterly Top Performing Stocks

### `inspect_topperformersquaterly.py`
**Purpose**
- Inspect and validate top-performing stocks

**Display**
- Percentage change  
- Price window (start vs end)

---

### `top_performers_visual.py`
**Purpose**
- Visualize price movement of top-performing stocks

**Output**
- Line plots showing growth trends over the quarter 📈

---

## 🔴 Quarterly Worst Performing Stocks

### `worstperformersquaterly.py`
**Purpose**
- Identify worst-performing stocks over the last **3 months**

**Schema**
```sql
worst_performing_stocks_by_percent (
    dummy_partition TEXT,
    percent_change DECIMAL,
    ticker TEXT,
    start_date DATE,
    end_date DATE,
    start_price DECIMAL,
    end_price DECIMAL,
    PRIMARY KEY (dummy_partition, percent_change)
) WITH CLUSTERING ORDER BY (percent_change ASC);


### `inspect_worst_performers.py`
**Purpose**
- Inspect and validate worst-performing stocks

---

### `worst_performers_visual.py`
**Purpose**
- Visualize declining stock trends

**Output**
- Red-colored line charts for intuitive interpretation 📉

---

## 📦 Volume Spike Detection

### `volume_visual.py`
**Purpose**
- Detect unusual trading activity using volume spikes

**Metric**
```text
Volume Spike = Latest Day Volume / Average Volume of Previous 5 Days
