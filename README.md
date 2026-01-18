# Medical-Telegram-Warehouse

##  Project Overview
End-to-end data pipeline for analyzing Ethiopian medical businesses from Telegram channels. Built for 10 Academy Week 8 Challenge.

##  Tasks Completed

### **Task 1: Data Scraping and Collection**
**Objective**: Extract medical product data from Ethiopian Telegram channels

**Results**:
- Scraped **5 medical channels**: @CheMed123, @lobelia4cosmetics, @tikvahpharma, @EAHCI, @tenamereja
- Collected **250 messages** with complete metadata
- Downloaded **194 product images** automatically
- Organized data lake with partitioned structure: `data/raw/telegram_messages/YYYY-MM-DD/`
- Implemented error handling, rate limiting, and comprehensive logging

**Key Features**:
- Asynchronous scraping with Telethon
- Automatic image downloading and organization
- JSON serialization with UTF-8 encoding
- Error recovery and duplicate prevention

---

### **Task 2: Data Modeling and Transformation**
**Objective**: Transform raw data into analytical star schema using dbt

**Results**:
- PostgreSQL database `medical_warehouse` created
- **250 messages** loaded into `raw.telegram_messages`
- Built **4-layer dbt pipeline**:
  1. **Staging**: `stg_telegram_messages` - Cleaned and standardized raw data
  2. **Dimensions**: 
     - `dim_channels` - Channel metadata and classification (5 channels)
     - `dim_dates` - Time dimension for analysis (1096 dates)
  3. **Fact**: `fct_messages` - Message metrics with foreign keys (250 fact rows)
- All dbt models built successfully in **2.55 seconds**
- Data quality tests implemented

**Star Schema Design**:
```
fct_messages (Fact)
    ├── dim_channels (Channel info & stats)
    ├── dim_dates (Time hierarchy)
    └── Message metrics (views, forwards, images)
```

**Analytical Capabilities**:
- Channel performance comparison
- Time-based trend analysis
- Image content analysis
- Product mention tracking


##  Technologies Used
- **Data Extraction**: Python, Telethon, asyncio
- **Data Storage**: PostgreSQL, JSON files
- **Data Transformation**: dbt (Data Build Tool)
- **Data Modeling**: Star Schema, Dimensional Modeling
- **Quality Assurance**: dbt tests, validation scripts

---

## Requirements Met

### Task 1:
- Working Telegram scraper with error handling
- At least 3 channels scraped (5 completed)
- At least 50 messages per channel (50 each)
- Images downloaded and organized
- JSON files in partitioned data lake
- Comprehensive logging

### Task 2:
- Raw data loaded to PostgreSQL
- dbt project with staging and mart models
- Star schema implemented (dimensions + fact table)
- Data quality tests
- Documentation generated

---

##  Quick Start

1. **Set up environment**:
   ```bash
   pip install -r requirements.txt
   cp .env.example .env  # Add Telegram API credentials
   ```

2. **Run scraper (Task 1)**:
   ```bash
   python src/scraper.py
   ```

3. **Load to database (Task 2)**:
   ```bash
   python src/load_to_postgres.py
   ```

4. **Transform with dbt**:
   ```bash
   cd medical_warehouse
   dbt run
   dbt test
   dbt docs serve
   ```

---

##  Data Insights
- **5 medical channels** analyzed covering pharmaceuticals, cosmetics, and health information
- **250 products/messages** available for analysis
- **77.6% of messages** contain product images
- Star schema enables business questions:
  - Which channels have highest engagement?
  - What are trending medical products?
  - How does visual content impact views?
