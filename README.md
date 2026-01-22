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


### **Task 3: Data Enrichment with Object Detection (YOLO)**
**Objective**: Use computer vision to analyze images and integrate findings into the data warehouse

**Results**:
- Processed **279 medical product images** using YOLOv8n
- Detected **418 total objects** across all images
- Classified images into **4 categories**:
  - **Lifestyle** (63 images, 22.6%) - People without products
  - **Product Display** (36 images, 12.9%) - Products/containers without people
  - **Promotional** (7 images, 2.5%) - People with products
  - **Other** (173 images, 62.0%) - No relevant objects detected
- Created `fct_image_detections` dbt model integrating with existing star schema
- Loaded results to PostgreSQL with data validation

**Key Insights**:
1. **Engagement Analysis**: Promotional posts get **788.05 more average views** than product display posts
2. **Channel Visual Content**:
   - @lobelia4cosmetics: 100% images (50/50 messages)
   - @CheMed123: 92% images (46/50 messages)
   - @EAHCI: 88% images (44/50 messages)
   - @tikvahpharma: 60% images (30/50 messages)
   - @tenamereja: 48% images (24/50 messages)
3. **Top Detected Objects**: person (60), bottle (19), clock (5), orange (3), chair (2)

**Technical Implementation**:
- **Script**: `src/yolo_detect.py` - Automated image scanning and classification
- **Loader**: `src/load_yolo_results.py` - PostgreSQL integration with error handling
- **Analysis**: `scripts/yolo_analysis_report.py` - Business insights extraction

**Limitations & Solutions**:
- **Limitation**: YOLOv8 generic categories (no medical specificity)
- **Solution**: Future fine-tuning on medical product datasets
- **Limitation**: Can't read text in images
- **Solution**: Integrate OCR for product name extraction


### **Task 4: Analytical API with FastAPI**
**Objective**: Expose data warehouse insights through RESTful API endpoints

**Results**:
- Built **4 analytical endpoints** serving business intelligence:
  1. **`/api/reports/top-products`** - Most frequently mentioned medical products
  2. **`/api/channels/{channel_name}/activity`** - Channel posting trends and engagement
  3. **`/api/search/messages`** - Search messages by keyword (e.g., "paracetamol")
  4. **`/api/reports/visual-content`** - Image usage statistics across channels
- Implemented **Pydantic schemas** for request/response validation
- Added comprehensive error handling and HTTP status codes
- Generated **automatic OpenAPI documentation** at `/docs`

**API Architecture**:
```python
api/
├── main.py              # FastAPI application
├── schemas.py           # Pydantic models
├── database.py          # PostgreSQL connection
└── endpoints/           # Route handlers
```

**Sample Requests**:
```bash
# Top 10 medical products
curl "http://localhost:8000/api/reports/top-products?limit=10"

# Channel activity for CheMed
curl "http://localhost:8000/api/channels/CheMed123/activity"

# Search for paracetamol mentions
curl "http://localhost:8000/api/search/messages?query=paracetamol&limit=20"
```

**Features**:
-  SQLAlchemy ORM for database operations
-  Async database connections
-  JWT authentication (ready for production)
-  Rate limiting and CORS configuration
-  Comprehensive API documentation


### **Task 5: Pipeline Orchestration with Dagster**
**Objective**: Automate the entire data pipeline with scheduling and monitoring

**Results**:
- Created **end-to-end orchestration pipeline** with 4 sequential operations:
  1. **`scrape_telegram_data`** - Run Telegram scraper
  2. **`load_raw_to_postgres`** - Load JSON to PostgreSQL
  3. **`run_dbt_transformations`** - Execute dbt models
  4. **`run_yolo_enrichment`** - Run object detection and load results
- Implemented **daily scheduling** (2 AM UTC) for automated execution
- Built **observable pipeline** with Dagster UI for monitoring
- Added **error recovery** and comprehensive logging

**Pipeline Architecture**:
```python
@pipeline
def medical_telegram_pipeline():
    scrape = scrape_telegram_data()
    load = load_raw_to_postgres()
    dbt = run_dbt_transformations()
    yolo = run_yolo_enrichment()
    
    load.depends_on(scrape)    # Load after scraping
    dbt.depends_on(load)       # Transform after loading
    yolo.depends_on(dbt)       # Enrich after transformation
```

**Key Features**:
- **Schedule**: Daily automated execution at 2 AM UTC
- **Monitoring**: Real-time pipeline visualization in Dagster UI
- **Error Handling**: Automatic retries and failure notifications
- **Dependencies**: Proper execution order enforcement
- **Logging**: Centralized log management across all steps

**Usage**:
```bash
# Start Dagster UI
dagster dev -f pipeline.py

# Access UI at: http://localhost:3000

# Manual pipeline execution
dagster pipeline execute -f pipeline.py
```

**Screenshots Available**:
1. Dagster UI showing pipeline graph
2. Successful pipeline execution (all green checks)
3. Schedule configuration (daily at 2 AM)
4. Step-by-step execution logs


##  Technologies Used
- **Data Extraction**: Python 3.8+, Telethon, asyncio
- **Data Storage**: PostgreSQL 15, JSON files, organized directories
- **Data Transformation**: dbt (Data Build Tool), SQL
- **Computer Vision**: YOLOv8, Ultralytics, OpenCV
- **API Development**: FastAPI, Pydantic, SQLAlchemy, Uvicorn
- **Orchestration**: Dagster, Dagster-Webserver
- **Data Modeling**: Kimball Star Schema, Dimensional Modeling
- **Quality Assurance**: dbt tests, custom validation scripts, Pydantic validation
- **Documentation**: dbt docs, OpenAPI/Swagger, markdown reports


##  Business Insights Summary

### **Channel Analysis**:
1. **Visual Content Leaders**: 
   - Cosmetics channels (@lobelia4cosmetics) use 100% images
   - Medical channels average 75% image usage
   - Information channels use less visual content (48%)

2. **Engagement Patterns**:
   - Promotional content (people + products) drives highest engagement
   - Product-only images have lower view counts
   - Channels with consistent visual content maintain higher engagement

3. **Content Strategy**:
   - Pharmaceutical channels focus on product display
   - Cosmetics channels use lifestyle imagery
   - Medical training channels use educational content

### **Technical Architecture Benefits**:
- **Scalability**: Modular design allows easy addition of new channels
- **Maintainability**: Clear separation of concerns across 5 tasks
- **Extensibility**: API-first approach enables new analytical queries
- **Reliability**: Comprehensive error handling and data validation
- **Observability**: Full pipeline monitoring with Dagster
