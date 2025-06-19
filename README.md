# Soil Health Data Pipeline

A comprehensive data engineering solution for scraping, processing, and consolidating and EDA of soil health data from the Indian Government's Soil Health Portal (soilhealth.dac.gov.in).

## 🎯 Project Overview

This project automates the extraction of macro and micro nutrient data from soil health reports across different states, districts, and blocks in India. The pipeline consists of two main components:

1. **Data Scraper** (`soil_health_scraper.py`) - Automated web scraping using Selenium
2. **Data Consolidator** (`soil_data_consolidator.py`) - Data cleaning, standardization, and consolidation
3. **Data Analysis & Insights** ('EDA.ipynb') -  Handling Missing Data and outliers, Data Visualization 

## 🏗️ Architecture

```
Raw Data Collection → Data Processing → Consolidated Output
     (Scraper)     →  (Consolidator) →   (Analysis Ready)
```

### Data Flow
1. **Scraping**: Multi-threaded scraping of macro and micro nutrient data
2. **Storage**: Organized file structure by year/state/district/block
3. **Processing**: Column standardization, data cleaning, and validation
4. **Consolidation**: Merging macro/micro data with comprehensive statistics
5. **EDA**: Handling the Missing Data and outliers

## 📁 Project Structure

```
soil-health-pipeline/
├── data/
│   ├── raw/                    # Scraped CSV files
│   │   └── YYYY/              # Year-wise organization
│   │       └── STATE/         # State-wise folders
│   │           └── DISTRICT/  # District-wise folders
│   │               └── *.csv  # Block-level data files
│   └── processed/             # Consolidated output
│       ├── soil_health_consolidated.csv
│       ├── soil_health_YYYY.csv
│       ├── consolidation_summary.txt
│       └── consolidation.log
├── soil_health_scraper.py     # Web scraping module
├── soil_data_consolidator.py  # Data processing module
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Chrome browser
- ChromeDriver (Windows path configured in scraper)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd soil-health-pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Setup ChromeDriver**
   - Download ChromeDriver from https://chromedriver.chromium.org/
   - Update the path in `soil_health_scraper.py` (line 19)
   - Current path: `C:\chromedriver-win64\chromedriver-win64\chromedriver.exe`

### Usage

#### 1. Data Scraping
```bash
# Run the scraper (both macro and micro nutrients)
python soil_health_scraper.py
```

**Scraper Features:**
- **Multi-threaded**: Simultaneous macro and micro nutrient scraping
- **Smart Skip Logic**: Automatically skips years with existing data
- **Robust Error Handling**: Handles stale elements and network issues
- **Organized Storage**: Hierarchical folder structure
- **Progress Tracking**: Detailed console output with emojis

#### 2. Data Consolidation
```bash
# Process and consolidate scraped data
python soil_data_consolidator.py
```

**Consolidator Features:**
- **Column Standardization**: Maps various column name formats
- **Data Cleaning**: Handles missing values, outliers, and type conversions
- **Macro-Micro Merging**: Intelligent joining of related datasets
- **Summary Statistics**: Comprehensive data quality reports
- **Multiple Outputs**: Consolidated file + yearly splits

### 3. Data Analysis & Insights
'''bash
# Handing Missing Data and EDA
EDA.ipynb

**Data Analysis & Insights Features**
- **Handing Missing Data**: Handles missing values,outliers of merged data
- **Handling Outliers**: Handles the outiers presenet in the Data
- **Data Visualization**: Visualized the Data
## 🔧 Configuration

### Scraper Configuration

```python
# Skip specific years (modify in __main__ section)
years_to_skip = ["2025-26", "2024-25"]

# Chrome ports for multi-threading
MACRO_PORT = 9222
MICRO_PORT = 9223
```

### Consolidator Configuration

```python
# Data paths
RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"

# Supported column variations (see column_mappings in code)
# The consolidator automatically maps 50+ column name variations
```

## 📊 Data Schema

### Input Data (Scraped)
- **Geographic**: Year, State, District, Block, Village
- **Farmer Info**: Name, Mobile, Survey Number, Area
- **Sample Info**: Sample ID, Collection Date
- **Nutrients**: pH, EC, OC, N, P, K, Fe, Mn, Cu, Zn, B, S

### Output Data (Consolidated)
- **Standardized Columns**: Consistent naming across all sources
- **Data Types**: Proper numeric/string type enforcement
- **Quality Flags**: Missing data percentages and validation
- **Metadata**: Source file tracking and processing timestamps

## 🎛️ Advanced Features

### Smart Data Handling
- **Duplicate Detection**: Prevents re-scraping existing data
- **Error Recovery**: Automatic page refresh and retry logic
- **Memory Optimization**: Efficient DataFrame operations
- **Logging**: Comprehensive error and progress logging

### Data Quality Assurance
- **Range Validation**: pH (0-14), nutrients (non-negative)
- **Missing Value Handling**: Multiple null value formats
- **Outlier Detection**: Statistical outlier identification
- **Consistency Checks**: Cross-field validation

## 📈 Output Analysis

### Consolidated Dataset
- **Format**: CSV with standardized columns
- **Size**: Typically 100K+ records across multiple years
- **Coverage**: Pan-India data with state/district/block granularity

### Summary Statistics
```
Total Records: 50,000+
Unique Farmers: 30,000+
Years Covered: 2018-2024
States Covered: 25+
Districts Covered: 500+
Blocks Covered: 2,000+
```

## 🔍 Troubleshooting

### Common Issues

1. **ChromeDriver Path Error**
   ```
   selenium.common.exceptions.WebDriverException: 'chromedriver' executable needs to be in PATH
   ```
   **Solution**: Update ChromeDriver path in `soil_health_scraper.py`

2. **Stale Element Reference**
   ```
   selenium.common.exceptions.StaleElementReferenceException
   ```
   **Solution**: Implemented automatic retry logic (handled internally)

3. **Empty Download Folder**
   ```
   Downloaded file not found
   ```
   **Solution**: Check Downloads folder permissions and Chrome settings

4. **Memory Issues**
   ```
   MemoryError during consolidation
   ```
   **Solution**: Process data in chunks or increase system memory

### Debug Mode
Enable detailed logging by modifying the logging level:
```python
logging.basicConfig(level=logging.DEBUG)
```

## 🛠️ Technical Details

### Dependencies
- **selenium**: Web automation and scraping
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical operations
- **pathlib**: Modern path handling
- **logging**: Comprehensive logging system

### Performance Optimization
- **Multi-threading**: Parallel macro/micro scraping
- **Efficient I/O**: Batch file operations
- **Memory Management**: Chunked data processing
- **Smart Caching**: Skip existing data logic

### Error Handling Strategy
- **Graceful Degradation**: Continue processing on individual failures
- **Comprehensive Logging**: Track all errors and warnings
- **Automatic Recovery**: Page refresh and retry mechanisms
- **Data Validation**: Multiple validation layers

## 🤝 Contributing

### Code Style
- Follow PEP 8 guidelines
- Use meaningful variable names
- Add comprehensive docstrings
- Include type hints where appropriate

### Testing
- Test with different year ranges
- Validate data quality metrics
- Check cross-platform compatibility
- Verify error handling scenarios

## 📋 Maintenance

### Regular Tasks
- **Update ChromeDriver**: Match Chrome browser version
- **Monitor Website Changes**: Check for UI updates on source site
- **Data Quality Review**: Regular statistical analysis
- **Performance Tuning**: Optimize for larger datasets

### Backup Strategy
- **Raw Data**: Keep original scraped files
- **Processed Data**: Version control consolidated outputs
- **Logs**: Archive processing logs for debugging

## 📄 License

This project is designed for educational and research purposes. Please ensure compliance with the source website's terms of service.

## 🆘 Support

For technical support or questions:
1. Check the troubleshooting section
2. Review the processing logs
3. Validate your Chrome/ChromeDriver setup
4. Ensure all dependencies are installed

---

**Note**: This pipeline is designed to handle the dynamic nature of government data portals with robust error handling and smart retry logic. The code is production-ready with comprehensive logging and monitoring capabilities.
