# Data Sources Documentation

This document describes the data sources, structures, and collection methods used in the FX-AlphaLab project.

## Economic Calendar Data

### Source
- **Provider**: Investing.com Economic Calendar
- **URL**: https://www.investing.com/economic-calendar/
- **Update Frequency**: Real-time
- **Coverage**: Global economic events with focus on major economies

### Data Collection Method
The economic calendar data is collected using the `EconomicCalendarCollector` class located at `data/ingestion/calendar_collector.py`.

#### Features
- **Respectful Scraping**: Robots.txt compliance with configurable delays (3-5 seconds default)
- **Error Handling**: Retry logic with exponential backoff
- **User-Agent Rotation**: Multiple browser user agents to avoid detection
- **Missing Data Handling**: Graceful handling of events without actual values
- **Rate Limiting**: Built-in delays to respect server limits

### Data Structure

#### Economic Event Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `date` | String | Event date in YYYY-MM-DD format | "2024-02-08" |
| `time` | String | Event time (local timezone) | "13:30" |
| `country` | String | Country name | "United States" |
| `event` | String | Economic event name | "Non-Farm Payrolls" |
| `impact` | String | Market impact level (High/Medium/Low/Unknown) | "High" |
| `actual` | String | Actual released value (null if not released) | "150K" |
| `forecast` | String | Forecasted value | "180K" |
| `previous` | String | Previous period value | "160K" |
| `event_url` | String | URL to detailed event page (optional) | "https://www.investing.com/events/non-farm-payrolls" |
| `scraped_at` | String | Timestamp when data was collected (UTC) | "2024-02-08T12:00:00" |

#### Impact Levels
- **High**: Major market-moving events (e.g., Non-Farm Payrolls, Fed decisions)
- **Medium**: Significant economic indicators (e.g., CPI, GDP)
- **Low**: Minor economic releases
- **Unknown**: Impact level could not be determined

#### Value Formats
- **Numeric values**: Raw numbers with suffixes (K, M, B) for thousands, millions, billions
- **Percentages**: Values with % symbol
- **Null values**: Missing or unreleased data represented as null/None
- **Special cases**: "-", "N/A", "NA" are cleaned to null

### Output Formats

#### CSV Export
Default filename: `economic_events.csv`

```csv
date,time,country,event,impact,actual,forecast,previous,event_url,scraped_at
2024-02-08,13:30,United States,Non-Farm Payrolls,High,150K,180K,160K,https://www.investing.com/events/non-farm-payrolls,2024-02-08T12:00:00
2024-02-08,14:00,Eurozone,ECB Interest Rate Decision,Medium,4.50%,4.50%,4.50%,,2024-02-08T12:00:00
```

#### Pandas DataFrame
The `get_events_dataframe()` method returns a pandas DataFrame with:
- Automatic datetime conversion for date columns
- Proper data type inference
- Missing value handling (NaN for null values)

### Supported Countries/Regions

#### Primary Coverage
- **United States (us)**: Federal Reserve decisions, employment data, inflation metrics
- **Eurozone (eu)**: ECB decisions, inflation, GDP, unemployment
- **United Kingdom (uk)**: Bank of England decisions, economic indicators

#### Extended Coverage
The collector can be configured to collect data from additional countries by modifying the `countries` parameter in the `collect_events()` method.

### Usage Examples

#### Basic Usage
```python
from data.ingestion.calendar_collector import EconomicCalendarCollector

# Initialize collector
collector = EconomicCalendarCollector()

# Collect today's events
events = collector.collect_events(
    start_date=None,  # Today
    end_date=None,    # Today only
    countries=["us", "eu", "uk"]
)

# Save to CSV
collector.save_to_csv(events, "economic_events.csv")

# Get as DataFrame
df = collector.get_events_dataframe(events)
```

#### Date Range Collection
```python
# Collect events for a specific date range
events = collector.collect_events(
    start_date="2024-02-01",
    end_date="2024-02-07",
    countries=["us", "eu", "uk"]
)
```

#### Custom Configuration
```python
# Collector with custom settings
collector = EconomicCalendarCollector(
    min_delay=5.0,    # 5-second minimum delay
    max_delay=8.0,    # 8-second maximum delay
    max_retries=5,    # 5 retry attempts
    timeout=45        # 45-second timeout
)
```

### Error Handling

#### Common Issues
1. **Network Failures**: Automatic retry with exponential backoff
2. **Rate Limiting**: Respects HTTP 429 status codes with delay adjustments
3. **Missing Data**: Events without actual values are still captured
4. **HTML Structure Changes**: Robust parsing with fallback selectors

#### Logging
All operations are logged with appropriate levels:
- **INFO**: Successful operations and data collection summaries
- **WARNING**: Retry attempts and robots.txt restrictions
- **ERROR**: Failed requests and parsing errors

### Data Quality Considerations

#### Validation
- Date format validation (YYYY-MM-DD)
- Numeric value cleaning and normalization
- Country name standardization
- Impact level classification

#### Limitations
- **Time Zone**: Event times are in local timezone of the country
- **Historical Data**: Limited to what's available on Investing.com
- **Real-time**: Actual values may appear with delay after event time
- **Source Dependency**: Subject to website structure changes

### Maintenance

#### Regular Updates
- Monitor Investing.com website structure changes
- Update HTML selectors if parsing fails
- Review and adjust delay settings based on server response
- Validate data quality and completeness

#### Monitoring
- Track request success/failure rates
- Monitor average data collection times
- Log robots.txt compliance issues
- Alert on significant data anomalies

## Future Enhancements

### Planned Features
1. **Additional Data Sources**: Integration with other economic calendar providers
2. **Real-time Streaming**: WebSocket integration for live updates
3. **Historical Archive**: Automated historical data collection
4. **API Integration**: Direct API access where available
5. **Data Enrichment**: Additional metadata and classifications

### Scalability
- **Parallel Processing**: Multi-threaded collection for date ranges
- **Caching**: Local caching to avoid duplicate requests
- **Database Storage**: Direct database integration
- **Scheduled Collection**: Automated daily/weekly collection

## Dependencies

### Required Libraries
- `requests`: HTTP client for web requests
- `beautifulsoup4`: HTML parsing
- `pandas`: Data manipulation and DataFrame support
- `python-dotenv`: Environment variable management

### Optional Libraries
- `lxml`: Faster XML/HTML parsing (recommended for production)
- `requests-cache`: Response caching for repeated requests
- `tqdm`: Progress bars for long-running collections

## Security Considerations

### Web Scraping Ethics
- Always respects robots.txt rules
- Implements reasonable delays between requests
- Uses proper identification headers
- Avoids excessive load on target servers

### Data Privacy
- No personal data collection
- Only public economic event data
- Secure storage of collected data
- Compliance with data protection regulations
