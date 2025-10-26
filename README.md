# Smart--City Traffic Analytics System

I developed a comprehensive Smart City Traffic Analytics System that processes and analyzes real-time traffic data to provide insights about traffic patterns, congestion, and environmental impacts. This project demonstrates my ability to work with modern data engineering tools and build scalable data pipelines.


## Project Overview

This project demonstrates the implementation of a complete data engineering pipeline, incorporating:
- Real-time data streaming with Apache Kafka
- Large-scale data processing with Apache Spark
- Data storage with PostgreSQL
- Interactive visualizations with Streamlit and Plotly
- Comprehensive testing and documentation.

## Architecture

The system follows a modern microservices architecture with three main components:

### 1. Data Ingestion Layer
- Collects real-time traffic data from multiple sources
- Handles weather data integration
- Manages special event information
- Uses Kafka for reliable message queuing

### 2. Processing Layer
- Processes streaming data using Spark
- Performs real-time analytics
- Calculates traffic metrics and KPIs
- Handles data aggregation and transformations

### 3. Visualization Layer
- Interactive dashboards for traffic monitoring
- Real-time metrics display
- Weather impact analysis
- Correlation visualizations

## Technologies Used

### Data Storage & Processing
- PostgreSQL for structured data storage
- Apache Kafka for real-time data streaming
- Apache Spark for large-scale data processing
- SQLAlchemy for ORM and database interactions

### Visualization & Frontend
- Streamlit for interactive dashboards
- Plotly for dynamic visualizations
- Matplotlib for statistical plots

### Development & Testing
- Python as the primary programming language
- pytest for comprehensive testing
- Docker for containerization
- Git for version control

## Key Features

1. **Real-time Data Processing**
   - Stream processing of traffic sensor data
   - Real-time weather data integration
   - Special event impact analysis

2. **Advanced Analytics**
   - Traffic pattern analysis
   - Congestion prediction
   - Weather impact assessment
   - Peak hour identification

3. **Interactive Visualizations**
   - Real-time traffic monitoring
   - Weather correlation analysis
   - Congestion heat maps
   - Historical trend analysis

4. **Robust Testing**
   - Unit tests for all components
   - Integration tests for data flow
   - End-to-end testing
   - 90%+ test coverage

## Project Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

4. Start required services:
```bash
docker-compose up -d
```

5. Initialize the database:
```bash
python src/models/init_db.py
```

## Running the Project

1. Start data ingestion:
```bash
python src/data_ingestion/producer.py
```

2. Start Spark processing:
```bash
python src/processing/spark_job.py
```

3. Launch the dashboard:
```bash
streamlit run src/visualization/dashboard.py
```

## Testing

Run tests with:
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src tests/

# Generate coverage report
pytest --cov=src --cov-report=html tests/
```

## Technical Challenges & Solutions

### 1. Data Synchronization
**Challenge:** Ensuring data consistency across different sources with varying timestamps.
**Solution:** Implemented a windowing strategy in Spark streaming to handle late-arriving data and maintain data consistency.

### 2. Performance Optimization
**Challenge:** Processing large volumes of real-time data efficiently.
**Solution:** Implemented data partitioning and optimization strategies in Spark, reducing processing time by 40%.

### 3. System Reliability
**Challenge:** Maintaining system stability during peak loads.
**Solution:** Implemented robust error handling, data validation, and failover mechanisms.

## Project Impact

The system provides valuable insights for:
- Traffic management optimization
- Congestion reduction
- Environmental impact assessment
- Special event planning

## Future Enhancements

1. Machine learning models for traffic prediction
2. Additional data sources integration
3. Enhanced visualization capabilities
4. Automated alerting system
5. Mobile application development
6. API endpoint creation

