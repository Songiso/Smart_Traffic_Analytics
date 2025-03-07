import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from schema import Base

# Load environment variables
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Create database URL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def init_database():
    """Initialize the database with all tables."""
    try:
        # Create engine
        engine = create_engine(DATABASE_URL)
        
        # Create all tables
        Base.metadata.create_all(engine)
        
        print("Database initialized successfully!")
        
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        raise

if __name__ == "__main__":
    init_database()