import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuration class for the Urban Mobility Intelligence System"""
    
    # MongoDB Configuration
    MONGO_URI = os.getenv('MONGO_URI')
    DATABASE_NAME = os.getenv('DATABASE_NAME')
    
    # Spark Configuration
    SPARK_DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '2g')
    SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '2g')
    
    # File paths
    DATA_PATHS = {
        'delhi': 'data/delhi/',
        'bangalore': 'data/bangalore/'
    }
    
    # Required GTFS files for each city
    REQUIRED_FILES = {
        'delhi': ['agency.txt', 'calendar.txt', 'routes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt'],
        'bangalore': ['agency.txt', 'calendar.txt', 'routes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt']
    }
    
    # Output configurations
    OUTPUT_PATHS = {
        'data': 'outputs/data/',
        'visuals': 'outputs/visuals/',
        'models': 'models/'
    }
    
    @classmethod
    def setup_directories(cls):
        """Create necessary directories for the project"""
        directories = [
            'outputs/visuals',
            'outputs/data', 
            'models',
            'data/delhi',
            'data/bangalore',
            'logs'
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            print(f"Created directory: {directory}")
        
        print(" All directories created successfully!")
    
    @classmethod
    def get_city_data_path(cls, city):
        """Get data path for a specific city"""
        return cls.DATA_PATHS.get(city, f"data/{city}/")
    
    @classmethod
    def get_required_files(cls, city):
        """Get required files for a specific city"""
        return cls.REQUIRED_FILES.get(city, [])