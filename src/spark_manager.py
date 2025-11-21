"""
Spark session management with MongoDB connector and optimized configuration.

This module provides a singleton SparkSession instance with MongoDB connector
pre-configured for distributed data processing. It handles Windows-specific
Hadoop configurations and ensures proper resource allocation.
"""
import os
import warnings
from typing import Optional
from pyspark.sql import SparkSession
from config import Config

# Suppress unnecessary warnings
warnings.filterwarnings('ignore')

class SparkManager:
    """
    Singleton manager for SparkSession with MongoDB integration.
    
    This class ensures a single SparkSession instance is created and reused
    throughout the application. It handles Windows-specific Hadoop configurations
    and provides a clean interface for Spark operations.
    
    Architecture Note:
    - Uses Hadoop/HDFS (or local folder) for scalable storage of large GTFS data
    - Leverages Spark for distributed computation (joins, aggregations, ML)
    - Integrates MongoDB for flexible schema storage and analytics
    """
    _instance: Optional['SparkManager'] = None
    _spark: Optional[SparkSession] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkManager, cls).__new__(cls)
            cls._instance._initialize_spark()
        return cls._instance
    
    def _setup_windows_hadoop(self):
        """Configure Hadoop environment for Windows systems."""
        if os.name != 'nt':
            return
            
        hadoop_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'hadoop'))
        bin_path = os.path.join(hadoop_path, "bin")
        os.makedirs(bin_path, exist_ok=True)

        if not os.environ.get("HADOOP_HOME"):
            os.environ["HADOOP_HOME"] = hadoop_path
        if bin_path not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{bin_path};{os.environ.get('PATH', '')}"
    
    def _initialize_spark(self):
        """Initialize and configure the SparkSession."""
        try:
            import findspark
            findspark.init()
            
            self._setup_windows_hadoop()
            
            # MongoDB Spark connector configuration
            mongo_pkg = "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0"
            
            self._spark = (
                SparkSession.builder
                .appName("UrbanMobilityPipeline")
                .config("spark.jars.packages", mongo_pkg)
                .config("spark.mongodb.read.connection.uri", Config().MONGO_URI)
                .config("spark.mongodb.write.connection.uri", Config().MONGO_URI)
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.driver.memory", Config.SPARK_DRIVER_MEMORY)
                .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY)
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
                .config("spark.cleaner.periodicGC.interval", "1min")
                .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .getOrCreate()
            )
            
            # Set log level to WARN to reduce verbosity
            self._spark.sparkContext.setLogLevel("WARN")
            
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Spark session: {str(e)}")
    
    @property
    def spark(self) -> SparkSession:
        """Get the SparkSession instance.
        
        Returns:
            SparkSession: The active SparkSession instance.
            
        Raises:
            RuntimeError: If SparkSession initialization failed.
        """
        if self._spark is None:
            self._initialize_spark()
        return self._spark
    
    def get_spark_session(self) -> SparkSession:
        """Alias for the spark property for backward compatibility.
        
        Returns:
            SparkSession: The active SparkSession instance.
            
        Raises:
            RuntimeError: If SparkSession initialization failed.
        """
        return SparkManager().spark

        
    @staticmethod
    def stop_spark(spark):
        """Stop Spark session gracefully"""
        if spark:
            spark.stop()
            print("Spark session stopped successfully!")

    @classmethod
    def get_spark_session(cls) -> SparkSession:
        """Alias for the spark property for backward compatibility."""
        instance = cls()
        return instance.spark
