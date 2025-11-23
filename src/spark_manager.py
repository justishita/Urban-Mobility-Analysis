import os
import warnings
from typing import Optional
from pyspark.sql import SparkSession
from config import Config

warnings.filterwarnings('ignore')

class SparkManager:
    _instance: Optional['SparkManager'] = None
    _spark: Optional[SparkSession] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkManager, cls).__new__(cls)
            cls._instance._initialize_spark()
        return cls._instance
    
    def _setup_windows_hadoop(self):
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
        try:
            import findspark
            findspark.init()
            
            self._setup_windows_hadoop()
            
            # MongoDB Spark connector configuration
            mongo_packages = [
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
                "org.mongodb:mongodb-driver-sync:4.11.1",
                "org.mongodb:bson:4.11.1",
                "org.mongodb:mongodb-driver-core:4.11.1"
            ]
            
            packages_str = ",".join(mongo_packages)
            
            self._spark = (
                SparkSession.builder
                .appName("UrbanMobilityPipeline")
                .config("spark.jars.packages", packages_str)
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
                .config("spark.local.dir", "C:/spark_temp") 
                .config("spark.test.noDeleteOutput", "true")
                .master("local[*]")
                .getOrCreate()
            )
            
            self._spark.sparkContext.setLogLevel("WARN")
            print("Spark session created successfully with MongoDB support")
            
        except Exception as e:
            print(f"Spark session with MongoDB failed: {e}")
            print("Falling back to basic Spark session...")
            self._initialize_basic_spark()
    
    def _initialize_basic_spark(self):
        """Initialize basic Spark session without MongoDB dependencies"""
        try:
            self._spark = (
                SparkSession.builder
                .appName("UrbanMobilityPipeline")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .master("local[*]")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("WARN")
            print("Basic Spark session created (MongoDB features disabled)")
        except Exception as e:
            print(f"Basic Spark session also failed: {e}")
            raise RuntimeError(f"Failed to initialize any Spark session: {str(e)}")
    
    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._initialize_spark()
        return self._spark
    
    @classmethod
    def get_spark_session(cls) -> SparkSession:
        instance = cls()
        return instance.spark

    @staticmethod
    def stop_spark(spark):
        if spark:
            spark.stop()
            print("Spark session stopped successfully!")