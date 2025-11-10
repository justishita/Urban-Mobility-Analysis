import warnings
warnings.filterwarnings('ignore')

class SparkManager:
    """Manage Spark session with proper configuration and error handling"""
    
    @staticmethod
    def setup_spark():
        """Setup and return Spark session with optimized configuration"""
        try:
            import findspark
            findspark.init()
            
            from pyspark.sql import SparkSession
            from config import Config
            
            # Create Spark session with optimized configuration
            spark = SparkSession.builder \
                .appName("UrbanMobilityAnalysis") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.driver.memory", Config.SPARK_DRIVER_MEMORY) \
                .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY) \
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
                .getOrCreate()
            
            # Set log level to WARN to reduce verbosity
            spark.sparkContext.setLogLevel("WARN")
            
            print("Spark session created successfully!")
            print(f" Spark version: {spark.version}")
            print(f" Driver Memory: {Config.SPARK_DRIVER_MEMORY}")
            print(f" Executor Memory: {Config.SPARK_EXECUTOR_MEMORY}")
            
            return spark
            
        except ImportError as e:
            print(f"Spark dependencies not found: {e}")
            print("Please install: pip install pyspark findspark")
            return None
        except Exception as e:
            print(f"Error creating Spark session: {e}")
            return None
    
    @staticmethod
    def stop_spark(spark):
        """Stop Spark session gracefully"""
        if spark:
            spark.stop()
            print("Spark session stopped successfully!")
    
    @staticmethod
    def get_spark_config():
        """Get Spark configuration for different environments"""
        configs = {
            'local': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.driver.memory': '2g',
                'spark.executor.memory': '2g',
                'spark.sql.legacy.timeParserPolicy': 'LEGACY'
            },
            'cluster': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.driver.memory': '4g',
                'spark.executor.memory': '4g',
                'spark.executor.instances': '4',
                'spark.sql.legacy.timeParserPolicy': 'LEGACY'
            }
        }
        return configs