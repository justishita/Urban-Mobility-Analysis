import os
import warnings
warnings.filterwarnings('ignore')
class SparkManager:
    """Manage Spark session with MongoDB and Windows Hadoop fix"""

    @staticmethod
    def setup_spark():
        """Setup and return Spark session with MongoDB + optimized configuration"""
        try:
            import findspark
            findspark.init()

            from pyspark.sql import SparkSession
            from config import Config
            import os

            # Windows Hadoop setup
            if os.name == 'nt':
                hadoop_path = r"C:\hadoop"
                bin_path = os.path.join(hadoop_path, "bin")
                os.makedirs(bin_path, exist_ok=True)

                if not os.environ.get("HADOOP_HOME"):
                    os.environ["HADOOP_HOME"] = hadoop_path
                    print(f"✅ HADOOP_HOME set to {hadoop_path}")
                if bin_path not in os.environ.get("PATH", ""):
                    os.environ["PATH"] += f";{bin_path}"
                    print(f"✅ Added {bin_path} to PATH")

            mongo_pkg = "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"

            spark = (
                SparkSession.builder
                .appName("UrbanMobilityPipeline")
                # .config("spark.jars.packages", mongo_pkg)
                # .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/urban_mobility_analysis")
                # .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/urban_mobility_analysis")
                # .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                # .config("spark.sql.adaptive.enabled", "true")
                # .config("spark.driver.memory", Config.SPARK_DRIVER_MEMORY)
                # .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY)
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
                .getOrCreate()
            )

            spark.sparkContext.setLogLevel("WARN")
            print("Spark session created successfully with MongoDB connector!")
            print(f"Spark version: {spark.version}")
            return spark

        except Exception as e:
            print(f"Error creating Spark session: {e}")
            import traceback; traceback.print_exc()
            return None
        
    @staticmethod
    def stop_spark(spark):
        """Stop Spark session gracefully"""
        if spark:
            spark.stop()
            print("Spark session stopped successfully!")
