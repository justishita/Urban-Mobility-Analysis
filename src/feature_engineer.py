import os
from datetime import datetime
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml import Pipeline
from config import Config
import traceback
import numpy as np

# try to import pymongo for fallback reads
try:
    from pymongo import MongoClient
    PYMONGO_AVAILABLE = True
except Exception:
    PYMONGO_AVAILABLE = False

class FeatureEngineer:
    """Feature engineering for predictive modeling (robust Mongo + fallback)"""

    def __init__(self, spark):
        self.spark = spark
        self.mongo_uri = Config().MONGO_URI
        self.database_name = Config().DATABASE_NAME

    def _spark_read_mongo(self, collection_name):
        """
        Try to read a collection via Spark's Mongo connector using format 'mongodb'.
        If that fails, raise exception to trigger fallback.
        """
        try:
            df = self.spark.read.format("mongodb") \
                .option("uri", self.mongo_uri) \
                .option("database", self.database_name) \
                .option("collection", collection_name) \
                .load()
            return df
        except Exception as e:
            raise

    def _pymongo_to_spark(self, collection_name):
        """Fallback: use pymongo -> pandas -> spark"""
        if not PYMONGO_AVAILABLE:
            raise RuntimeError("pymongo not available for fallback reads")
        client = None
        try:
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=10000)
            db = client[self.database_name]
            cursor = db[collection_name].find({}, {'_id': 0})
            docs = list(cursor)
            if not docs:
                return None
            pdf = pd.DataFrame(docs)
            # convert pandas -> spark DataFrame (let Spark infer schema)
            sdf = self.spark.createDataFrame(pdf)
            return sdf
        finally:
            if client:
                client.close()

    def _read_collection(self, collection_name):
        """Read a Mongo collection via spark mongodb connector, fallback to pymongo"""
        try:
            return self._spark_read_mongo(collection_name)
        except Exception as e:
            print(f" Spark Mongo read failed for {collection_name}: {e}")
            print(" Attempting PyMongo fallback...")
            try:
                return self._pymongo_to_spark(collection_name)
            except Exception as e2:
                print(f" PyMongo fallback failed: {e2}")
                return None

    def create_modeling_dataset(self):
        """Create dataset for predictive modeling — aggregates hourly features"""
        print("Creating modeling dataset...")
        all_features = []
        for city in ['delhi', 'bangalore']:
            print(f"Processing {city}...")
            try:
                ride_features = self.get_ride_features(city)
                if ride_features is not None:
                    all_features.append(ride_features)
                    print(f"  Added ride features for {city}")
                else:
                    print(f"  No ride features for {city}")
            except Exception as e:
                print(f"  Error processing {city}: {e}")
                traceback.print_exc()

        if not all_features:
            print("No features data available")
            return None

        # union all dataframes (ensure same schema)
        modeling_df = all_features[0]
        for df in all_features[1:]:
            modeling_df = modeling_df.unionByName(df, allowMissingColumns=True)

        # add demand index

        modeling_df = modeling_df.withColumn(
        "is_peak_hour",
        when(
            ((col("hour_of_day") >= 8) & (col("hour_of_day") <= 11)) |
            ((col("hour_of_day") >= 16) & (col("hour_of_day") <= 21)),
            1
        ).otherwise(0))


        modeling_df = modeling_df.withColumn(
            "demand_index",
            when(col("is_peak_hour") == 1, col("ride_count") * 1.5).otherwise(col("ride_count"))
        )

        print(f"Created modeling dataset with {modeling_df.count()} records")
        return modeling_df

    def get_ride_features(self, city):
        """Extract hourly aggregated features from rides collection"""
        collection_name = f"{city}_rides"
        print(f"  Loading {collection_name} ...")
        rides_df = self._read_collection(collection_name)
        if rides_df is None:
            print(f"    No data found in {collection_name}")
            return None

        # ensure timestamp exists & proper type
        try:
            # safe-cast timestamp if it's a string
            rides_df = rides_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
        except Exception:
            pass

        # make sure required numeric columns exist; if missing, add with nulls / defaults
        numeric_cols = {
            'fare': 0.0,
            'distance_km': 0.0,
            'duration_mins': 0.0,
            'surge_multiplier': 1.0
        }
        for nc, default in numeric_cols.items():
            if nc not in rides_df.columns:
                rides_df = rides_df.withColumn(nc, lit(default))

        rides_with_features = rides_df \
            .withColumn("hour_of_day", hour(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp"))) \
            .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
            .withColumn("is_peak_hour",
                        when(((col("hour_of_day") >= 8) & (col("hour_of_day") <= 11)) |
                             ((col("hour_of_day") >= 16) & (col("hour_of_day") <= 21)), 1).otherwise(0)) \
            .withColumn("fare_per_km", when(col("distance_km") > 0, col("fare") / col("distance_km")).otherwise(col("fare")))

        # group and aggregate (hour_of_day may be null if timestamp missing; filter those)
        rides_with_features = rides_with_features.filter(col("hour_of_day").isNotNull())

        features_df = rides_with_features.groupBy("hour_of_day", "is_weekend").agg(
            avg("fare").alias("avg_fare"),
            avg("fare_per_km").alias("avg_price_per_km"),
            avg("surge_multiplier").alias("avg_surge_multiplier"),
            stddev("surge_multiplier").alias("std_surge_multiplier"),
            count("*").alias("ride_count"),
            avg("distance_km").alias("avg_distance_km"),
            avg("duration_mins").alias("avg_duration_mins")
        ).withColumn("city", lit(city))

        # fill nulls for stddev etc.
        features_df = features_df.fillna({
            "std_surge_multiplier": 0.0,
            "avg_price_per_km": 0.0,
            "avg_fare": 0.0,
            "avg_distance_km": 0.0,
            "avg_duration_mins": 0.0
        })

        print(f"    Generated {features_df.count()} feature records for {city}")
        return features_df

    def create_simple_modeling_dataset(self):
        """Fallback to raw rides features (non-aggregated)"""
        print("Creating simple modeling dataset as fallback...")
        all_features = []
        for city in ['delhi', 'bangalore']:
            collection_name = f"{city}_rides"
            try:
                rides_df = self._read_collection(collection_name)
                if rides_df is None:
                    continue
                # add basic time features
                rides_df = rides_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
                rides_df = rides_df.withColumn("hour_of_day", hour(col("timestamp"))) \
                                   .withColumn("day_of_week", dayofweek(col("timestamp"))) \
                                   .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
                                   .withColumn("is_peak_hour", when(((col("hour_of_day") >= 8) & (col("hour_of_day") <= 11)) |
                                                                  ((col("hour_of_day") >= 16) & (col("hour_of_day") <= 21)), 1).otherwise(0)) \
                                   .withColumn("fare_per_km", when(col("distance_km") > 0, col("fare") / col("distance_km")).otherwise(col("fare")))
                rides_df = rides_df.select(
                    "surge_multiplier", "hour_of_day", "is_weekend", "is_peak_hour",
                    "fare", "distance_km", "duration_mins", "fare_per_km"
                ).withColumn("city", lit(city))
                all_features.append(rides_df)
                print(f"  Added {rides_df.count()} records from {city}")
            except Exception as e:
                print(f"  Error processing {city}: {e}")
                traceback.print_exc()

        if not all_features:
            print("No data available for modeling")
            return None

        modeling_df = all_features[0]
        for df in all_features[1:]:
            modeling_df = modeling_df.unionByName(df, allowMissingColumns=True)

        print(f"Created simple modeling dataset with {modeling_df.count()} records")
        return modeling_df

    def prepare_features_for_ml(self, modeling_df, problem_type='regression'):
        """Prepare features and return processed df, pipeline model, and feature list"""
        print(f"Preparing features for {problem_type}...")
        try:
            if problem_type == 'regression':
                feature_cols = ['hour_of_day', 'is_weekend', 'is_peak_hour',
                                'ride_count', 'avg_fare', 'avg_price_per_km',
                                'avg_distance_km', 'avg_duration_mins', 'demand_index']
                label_col = 'avg_surge_multiplier'
            else:
                feature_cols = ['hour_of_day', 'is_weekend', 'is_peak_hour',
                                'ride_count', 'avg_fare', 'avg_price_per_km',
                                'avg_distance_km', 'avg_duration_mins', 'demand_index']
                modeling_df = modeling_df.withColumn('high_surge', when(col('avg_surge_multiplier') > 1.5, 1).otherwise(0))
                label_col = 'high_surge'

            # ensure label exists (fill with reasonable default)
            if label_col not in modeling_df.columns:
                modeling_df = modeling_df.withColumn(label_col, lit(0))

            indexer = StringIndexer(inputCol="city", outputCol="city_index", handleInvalid="keep")
            assembler = VectorAssembler(inputCols=feature_cols + ["city_index"], outputCol="raw_features", handleInvalid="keep")
            scaler = StandardScaler(inputCol="raw_features", outputCol="scaled_features", withStd=True, withMean=False)

            pipeline = Pipeline(stages=[indexer, assembler, scaler])
            feature_model = pipeline.fit(modeling_df)
            processed_df = feature_model.transform(modeling_df)
            processed_df = processed_df.withColumn("label", col(label_col).cast("double"))

            print(f"Prepared {len(feature_cols)} features for ML")
            return processed_df, feature_model, feature_cols + ["city_index"]
        except Exception as e:
            print(f"Error preparing features: {e}")
            traceback.print_exc()
            return None, None, []
