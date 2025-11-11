"""
Main pipeline orchestration for Urban Mobility Intelligence System
"""
import sys
import os
import json
from pymongo import MongoClient
from datetime import datetime
from config import Config
from spark_manager import SparkManager
from mongodb_manager import MongoDBManager
from spark_processor import SparkProcessor
from analysis_engine import AnalysisEngine
from eda_analyzer import EDAAnalyzer
import pandas as pd
import numpy as np
import traceback
from spark_manager import SparkManager


def print_executive_summary(report):
    """Print executive summary to console (safe access)"""
    print("\n" + "="*60)
    print(" EXECUTIVE SUMMARY")
    print("="*60)
    try:
        cities = report.get('cities_analyzed', ['delhi', 'bangalore'])
        print(f"\n Cities Analyzed: {', '.join(cities)}")
        print(f"Analysis Date: {report.get('execution_time', 'N/A')}")
        print("\n KEY FINDINGS:")
        for finding in report.get('key_findings', []):
            print(f"   • {finding}")
        if report.get('recommendations'):
            print("\n RECOMMENDATIONS:")
            for r in report['recommendations']:
                print(f"   • {r}")
        summary = report.get('summary', {})
        total_routes = sum(summary.get(c, {}).get('total_routes', 0) for c in cities)
        print(f"\n Total routes analyzed: {total_routes}")
        print(f" Cities with good coverage: {len(cities)}")
        print(f" Recommendations provided: {len(report.get('recommendations', []))}")
    except Exception as e:
        print(f"Error printing executive summary: {e}")
        traceback.print_exc()
    print("\n" + "="*60)

def main():
    """Main execution function"""
    print("Starting Urban Mobility Pipeline")
    print("=" * 60)
    start_time = datetime.now()
    spark = None
    mongo_manager = None

    try:
        # Step 1: Setup Directories and Configuration
        print("\n1. Initial Setup")
        Config.setup_directories()

        # Step 2: EDA Analysis
        print("\n2. Exploratory Data Analysis")
        eda_analyzer = EDAAnalyzer()
        eda_results = []
        for city in ['delhi', 'bangalore']:
            print(f"Performing EDA for {city.upper()}...")
            try:
                city_eda_results = eda_analyzer.analyze_city_data(city)
                eda_results.append(city_eda_results)
                print(f"EDA completed for {city}")
            except Exception as e:
                print(f"EDA failed for {city}: {e}")

        # Step 3: Setup Spark
        print("\n3. Spark Setup")
        spark = SparkManager.setup_spark()
        if spark is None:
            print("Spark setup failed. Please install Spark dependencies (pyspark, findspark).")
            return

        # Step 4: MongoDB Setup and Data Import
        print("\n4. MongoDB Setup and Data Import")
        try:
            mongo_manager = MongoDBManager()
            for city in ['delhi', 'bangalore']:
                print(f"\nProcessing {city.upper()}...")
                try:
                    success = mongo_manager.import_city_data(city)
                    if success:
                        print(f" Successfully imported {city} data to MongoDB")
                    else:
                        print(f" Failed to import {city} data (check logs)")
                except Exception as e:
                    print(f" Error importing {city}: {e}")
            print("\n" + "="*50)
            mongo_manager.get_database_stats()
        except Exception as e:
            print(f" MongoDB operations failed: {e}")
            traceback.print_exc()
            if spark:
                SparkManager.stop_spark(spark)
            return

        # Step 5: Spark Processing
        print("\n5. Spark Data Processing")
        spark_processor = SparkProcessor(spark)
        for city in ['delhi', 'bangalore']:
            print(f"\nProcessing {city.upper()} with Spark...")
            try:
                results = spark_processor.process_city_data(city, mongo_manager)
                if results:
                    spark_processor.save_spark_results(results, city)
                    print(f" Successfully processed {city} data")
                else:
                    print(f" Failed to process {city} data with Spark")
            except Exception as e:
                print(f" Spark processing failed for {city}: {e}")
                traceback.print_exc()

        # Step 6: Ride-Sharing Data Collection
        print("\n6. Ride-Sharing Data Collection")
        try:
            from ride_share_simulator import RideShareSimulator
            ride_simulator = RideShareSimulator()
            for city in ['delhi', 'bangalore']:
                print(f"Generating ride data for {city}...")
                rides = ride_simulator.generate_ride_data(city, num_rides=1000)
                print(f"Generated {len(rides)} ride records for {city}")
                success = ride_simulator.store_ride_data(rides, city)
                if success:
                    ride_simulator.create_ride_indexes(city)
                    print(f"Successfully stored ride data for {city}")
                else:
                    print(f"Failed to store ride data for {city}")
        except Exception as e:
            print(f"Ride-sharing data collection failed: {e}")
            traceback.print_exc()

        # Step 7: Data Analysis and Visualization
        print("\n7. Data Analysis and Visualization")
        analysis_engine = AnalysisEngine()
        all_analysis_results = []
        for city in ['delhi', 'bangalore']:
            print(f"\nAnalyzing {city.upper()}...")
            data = analysis_engine.load_analysis_data(city)
            if data:
                try:
                    analysis_results = analysis_engine.perform_comprehensive_analysis(city)
                    if analysis_results:
                        all_analysis_results.append(analysis_results)
                        analysis_engine.create_comprehensive_visualizations(city, data)
                        analysis_engine.create_advanced_analysis(city, data)
                        print(f"Completed ALL analysis for {city}")
                    else:
                        print(f"Analysis failed for {city}")
                except Exception as e:
                    print(f" Analysis failed for {city}: {e}")
                    traceback.print_exc()
            else:
                print(f"No data available for analysis in {city}")

        # Step 8: Ride-Sharing Data Check
        print("\n8. Ride-Sharing Data Check (no visualizations here)")
        try:
            for city in ['delhi', 'bangalore']:
                collection_name = f"{city}_rides"
                client = MongoClient(Config().MONGO_URI, serverSelectionTimeoutMS=5000)
                db = client[Config().DATABASE_NAME]
                count = db[collection_name].count_documents({})
                client.close()
                print(f"  {city}: {count} ride records available for modeling")
        except Exception as e:
            print(f"Ride-sharing check note: {e}")

        # Step 9: City Comparison
        print("\n9. Generating City Comparison Analysis")
        try:
            from city_comparison_engine import CityComparisonEngine
            comparison_engine = CityComparisonEngine(spark)
            comparison_result = comparison_engine.generate_comparison_analysis()
            if comparison_result:
                print("City comparison analysis completed")
            else:
                print("City comparison completed with limited data")
        except Exception as e:
            print(f"Error in city comparison: {e}")
            traceback.print_exc()

        # Step 10: Predictive Modeling & Machine Learning
        print("\n10. Predictive Modeling & Machine Learning")
        try:
            from feature_engineer import FeatureEngineer
            from model_builder import ModelBuilder
            from model_analyzer import ModelAnalyzer

            feature_engineer = FeatureEngineer(spark)
            model_builder = ModelBuilder(spark)
            model_analyzer = ModelAnalyzer(spark)

            modeling_df = feature_engineer.create_modeling_dataset()
            if modeling_df is None:
                print("DEBUG: modeling_df is None -> attempt simple fallback dataset")
                modeling_df = feature_engineer.create_simple_modeling_dataset()

            if modeling_df is None:
                print("No modeling data available; skipping ML.")
            else:
                print(f"DEBUG: Features created: {modeling_df.count()} records")
                modeling_df.show(5, truncate=False)

                for problem_type in ['regression', 'classification']:
                    print(f"\nTraining {problem_type.upper()} models...")
                    processed_df, feature_model, feature_cols = feature_engineer.prepare_features_for_ml(
                        modeling_df, problem_type
                    )
                    if processed_df and processed_df.count() > 0:
                        train_df, test_df = model_builder.split_data(processed_df)
                        models = model_builder.train_models(train_df, problem_type)
                        if models:
                            results = model_builder.evaluate_models(models, test_df, problem_type)
                            print(f"\n{problem_type.upper()} MODEL RESULTS:")
                            for model_name, metrics in results.items():
                                print(f"  {model_name}: {metrics}")
                            model_builder.save_models(models, problem_type)
                            model_builder.create_model_comparison(results, problem_type)
                            importance_results = model_analyzer.analyze_feature_importance(models, feature_cols, problem_type)
                            insights = model_analyzer.generate_insights(modeling_df, importance_results, problem_type)
                            print(f"{problem_type.upper()} modeling completed successfully!")
                        else:
                            print(f"No models trained for {problem_type}")
                    else:
                        print(f"Could not process features for {problem_type}")
        except Exception as e:
            print(f"Predictive modeling failed: {e}")
            traceback.print_exc()

        # Execution time
        execution_time = datetime.now() - start_time
        print(f"\nTotal execution time: {execution_time}")
        print("\nPipeline completed! Check outputs/ for results and visuals")

    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        traceback.print_exc()

    finally:
        if spark:
            SparkManager.stop_spark(spark)
        if mongo_manager:
            try:
                mongo_manager.close_connection()
            except Exception:
                pass

if __name__ == "__main__":
    main()
