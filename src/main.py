"""
Main pipeline orchestration for Urban Mobility Intelligence System
"""

import sys
import os
import json
from datetime import datetime
from config import Config
from spark_manager import SparkManager
from mongodb_manager import MongoDBManager
from spark_processor import SparkProcessor
from analysis_engine import AnalysisEngine
from eda_analyzer import EDAAnalyzer  # Add this import

def generate_summary_report(analysis_results):
    """Generate a comprehensive summary report"""
    report = {
        "project": "Urban Mobility Intelligence System",
        "execution_time": datetime.now().isoformat(),
        "cities_analyzed": [],
        "summary": {},
        "key_findings": [],
        "recommendations": []
    }
    
    for result in analysis_results:
        city = result['city']
        report["cities_analyzed"].append(city)
        
        if 'summary' in result:
            report["summary"][city] = result['summary']
            
            # Add key findings
            findings = []
            
            if 'route_analysis' in result:
                ra = result['route_analysis']
                findings.extend([
                    f"{city.title()} has {ra['total_routes']} bus routes",
                    f"Average {ra['avg_trips_per_route']:.1f} trips per route",
                    f"Total {ra['total_stop_events']:,} stop events recorded",
                    f"Busiest route: {ra['busiest_route']['route_id']} with {ra['busiest_route']['trips']} trips"
                ])
            
            if 'headway_analysis' in result:
                ha = result['headway_analysis']
                findings.extend([
                    f"Average headway: {ha['avg_headway_min']:.1f} minutes",
                    f"On-time performance: {ha['reliability_metrics']['on_time_percentage']:.1f}%",
                    f"High-frequency coverage: {ha['reliability_metrics']['high_frequency_percentage']:.1f}%"
                ])
            
            if 'service_analysis' in result:
                sa = result['service_analysis']
                findings.extend([
                    f"Peak service hour: {sa['peak_hour']}:00",
                    f"Service gaps: {len(sa['service_gaps'])} hours in 24-hour cycle"
                ])
            
            report["key_findings"].extend(findings)
            
            # Generate recommendations
            recommendations = generate_recommendations(result)
            report["recommendations"].extend(recommendations)
    
    # Save report
    with open('outputs/data/comprehensive_analysis_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print("Comprehensive report generated: outputs/data/comprehensive_analysis_report.json")
    return report

def generate_recommendations(analysis_result):
    """Generate recommendations based on analysis findings"""
    recommendations = []
    city = analysis_result['city']
    
    if 'headway_analysis' in analysis_result:
        ha = analysis_result['headway_analysis']
        if ha['avg_headway_min'] > 15:
            recommendations.append(
                f"Consider increasing frequency on key routes in {city} to reduce average headway"
            )
    
    if 'service_analysis' in analysis_result:
        sa = analysis_result['service_analysis']
        if len(sa['service_gaps']) > 6:
            recommendations.append(
                f"Address service gaps in {city}, particularly during {sa['service_gaps']}"
            )
    
    if 'route_analysis' in analysis_result:
        ra = analysis_result['route_analysis']
        if ra['route_distribution']['low_frequency'] > ra['route_distribution']['high_frequency']:
            recommendations.append(
                f"Optimize route frequencies in {city} - too many low-frequency routes"
            )
    
    return recommendations

def print_executive_summary(report):
    """Print executive summary to console"""
    print("\n" + "="*60)
    print(" EXECUTIVE SUMMARY")
    print("="*60)
    
    print(f"\n Cities Analyzed: {', '.join(report['cities_analyzed'])}")
    print(f"Analysis Date: {report['execution_time']}")
    
    print(f"\n KEY FINDINGS:")
    for finding in report['key_findings']:
        print(f"   • {finding}")
    
    if report['recommendations']:
        print(f"\n RECOMMENDATIONS:")
        for recommendation in report['recommendations']:
            print(f"   • {recommendation}")
    
    print(f"\n Overall Assessment:")
    total_routes = sum(len(report['summary'][city].get('total_routes', 0)) 
                      for city in report['cities_analyzed'] 
                      if 'total_routes' in report['summary'][city])
    print(f" Total routes analyzed: {total_routes}")
    print(f" Cities with good coverage: {len(report['cities_analyzed'])}")
    print(f" Recommendations provided: {len(report['recommendations'])}")

def main():
    """Main execution function"""
    print("Starting Urban Mobility Pipeline")
    print("=" * 60)
    
    # Track execution time
    start_time = datetime.now()
    
    try:
        # Step 1: Setup Directories and Configuration
        print("\n1. Initial Setup")
        Config.setup_directories()
        
        # Step 2: EDA Analysis (NEW STEP)
        print("\n2.Exploratory Data Analysis")
        eda_analyzer = EDAAnalyzer()
        eda_results = []
        
        for city in ['delhi', 'bangalore']:
            print(f"\ Performing EDA for {city.upper()}...")
            city_eda_results = eda_analyzer.analyze_city_data(city)
            eda_results.append(city_eda_results)
            print(f" EDA completed for {city}")
        
        # Step 3: Setup Spark
        print("\n3. Spark Setup")
        spark = SparkManager.setup_spark()
        
        if spark is None:
            print(" Spark setup failed. Please install Spark dependencies.")
            print("Run: pip install pyspark findspark")
            return
        
        # Step 4: MongoDB Setup and Data Import
        print("\n4. MongoDB Setup and Data Import")
        try:
            mongo_manager = MongoDBManager()
            
            # Import data for each city
            for city in ['delhi', 'bangalore']:
                print(f"\nProcessing {city.upper()}...")
                success = mongo_manager.import_city_data(city)
                if success:
                    print(f" Successfully imported {city} data to MongoDB")
                else:
                    print(f"Failed to import {city} data")
            
            # Show database statistics
            print("\n" + "="*50)
            mongo_manager.get_database_stats()
            
        except Exception as e:
            print(f" MongoDB operations failed: {e}")
            SparkManager.stop_spark(spark)
            return
        
        # Step 5: Spark Processing
        print("\n5. Spark Data Processing")
        spark_processor = SparkProcessor(spark)
        
        for city in ['delhi', 'bangalore']:
            print(f"\nProcessing {city.upper()} with Spark...")
            results = spark_processor.process_city_data(city, mongo_manager)
            
            if results:
                spark_processor.save_spark_results(results, city)
                print(f" Successfully processed {city} data")
            else:
                print(f" Failed to process {city} data with Spark")
        
            # Step 6: Data Analysis and Visualization
        # Step 5.5: Ride-Sharing Data Collection
        print("\n5.5 Ride-Sharing Data Collection")
        try:
            from ride_share_simulator import RideShareSimulator
            
            ride_simulator = RideShareSimulator()
            
            for city in ['delhi', 'bangalore']:
                print(f"Generating ride data for {city}...")
                
                # Generate ride data
                rides = ride_simulator.generate_ride_data(city, num_rides=1000)
                print(f"Generated {len(rides)} ride records for {city}")
                
                # Store in MongoDB
                success = ride_simulator.store_ride_data(rides, city)
                if success:
                    ride_simulator.create_ride_indexes(city)
                    print(f"Successfully stored ride data for {city}")
                else:
                    print(f"Failed to store ride data for {city}")
                    
        except Exception as e:
            print(f"Ride-sharing data collection failed: {e}")
            import traceback
            traceback.print_exc()


        print("\n6. Data Analysis and Visualization")
        analysis_engine = AnalysisEngine()
        all_analysis_results = []

        for city in ['delhi', 'bangalore']:
            print(f"\nAnalyzing {city.upper()}...")
            
            # Load data once
            data = analysis_engine.load_analysis_data(city)
            
            if data:
                # Perform basic analysis
                analysis_results = analysis_engine.perform_comprehensive_analysis(city)
                
                if analysis_results:
                    all_analysis_results.append(analysis_results)
                    
                    # Create ALL visualizations
                    analysis_engine.create_comprehensive_visualizations(city, data)  # Basic graphs
                    analysis_engine.create_advanced_analysis(city, data)             # Advanced graphs
                    print(f"Completed ALL analysis for {city}")
                else:
                    print(f"Analysis failed for {city}")
            else:
                print(f"No data available for analysis in {city}")

        # Step 7: Ride-Sharing Data Collection and Analysis
        print("\n7. Ride-Sharing Data Collection and Analysis")
        try:
            # Import ride-sharing modules
            from ride_share_simulator import RideShareSimulator
            from ride_share_analyzer import RideShareAnalyzer
            
            ride_simulator = RideShareSimulator()
            ride_analyzer = RideShareAnalyzer(spark)
            
            for city in ['delhi', 'bangalore']:
                print(f"\nProcessing ride-sharing data for {city}...")
                
                # Generate ride data
                print("Generating ride data...")
                rides = ride_simulator.generate_ride_data(city, num_rides=2000)
                
                # Store in MongoDB
                print("Storing in MongoDB...")
                ride_simulator.store_ride_data(rides, city)
                ride_simulator.create_ride_indexes(city)
                print(f"Stored {len(rides)} ride records for {city}")
                
                # Load and preprocess ride data
                print("Loading and preprocessing data...")
                rides_df = ride_analyzer.load_ride_data(city)
                if rides_df:
                    rides_clean = ride_analyzer.preprocess_ride_data(rides_df)
                    
                    # Analyze ride patterns
                    print("Analyzing ride patterns...")
                    ride_analysis = ride_analyzer.analyze_ride_patterns(rides_clean)
                    surge_analysis = ride_analyzer.analyze_surge_patterns(rides_clean)
                    
                    # Create visualizations
                    print("Creating visualizations...")
                    ride_analyzer.create_ride_visualizations(ride_analysis, city)
                    ride_analyzer.create_surge_visualizations(surge_analysis, city)
                    ride_analyzer.create_comparison_visualizations(rides_clean, city)
                    
                    print(f"Completed ride-sharing analysis for {city}")
                else:
                    print(f"No ride data available for {city}")
                    
        except Exception as e:
            print(f"Ride-sharing analysis failed: {e}")
            import traceback
            traceback.print_exc()

        # Step 8: Generate City Comparison
        print("\n8. Generating City Comparison Analysis")
        try:
            # Load data for both cities
            delhi_data = analysis_engine.load_analysis_data('delhi')
            bangalore_data = analysis_engine.load_analysis_data('bangalore')
            
            if delhi_data and bangalore_data:
                analysis_engine._plot_city_comparison(delhi_data, bangalore_data)
                print("City comparison analysis completed")
            else:
                print("Cannot generate city comparison - missing data")
        except Exception as e:
            print(f"Error in city comparison: {e}")

        # Step 9: Generate Summary Report
        print("\n9. Generating Summary Report")
        if all_analysis_results:
            report = generate_summary_report(all_analysis_results)
            print_executive_summary(report)
        else:
            print("No analysis results to report")
        
        # Calculate execution time
        execution_time = datetime.now() - start_time
        print(f"\n Total execution time: {execution_time}")
        
        print("\n Pipeline completed successfully!")
        print(" Check the 'outputs/' folder for results and visualizations")
        
    except Exception as e:
        print(f" Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up resources
        if 'spark' in locals():
            SparkManager.stop_spark(spark)
        if 'mongo_manager' in locals():
            mongo_manager.close_connection()

if __name__ == "__main__":
    main()