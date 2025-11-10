import os
import pymongo
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import pandas as pd
from config import Config
from data_loader import DataLoader



class MongoDBManager:
    """Manage MongoDB operations including connection, data import, and indexing"""
    
    def __init__(self):
        self.config = Config()
        self.client = None
        self.db = None
        self._connect()
    
    def _connect(self):
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(
                self.config.MONGO_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            # Test connection
            self.client.admin.command('ismaster')
            self.db = self.client[self.config.DATABASE_NAME]
            print("MongoDB connected successfully!")
            print(f" Database: {self.config.DATABASE_NAME}")
            
        except ServerSelectionTimeoutError:
            print(" MongoDB server selection timeout. Please check if MongoDB is running.")
            raise
        except ConnectionFailure:
            print(" MongoDB connection failed. Please check the connection string.")
            raise
        except Exception as e:
            print(f" MongoDB connection error: {e}")
            raise
    
    def import_city_data(self, city):
        """Import all GTFS data for a city to MongoDB"""
        if not DataLoader.validate_gtfs_files(city):
            print(f"GTFS validation failed for {city}")
            return False
        
        print(f"\n Importing {city} data to MongoDB...")
        
        file_types = [f.replace('.txt', '') for f in os.listdir(Config.get_city_data_path(city)) 
                     if f.endswith('.txt')]
        
        total_records = 0
        imported_files = 0
        
        for file_type in file_types:
            df = DataLoader.load_gtfs_file(city, file_type)
            
            if not df.empty:
                # Clean column names
                df = DataLoader.clean_column_names(df)
                
                # Validate structure
                if not DataLoader.validate_gtfs_structure(df, file_type):
                    print(f"Skipping {file_type} due to structural issues")
                    continue
                
                collection_name = f"{city}_{file_type}"
                success = self._import_dataframe_to_collection(df, collection_name)
                
                if success:
                    total_records += len(df)
                    imported_files += 1
                    self.create_indexes(collection_name, file_type)
        
        print(f"Imported {imported_files} files with {total_records} total records for {city}")
        return imported_files > 0
    
    def _import_dataframe_to_collection(self, df, collection_name):
        """Import a DataFrame to MongoDB collection"""
        try:
            collection = self.db[collection_name]
            
            # Clear existing data
            collection.delete_many({})
            
            # Convert DataFrame to records and insert
            records = df.to_dict('records')
            if records:
                result = collection.insert_many(records)
                print(f" {collection_name}: {len(result.inserted_ids)} records")
                return True
            else:
                print(f" No records to insert for {collection_name}")
                return False
                
        except Exception as e:
            print(f" Error importing to {collection_name}: {e}")
            return False
    
    def create_indexes(self, collection_name, file_type):
        """Create appropriate indexes for each collection type"""
        try:
            collection = self.db[collection_name]
            
            index_configs = {
                'routes': [
                    [('route_id', ASCENDING)],
                    [('route_short_name', ASCENDING)]
                ],
                'trips': [
                    [('trip_id', ASCENDING)],
                    [('route_id', ASCENDING)],
                    [('service_id', ASCENDING)]
                ],
                'stops': [
                    [('stop_id', ASCENDING)],
                    [('stop_lat', ASCENDING), ('stop_lon', ASCENDING)]
                ],
                'stop_times': [
                    [('trip_id', ASCENDING)],
                    [('stop_id', ASCENDING)],
                    [('trip_id', ASCENDING), ('stop_sequence', ASCENDING)],
                    [('arrival_time', ASCENDING)]
                ],
                'agency': [
                    [('agency_id', ASCENDING)]
                ]
            }
            
            if file_type in index_configs:
                for index_fields in index_configs[file_type]:
                    collection.create_index(index_fields)
                print(f" Created indexes for {collection_name}")
                
        except Exception as e:
            print(f"Error creating indexes for {collection_name}: {e}")
    
    def get_database_stats(self):
        """Get comprehensive statistics about the database"""
        try:
            stats = {
                'collections': {},
                'total_documents': 0,
                'database_size': 0
            }
            
            collections = self.db.list_collection_names()
            
            print("\nDatabase Statistics:")
            print("=" * 40)
            
            for coll_name in collections:
                count = self.db[coll_name].count_documents({})
                stats['collections'][coll_name] = count
                stats['total_documents'] += count
                
                # Get collection stats
                coll_stats = self.db.command('collstats', coll_name)
                stats['database_size'] += coll_stats.get('size', 0)
                
                print(f"   {coll_name}: {count} documents")
            
            stats['database_size_mb'] = round(stats['database_size'] / (1024 * 1024), 2)
            print(f"   Total documents: {stats['total_documents']}")
            print(f"   Database size: {stats['database_size_mb']} MB")
            
            return stats
            
        except Exception as e:
            print(f"Error getting database stats: {e}")
            return {}
    
    def get_collection_sample(self, collection_name, limit=5):
        """Get sample documents from a collection"""
        try:
            collection = self.db[collection_name]
            documents = list(collection.find().limit(limit))
            return documents
        except Exception as e:
            print(f"Error getting sample from {collection_name}: {e}")
            return []
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            print(" MongoDB connection closed")