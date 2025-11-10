import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import time
from config import Config
from pymongo import MongoClient
import json
from math import radians, sin, cos, sqrt, atan2


class RideShareSimulator:
    """Simulate realistic ride-sharing data with surge pricing"""
    
    def __init__(self):
        self.config = Config()
        self.route_cache = {}
        
        # Base pricing models for Indian ride-sharing (approximate)
       # Update the pricing_models in __init__ method:

        self.pricing_models = {
            'uber': {
                'mini': {'base': 45, 'per_km': 12, 'per_min': 1, 'booking_fee': 0},
                'prime': {'base': 70, 'per_km': 16, 'per_min': 1.5, 'booking_fee': 0},
                'auto': {'base': 35, 'per_km': 8, 'per_min': 0.5, 'booking_fee': 0},
                'moto': {'base': 25, 'per_km': 6, 'per_min': 0.3, 'booking_fee': 0}
            },
            'ola': {
                'mini': {'base': 40, 'per_km': 11, 'per_min': 0.9, 'booking_fee': 5},
                'prime': {'base': 65, 'per_km': 15, 'per_min': 1.3, 'booking_fee': 5},
                'auto': {'base': 30, 'per_km': 7, 'per_min': 0.4, 'booking_fee': 0},
                'bike': {'base': 20, 'per_km': 5, 'per_min': 0.2, 'booking_fee': 0}
            },
            'rapido': {
                'bike': {'base': 20, 'per_km': 4, 'per_min': 0.2, 'booking_fee': 0}
            }
        }
        
    def get_route_info(self, start_lat, start_lon, end_lat, end_lon):
        """Get distance and duration with caching"""
        # Create a cache key
        cache_key = f"{start_lat:.4f},{start_lon:.4f},{end_lat:.4f},{end_lon:.4f}"
        
        # Check cache first
        if cache_key in self.route_cache:
            return self.route_cache[cache_key]
        
        try:
            # Calculate straight-line distance (much faster than API calls)
            distance_km = self._calculate_straight_distance(start_lat, start_lon, end_lat, end_lon)
            
            # Skip if distance is unreasonable
            if distance_km > 50:  # Max 50km for urban rides
                distance_km = np.random.uniform(2, 15)
            
            # Estimate duration based on urban traffic patterns
            base_duration = (distance_km / 25) * 60  # 25 km/h average
            traffic_factor = np.random.uniform(1.2, 2.0)  # Traffic multiplier
            duration_mins = base_duration * traffic_factor
            
            result = (round(distance_km, 2), round(max(5, duration_mins)))  # At least 5 minutes
            
            # Cache the result
            self.route_cache[cache_key] = result
            return result
            
        except Exception as e:
            print(f"Route calculation error: {e}")
            # Reasonable fallback
            fallback = (round(np.random.uniform(3, 12), 2), round(np.random.uniform(10, 45)))
            self.route_cache[cache_key] = fallback
            return fallback
            
    
    def _calculate_straight_distance(self, lat1, lon1, lat2, lon2):
        """Calculate straight-line distance between two points"""
        from math import radians, sin, cos, sqrt, atan2
        
        R = 6371  # Earth radius in km
        
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        
        return R * c
    
    def calculate_surge_multiplier(self, timestamp, start_lat, start_lon, city):
        """Predict surge pricing based on time, location, and demand factors"""
        hour = timestamp.hour
        day_of_week = timestamp.weekday()
        month = timestamp.month
        day = timestamp.day
        
        # Base surge factors
        surge = 1.0
        
        # **ACCURATE PEAK HOURS** (Based on Indian traffic patterns)
        # Morning peak: 8 AM - 11 AM (office/school hours)
        # Evening peak: 4 PM - 9 PM (return home + evening outings)
        if (hour >= 8 and hour <= 11) or (hour >= 16 and hour <= 21):
            surge += 0.4  # Higher surge during actual peak hours
        
        # **LUNCH TIME SURGE** (1 PM - 3 PM)
        elif hour >= 13 and hour <= 15:
            surge += 0.2
        
        # **LATE NIGHT SURGE** (10 PM - 2 AM)
        elif hour >= 22 or hour <= 2:
            surge += 0.5  # Higher for safety/availability
        
        # **WEEKEND SURGE** (Friday evening to Sunday night)
        if day_of_week >= 5:  # Saturday (6) and Sunday (7)
            surge += 0.3
        elif day_of_week == 4 and hour >= 16:  # Friday evening
            surge += 0.2
        
        # **FESTIVAL AND HOLIDAY SURGE** (Major Indian festivals)
        festival_surge = self._get_festival_surge(timestamp, city)
        surge += festival_surge
        
        # Location-based surge (business districts, airports, etc.)
        location_surge = self._get_location_surge(start_lat, start_lon, city)
        surge += location_surge
        
        # Weather factor (rainy season surge)
        weather_surge = self._get_weather_surge(month, city)
        surge += weather_surge
        
        # Add some randomness
        surge += np.random.uniform(-0.1, 0.1)
        
        # Cap surge between 1.0 and 5.0 (realistic for India - can go up to 5x during festivals)
        return max(1.0, min(5.0, surge))

    def _get_festival_surge(self, timestamp, city):
        """Calculate festival-based surge pricing for Indian festivals"""
        month = timestamp.month
        day = timestamp.day
        surge = 0.0
        
        # **MAJOR INDIAN FESTIVALS** (with high travel demand)
        
        # January
        if month == 1 and day in [1, 14, 26]:  # New Year, Makar Sankranti, Republic Day
            surge += 0.8
        
        # February-March (Holi season)
        elif month == 3 and day in [10, 11, 12, 13]:  # Holi dates (example)
            surge += 1.2  # Very high surge during Holi
        
        # August-September (Rakhi, Janmashtami, Ganesh Chaturthi)
        elif month == 8 and day in [15, 19, 30]:  # Independence Day, Rakhi, Janmashtami
            surge += 0.7
        elif month == 9 and day in [7, 8, 9, 17]:  # Ganesh Chaturthi
            surge += 0.9
        
        # October (Durga Puja/Dussehra season)
        elif month == 10 and day in [12, 13, 14, 15, 23, 24, 31]:  # Durga Puja, Dussehra, Diwali prep
            surge += 1.0
        
        # November (Diwali season - HIGHEST SURGE)
        elif month == 11 and day in [1, 2, 3, 4, 12, 13, 14]:  # Diwali week
            surge += 1.5  # Maximum surge during Diwali
        
        # December (Christmas/New Year eve)
        elif month == 12 and day in [24, 25, 31]:  # Christmas Eve, Christmas, New Year Eve
            surge += 1.0
        
        # **CITY-SPECIFIC FESTIVALS**
        if city == 'delhi':
            # Delhi specific festivals
            if month == 10:  # Durga Puja in Delhi
                surge += 0.3
        elif city == 'bangalore':
            # Bangalore specific festivals
            if month == 8:  # Ganesh Chaturthi in Bangalore
                surge += 0.4
            if month == 4:  # Ugadi
                surge += 0.3
        
        return surge

    def _get_weather_surge(self, month, city):
        """Account for monsoon/weather related surge"""
        surge = 0.0
        
        # Monsoon season (June-September) - higher demand due to rain
        if month >= 6 and month <= 9:
            surge += 0.3
        
        # Extreme weather days (random occurrence)
        if np.random.random() < 0.1:  # 10% chance of bad weather day
            surge += 0.4
        
        return surge

    def _get_location_surge(self, lat, lon, city):
        """Calculate location-based surge multiplier for Indian cities"""
        surge = 0.0
        
        # **DELHI HIGH SURGE AREAS**
        if city == 'delhi':
            high_surge_areas = [
                {'lat': 28.6139, 'lon': 77.2090, 'radius': 3, 'multiplier': 0.3},  # Central Delhi
                {'lat': 28.5562, 'lon': 77.1000, 'radius': 2, 'multiplier': 0.4},  # Connaught Place
                {'lat': 28.5355, 'lon': 77.3910, 'radius': 3, 'multiplier': 0.3},  # Noida
                {'lat': 28.4595, 'lon': 77.0266, 'radius': 3, 'multiplier': 0.3},  # Gurgaon
                {'lat': 28.7041, 'lon': 77.1025, 'radius': 2, 'multiplier': 0.2},  # North Campus
            ]
        
        # **BANGALORE HIGH SURGE AREAS**
        elif city == 'bangalore':
            high_surge_areas = [
                {'lat': 12.9716, 'lon': 77.5946, 'radius': 3, 'multiplier': 0.3},  # MG Road
                {'lat': 12.9784, 'lon': 77.6408, 'radius': 2, 'multiplier': 0.4},  # Indiranagar
                {'lat': 12.9279, 'lon': 77.6271, 'radius': 2, 'multiplier': 0.3},  # Koramangala
                {'lat': 13.0359, 'lon': 77.5970, 'radius': 3, 'multiplier': 0.3},  # Hebbal
                {'lat': 12.9538, 'lon': 77.4907, 'radius': 2, 'multiplier': 0.2},  # Malleshwaram
            ]
        
        for area in high_surge_areas:
            distance = self._calculate_straight_distance(lat, lon, area['lat'], area['lon'])
            if distance <= area['radius']:
                # Surge decreases with distance from center
                distance_factor = 1 - (distance / area['radius'])
                surge += area['multiplier'] * distance_factor
        
        return surge

    def calculate_fare(self, service, vehicle_type, distance_km, duration_mins, surge_multiplier):
        """Calculate fare based on pricing model"""
        if service in self.pricing_models and vehicle_type in self.pricing_models[service]:
            model = self.pricing_models[service][vehicle_type]
            fare = (model['base'] + 
                   model['per_km'] * distance_km + 
                   model['per_min'] * duration_mins + 
                   model['booking_fee'])
            
            # Apply surge
            fare *= surge_multiplier
            
            # Round to nearest 10
            return round(fare / 10) * 10
        else:
            return 0
    
    def generate_ride_data(self, city, num_rides=1000, days_back=30):
        """Generate realistic ride data for a city"""
        rides = []
        
        # City center coordinates
        city_centers = {
            'delhi': {'lat': 28.6139, 'lon': 77.2090},
            'bangalore': {'lat': 12.9716, 'lon': 77.5946}
        }
        
        center = city_centers.get(city, {'lat': 28.6139, 'lon': 77.2090})
        
        for i in range(num_rides):
            # Random timestamp within last 30 days
            days_ago = np.random.randint(0, days_back)
            hours_ago = np.random.randint(0, 24)
            minutes_ago = np.random.randint(0, 60)
            
            timestamp = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
            
            # Generate random start and end points around city center
            start_lat = center['lat'] + np.random.uniform(-0.2, 0.2)
            start_lon = center['lon'] + np.random.uniform(-0.2, 0.2)
            end_lat = center['lat'] + np.random.uniform(-0.3, 0.3)
            end_lon = center['lon'] + np.random.uniform(-0.3, 0.3)
            
            # Get route info
            distance_km, duration_mins = self.get_route_info(start_lat, start_lon, end_lat, end_lon)
            
            # Skip if distance is too small
            if distance_km < 1:
                continue
            
            # Random service and vehicle type
            service = np.random.choice(['uber', 'ola', 'rapido'])
            if service == 'rapido':
                vehicle_type = 'bike'
            else:
                vehicle_type = np.random.choice(['mini', 'prime', 'auto'])
            
            # Calculate surge
            surge_multiplier = self.calculate_surge_multiplier(timestamp, start_lat, start_lon, city)
            
            # Calculate fare
            fare = self.calculate_fare(service, vehicle_type, distance_km, duration_mins, surge_multiplier)
            
            ride = {
                'ride_id': f"{city}_ride_{i}",
                'city': city,
                'timestamp': timestamp,
                'service': service,
                'vehicle_type': vehicle_type,
                'start_lat': start_lat,
                'start_lon': start_lon,
                'end_lat': end_lat,
                'end_lon': end_lon,
                'distance_km': round(distance_km, 2),
                'duration_mins': round(duration_mins, 2),
                'surge_multiplier': round(surge_multiplier, 2),
                'fare': fare,
                'demand_features': {
                    'hour_of_day': timestamp.hour,
                    'day_of_week': timestamp.weekday(),
                    'is_peak_hour': 1 if (timestamp.hour >= 7 and timestamp.hour <= 10) or (timestamp.hour >= 17 and timestamp.hour <= 20) else 0,
                    'is_weekend': 1 if timestamp.weekday() >= 5 else 0
                }
            }
            
            rides.append(ride)
        
        return rides
    
    def store_ride_data(self, rides, city):
        """Store ride data in MongoDB"""
        try:
            client = MongoClient(self.config.MONGO_URI)
            db = client[self.config.DATABASE_NAME]
            collection = db[f"{city}_rides"]
            
            # Clear existing data
            collection.delete_many({})
            
            # Insert new data
            if rides:
                collection.insert_many(rides)
                print(f"Stored {len(rides)} ride records for {city}")
            
            client.close()
            return True
            
        except Exception as e:
            print(f"Error storing ride data: {e}")
            return False

    def create_ride_indexes(self, city):
        """Create indexes for ride data"""
        try:
            client = MongoClient(self.config.MONGO_URI)
            db = client[self.config.DATABASE_NAME]
            collection = db[f"{city}_rides"]
            
            indexes = [
                [('timestamp', 1)],
                [('service', 1)],
                [('vehicle_type', 1)],
                [('surge_multiplier', 1)],
                [('fare', 1)]
            ]
            
            for index_fields in indexes:
                collection.create_index(index_fields)
            
            client.close()
            print(f"Created indexes for {city}_rides")
            
        except Exception as e:
            print(f"Error creating indexes: {e}")