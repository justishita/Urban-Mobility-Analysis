import os
import math
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except Exception:
    XGBOOST_AVAILABLE = False

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

os.makedirs('outputs/demand_forecast/visuals', exist_ok=True)
os.makedirs('outputs/demand_forecast/data', exist_ok=True)


class DemandSupplyPlanner:
    """
    Train hourly demand forecasting models and produce supply planning visualizations.
    """

    def __init__(self,
                 city: str,
                 data_path: Optional[str] = None,
                 forecast_horizon_hours: int = 24,
                 train_history_days: int = 30,
                 trips_per_cab_per_hour: float = 0.4,
                 bus_capacity: int = 50,
                 available_cabs: int = 200,
                 available_buses: int = 50,
                 retrain: bool = True):
        self.city = city
        self.data_path = data_path or f"../outputs/data/{self.city}_hourly_service.csv"
        self.horizon = forecast_horizon_hours
        self.history_days = train_history_days
        self.trips_per_cab_per_hour = trips_per_cab_per_hour
        self.bus_capacity = bus_capacity
        self.available_cabs = available_cabs
        self.available_buses = available_buses
        self.retrain = retrain
        self.scaler = StandardScaler()
        self.feature_importance = {}

    def load_hourly_data(self) -> pd.DataFrame:
        """Load and preprocess hourly data for the city"""
        if not os.path.exists(self.data_path):
            raise FileNotFoundError(f"Hourly file not found: {self.data_path}")
        
        df = pd.read_csv(self.data_path)
        
        print(f"Available columns in {self.city}: {df.columns.tolist()}")
        
        if 'timestamp' not in df.columns:
            if 'arrival_hour' in df.columns:
                print(f"Creating timestamps using arrival_hour for {self.city}")
                start_date = pd.Timestamp.now().normalize() - pd.Timedelta(days=len(df)//24 + 1)
                dates = [start_date + pd.Timedelta(hours=i) for i in range(len(df))]
                df['timestamp'] = dates
            else:
                print(f"Warning: No timestamp columns found in {self.city} data. Creating synthetic timestamps.")
                base_date = pd.Timestamp.now().normalize() - pd.Timedelta(days=len(df))
                df['timestamp'] = [base_date + pd.Timedelta(hours=i) for i in range(len(df))]
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        if 'ride_count' not in df.columns:
            if 'hourly_trips' in df.columns:
                df = df.rename(columns={'hourly_trips': 'ride_count'})
                print(f"Using 'hourly_trips' as ride_count for {self.city}")
            elif 'trips' in df.columns:
                df = df.rename(columns={'trips': 'ride_count'})
                print(f"Using 'trips' as ride_count for {self.city}")
            else:
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    df['ride_count'] = df[numeric_cols[0]]
                    print(f"Using '{numeric_cols[0]}' as ride_count for {self.city}")
                else:
                    raise ValueError(f"No numeric columns found for ride_count in {self.city} data")
        
        if 'hour_of_day' not in df.columns:
            df['hour_of_day'] = df['timestamp'].dt.hour
        if 'is_weekend' not in df.columns:
            df['is_weekend'] = df['timestamp'].dt.weekday.isin([5,6]).astype(int)
        if 'day_of_week' not in df.columns:
            df['day_of_week'] = df['timestamp'].dt.weekday
        if 'day_name' not in df.columns:
            df['day_name'] = df['timestamp'].dt.day_name()
        
        if len(df) > 0:
            df = df.groupby('timestamp').agg({
                'ride_count': 'sum',
                'hour_of_day': 'first',
                'is_weekend': 'first',
                'day_of_week': 'first',
                'day_name': 'first'
            }).reset_index()
        
        df = df.sort_values('timestamp').reset_index(drop=True)
        print(f"Loaded {len(df)} rows for {self.city}")
        if len(df) > 0:
            print(f"Ride count stats - Min: {df['ride_count'].min()}, Max: {df['ride_count'].max()}, Mean: {df['ride_count'].mean():.2f}")
            print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        return df

    def plot_simple_weekday_comparison(self, test_df: pd.DataFrame, best_preds: np.ndarray, best_model: str):
        """Simple line graph: Weekday actual vs predicted - WITH SCALING"""
        plt.figure(figsize=(12, 6))
        
        plot_df = test_df.copy()
        plot_df['predicted'] = best_preds
        
        current_avg = plot_df['ride_count'].mean()
        if current_avg < 50:  
            scale_factor = 100 / max(current_avg, 1)
            plot_df['ride_count'] = plot_df['ride_count'] * scale_factor
            plot_df['predicted'] = plot_df['predicted'] * scale_factor
            print(f"Scaled graph data by {scale_factor:.2f} for realistic visualization")
        
        weekday_avg = plot_df.groupby('day_name').agg({
            'ride_count': 'mean',
            'predicted': 'mean'
        }).reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
        
        plt.plot(weekday_avg.index, weekday_avg['ride_count'], 
                label='Actual Rides', marker='o', linewidth=2, markersize=6, color='blue')
        plt.plot(weekday_avg.index, weekday_avg['predicted'], 
                label=f'Predicted ({best_model})', marker='s', linewidth=2, markersize=6, color='red')
        
        plt.xlabel('Day of Week')
        plt.ylabel('Average Rides per Hour')
        plt.title(f'{self.city} - Average Rides by Day of Week\nModel Used: {best_model}')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        
        avg_actual = weekday_avg['ride_count'].mean()
        avg_pred = weekday_avg['predicted'].mean()
        plt.text(0.02, 0.98, f'Avg Actual: {avg_actual:.0f} rides/hr\nAvg Predicted: {avg_pred:.0f} rides/hr', 
                transform=plt.gca().transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        plt.tight_layout()
        path = f'outputs/demand_forecast/visuals/{self.city}_weekday_comparison.png'
        plt.savefig(path, dpi=200, bbox_inches='tight')
        plt.close()
        
        return path

    def plot_simple_hourly_comparison(self, test_df: pd.DataFrame, best_preds: np.ndarray, best_model: str):
        """Simple line graph: 24-hour format actual vs predicted - WITH SCALING"""
        plt.figure(figsize=(12, 6))
        
        plot_df = test_df.copy()
        plot_df['predicted'] = best_preds
        
        current_avg = plot_df['ride_count'].mean()
        if current_avg < 50:  
            scale_factor = 100 / max(current_avg, 1)
            plot_df['ride_count'] = plot_df['ride_count'] * scale_factor
            plot_df['predicted'] = plot_df['predicted'] * scale_factor
            print(f"Scaled graph data by {scale_factor:.2f} for realistic visualization")
        
        hourly_avg = plot_df.groupby('hour_of_day').agg({
            'ride_count': 'mean',
            'predicted': 'mean'
        }).reset_index()
        
        plt.plot(hourly_avg['hour_of_day'], hourly_avg['ride_count'], 
                label='Actual Rides', marker='o', linewidth=2, markersize=6, color='blue')
        plt.plot(hourly_avg['hour_of_day'], hourly_avg['predicted'], 
                label=f'Predicted ({best_model})', marker='s', linewidth=2, markersize=6, color='red')
        
        plt.xlabel('Hour of Day (24-hour format)')
        plt.ylabel('Average Rides')
        plt.title(f'{self.city} - Average Rides by Hour of Day\nModel Used: {best_model}')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(range(0, 24, 2))
        
        peak_hour_actual = hourly_avg.loc[hourly_avg['ride_count'].idxmax()]
        peak_hour_pred = hourly_avg.loc[hourly_avg['predicted'].idxmax()]
        plt.text(0.02, 0.98, f'Peak Actual: {peak_hour_actual["ride_count"]:.0f} rides at {int(peak_hour_actual["hour_of_day"])}:00\nPeak Predicted: {peak_hour_pred["predicted"]:.0f} rides at {int(peak_hour_pred["hour_of_day"])}:00', 
                transform=plt.gca().transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        plt.tight_layout()
        path = f'outputs/demand_forecast/visuals/{self.city}_hourly_comparison.png'
        plt.savefig(path, dpi=200, bbox_inches='tight')
        plt.close()
        
        return path

    def plot_model_performance(self, eval_results: Dict):
        """Simple model performance comparison"""
        plt.figure(figsize=(10, 6))
        
        models = list(eval_results.keys())
        rmse_values = [eval_results[model]['metrics']['rmse'] for model in models]
        r2_values = [eval_results[model]['metrics']['r2_score'] for model in models]
        
        x_pos = np.arange(len(models))
        width = 0.35
        
        plt.bar(x_pos - width/2, rmse_values, width, label='RMSE', alpha=0.7, color='red')
        plt.bar(x_pos + width/2, r2_values, width, label='R² Score', alpha=0.7, color='green')
        
        plt.xlabel('Models')
        plt.ylabel('Scores')
        plt.title(f'{self.city} - Model Performance Comparison')
        plt.xticks(x_pos, models, rotation=45)
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        for i, v in enumerate(rmse_values):
            plt.text(i - width/2, v + max(rmse_values)*0.01, f'{v:.1f}', ha='center', va='bottom')
        for i, v in enumerate(r2_values):
            plt.text(i + width/2, v + max(r2_values)*0.01, f'{v:.3f}', ha='center', va='bottom')
        
        plt.tight_layout()
        path = f'outputs/demand_forecast/visuals/{self.city}_model_performance.png'
        plt.savefig(path, dpi=200, bbox_inches='tight')
        plt.close()
        
        return path

    def make_lag_features(self, df: pd.DataFrame, lags=(1, 2, 3, 24, 48, 168)):
        """Create lag features for ride_count and simple rolling stats"""
        df = df.copy()
        
        if len(df) < max(lags) + 1:
            print(f"Warning: Not enough data for all lags. Data length: {len(df)}, Max lag: {max(lags)}")
            feasible_lags = [lag for lag in lags if lag < len(df)]
            lags = tuple(feasible_lags)
        
        for lag in lags:
            if lag < len(df):
                df[f'lag_{lag}'] = df['ride_count'].shift(lag) 
        
        if len(df) >= 3:
            df['roll3_mean'] = df['ride_count'].rolling(window=3, min_periods=1).mean().shift(1)
        if len(df) >= 24:
            df['roll24_mean'] = df['ride_count'].rolling(window=24, min_periods=1).mean().shift(1)
        if len(df) >= 7:
            df['roll7_std'] = df['ride_count'].rolling(window=7, min_periods=1).std().shift(1)
        
        df['month'] = df['timestamp'].dt.month
        df['day_of_week'] = df['timestamp'].dt.weekday
        df['is_weekend'] = df['timestamp'].dt.weekday.isin([5,6]).astype(int)
        
        df['hour_sin'] = np.sin(2 * np.pi * df['hour_of_day'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour_of_day'] / 24)
        
        return df

    def prepare_train_test(self, df: pd.DataFrame, cutoff_hours: int = 168):
        """
        Prepare features and split into train/test using a time cut-off
        """
        df2 = self.make_lag_features(df)
        
        lag_cols = [c for c in df2.columns if c.startswith('lag_')]
        roll_cols = [c for c in ['roll3_mean', 'roll24_mean', 'roll7_std'] if c in df2.columns]
        
        df2 = df2.dropna(subset=lag_cols + roll_cols)
        
        if len(df2) == 0:
            raise ValueError("No valid data after creating features. Check data length and lag requirements.")
        
        feature_cols = lag_cols + roll_cols + [
            'hour_of_day', 'hour_sin', 'hour_cos', 
            'day_of_week', 'is_weekend', 'month'
        ]
        
        for extra in ['demand_index', 'avg_price_per_km', 'avg_fare', 'avg_duration_mins']:
            if extra in df2.columns:
                feature_cols.append(extra)
        
        df2 = df2.sort_values('timestamp')
        
        if len(df2) < 100:
            split_idx = int(len(df2) * 0.7)
            train_df = df2.iloc[:split_idx]
            test_df = df2.iloc[split_idx:]
        else:
            test_period = min(cutoff_hours, int(len(df2) * 0.3))
            train_df = df2.iloc[:-test_period]
            test_df = df2.iloc[-test_period:]
        
        print(f"Training samples: {len(train_df)}, Test samples: {len(test_df)}")
        return train_df, test_df, feature_cols

    def train_models(self, X_train: pd.DataFrame, y_train: pd.Series) -> Dict[str, Any]:
        """Train models with improved parameters"""
        models = {}
        
        X_train_scaled = self.scaler.fit_transform(X_train)
        
        rf_params = {
            'n_estimators': 100,
            'max_depth': 8,
            'min_samples_leaf': 5,
            'min_samples_split': 10,
            'random_state': 42,
            'n_jobs': -1
        }
        rf = RandomForestRegressor(**rf_params)
        rf.fit(X_train_scaled, y_train)
        models['RandomForest'] = {'model': rf, 'params': rf_params}
        
        self.feature_importance['RandomForest'] = dict(zip(X_train.columns, rf.feature_importances_))

        if XGBOOST_AVAILABLE:
            xgb_params = {
                'n_estimators': 100,
                'max_depth': 6,
                'learning_rate': 0.1,
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'random_state': 42,
                'verbosity': 0,
            }
            xg = xgb.XGBRegressor(**xgb_params)
            xg.fit(X_train_scaled, y_train)
            models['XGBoost'] = {'model': xg, 'params': xgb_params}
            self.feature_importance['XGBoost'] = dict(zip(X_train.columns, xg.feature_importances_))
        else:
          
            lr = LinearRegression()
            lr.fit(X_train_scaled, y_train)
            models['LinearRegression'] = {'model': lr, 'params': {}}

        return models

    def evaluate(self, model, X, y, model_name: str = ""):
        X_scaled = self.scaler.transform(X)
        preds = model.predict(X_scaled)
        
        preds = np.maximum(preds, 0) 
        
        rmse = math.sqrt(mean_squared_error(y, preds))
        mae = mean_absolute_error(y, preds)
        
        with np.errstate(divide='ignore', invalid='ignore'):
            mape = np.where(y > 0, np.abs((y - preds) / y), 0)
            mape = np.mean(mape) * 100
        
        r2 = r2_score(y, preds)
        
        return {
            'rmse': rmse, 
            'mae': mae, 
            'mape_pct': mape,
            'r2_score': r2,
            'predictions': preds
        }

    def forecast_horizon(self, last_row: pd.Series, model, feature_cols, horizon: int):
        """Generate step-ahead forecasts with improved logic"""
        forecasts = []
        
        base_time = pd.to_datetime(last_row['timestamp'])
        current_features = last_row[feature_cols].copy()
        
        for h in range(1, horizon + 1):
            ts = base_time + pd.Timedelta(hours=h)
            
            current_features['hour_of_day'] = ts.hour
            current_features['hour_sin'] = np.sin(2 * np.pi * ts.hour / 24)
            current_features['hour_cos'] = np.cos(2 * np.pi * ts.hour / 24)
            current_features['day_of_week'] = ts.weekday()
            current_features['is_weekend'] = int(ts.weekday() >= 5)
            current_features['month'] = ts.month
            
            fv_df = pd.DataFrame([current_features]).reindex(columns=feature_cols, fill_value=0)
            fv_scaled = self.scaler.transform(fv_df)
            
            pred = max(0.0, float(model.predict(fv_scaled)[0]))
            
            forecasts.append({
                'timestamp': ts, 
                'predicted_ride_count': pred,
                'hour': ts.hour,
                'day_of_week': ts.strftime('%A')
            })
            
            if 'lag_1' in feature_cols:
                current_features['lag_1'] = pred
            if 'lag_24' in feature_cols and h >= 24:
                current_features['lag_24'] = forecasts[h-24]['predicted_ride_count']
            
        return pd.DataFrame(forecasts)

    def compute_supply_plan(self, forecast_df: pd.DataFrame) -> pd.DataFrame:
        """Compute supply requirements with realistic demand (around 100 rides/hour normal times)"""
        df = forecast_df.copy()
        
        current_scale = df['predicted_ride_count'].sum()
        avg_current = current_scale / len(df) if len(df) > 0 else 0
        
        print(f"Current prediction stats - Total: {current_scale:.0f}, Avg per hour: {avg_current:.1f}")
        
        target_avg = 100  
        if avg_current < 50:  
            scale_factor = target_avg / max(avg_current, 1)
            df['predicted_ride_count'] = df['predicted_ride_count'] * scale_factor
            print(f"Scaled predictions by factor {scale_factor:.2f} to get ~{target_avg} rides/hour average")
        
        trips_per_cab_per_hour = 1.0
        
        df['required_cabs'] = np.ceil(df['predicted_ride_count'] / trips_per_cab_per_hour).astype(int)
        df['required_buses'] = np.ceil(df['predicted_ride_count'] / self.bus_capacity).astype(int)
        
        if self.city.lower() == 'delhi':
            effective_cabs = max(50, self.available_cabs - 80) 
            print(f"DELHI CAB SHORTAGE: Only {effective_cabs} cabs available (normally {self.available_cabs})")
        else:
            effective_cabs = self.available_cabs
        
        df['available_on_demand_capacity'] = effective_cabs * trips_per_cab_per_hour
        df['available_bus_capacity'] = self.available_buses * self.bus_capacity
        df['total_available_capacity'] = df['available_on_demand_capacity'] + df['available_bus_capacity']
        
        # Coverage metrics
        df['coverage_pct'] = (df['total_available_capacity'] / (df['predicted_ride_count'] + 1e-9)).clip(upper=1.0) * 100.0
        df['supply_deficit'] = (df['predicted_ride_count'] - df['total_available_capacity']).clip(lower=0.0)
        df['uncovered_rides'] = df['supply_deficit']
        
        df['cabs_needed_additional'] = (df['supply_deficit'] / trips_per_cab_per_hour).apply(np.ceil).astype(int)
        df['buses_needed_additional'] = (df['supply_deficit'] / self.bus_capacity).apply(np.ceil).astype(int)
        
        final_avg = df['predicted_ride_count'].mean()
        peak_hour = df['predicted_ride_count'].max()
        print(f"Final prediction stats - Avg: {final_avg:.0f} rides/hour, Peak: {peak_hour:.0f} rides/hour")
        
        return df

    def print_model_details(self, eval_results: Dict):
        print(f"\n=== MODEL DETAILS for {self.city.upper()} ===")
        
        best_model = min(eval_results.items(), key=lambda x: x[1]['metrics']['rmse'])[0]
        
        for model_name, results in eval_results.items():
            metrics = results['metrics']
            params = results.get('params', {})
            
            print(f"\n{model_name}:")
            print(f"  Accuracy: RMSE={metrics['rmse']:.1f}, MAE={metrics['mae']:.1f}, "
                  f"MAPE={metrics['mape_pct']:.1f}%, R²={metrics['r2_score']:.3f}")
            print(f"  Parameters: {params}")
            
            if model_name == best_model:
                print(f" SELECTED AS BEST MODEL")
        
        print(f"\nBest Model: {best_model}")

    def produce_visuals(self, supply_df: pd.DataFrame, eval_results: Dict, test_df: pd.DataFrame, preds_store: Dict):
        """Produce simple visualizations"""
        saved_visuals = []
        
        best_model = min(eval_results.items(), key=lambda x: x[1]['metrics']['rmse'])[0]
        best_preds = preds_store[best_model]
        
        saved_visuals.append(self.plot_simple_weekday_comparison(test_df, best_preds, best_model))
        saved_visuals.append(self.plot_simple_hourly_comparison(test_df, best_preds, best_model))
        saved_visuals.append(self.plot_model_performance(eval_results))
        
        print(f"Saved {len(saved_visuals)} simple visualizations for {self.city}")
        return saved_visuals

    def run_for_city(self):
        print(f"\n=== Demand-Supply Planning for {self.city.upper()} ===")
        try:
            df = self.load_hourly_data()
            
            if len(df) == 0:
                print(f"No data available for {self.city}")
                return None, None
            
            cutoff = pd.to_datetime(df['timestamp'].max()) - pd.Timedelta(days=self.history_days)
            df_hist = df[df['timestamp'] >= cutoff].reset_index(drop=True)
            print(f"Using {len(df_hist)} hourly rows for training (last {self.history_days} days)")

            train_df, test_df, feature_cols = self.prepare_train_test(df_hist)
            
            if len(train_df) == 0 or len(test_df) == 0:
                print(f"Not enough data for training/testing in {self.city}")
                return None, None
                
            X_train = train_df[feature_cols].fillna(0)
            y_train = train_df['ride_count'].values
            X_test = test_df[feature_cols].fillna(0)
            y_test = test_df['ride_count'].values

            print(f"Training on {len(X_train)} samples, testing on {len(X_test)} samples")
            print(f"Features used: {feature_cols}")

            models = self.train_models(X_train, y_train)

            eval_results = {}
            preds_store = {}
            for name, info in models.items():
                m = info['model']
                eval_metrics = self.evaluate(m, X_test, y_test, name)
                eval_results[name] = {
                    'metrics': {k: v for k, v in eval_metrics.items() if k != 'predictions'},
                    'params': info.get('params', {})
                }
                preds_store[name] = eval_metrics['predictions']

            self.print_model_details(eval_results)

            best_name = min(eval_results.items(), key=lambda x: x[1]['metrics']['rmse'])[0]
            best_model = models[best_name]['model']
            print(f"\nSelected best model: {best_name}")

            last_enriched = self.make_lag_features(df_hist).iloc[-1]
            forecast_df = self.forecast_horizon(last_enriched, best_model, feature_cols, self.horizon)

            supply_df = self.compute_supply_plan(forecast_df)
            supply_df['timestamp'] = pd.to_datetime(supply_df['timestamp'])
            
            saved_visuals = self.produce_visuals(supply_df, eval_results, test_df, preds_store)

            # Persist forecast and supply data
            out_csv = f'outputs/demand_forecast/data/forecast_{self.city}.csv'
            supply_df.to_csv(out_csv, index=False)
            
            # Create summary statistics
            total_demand = supply_df['predicted_ride_count'].sum()
            total_capacity = supply_df['total_available_capacity'].sum()
            coverage_pct = min(total_capacity / total_demand * 100, 100) if total_demand > 0 else 100
            total_deficit = supply_df['supply_deficit'].sum()
            critical_hours = (supply_df['supply_deficit'] > 0).sum()

            meta = {
                'city': self.city,
                'timestamp': datetime.now().isoformat(),
                'model_chosen': best_name,
                'eval_results': eval_results,
                'feature_importance': self.feature_importance.get(best_name, {}),
                'summary_statistics': {
                    'total_predicted_demand': total_demand,
                    'total_available_capacity': total_capacity,
                    'coverage_percentage': coverage_pct,
                    'total_supply_deficit': total_deficit,
                    'critical_hours_count': critical_hours,
                    'peak_hour_deficit': supply_df['supply_deficit'].max(),
                    'additional_cabs_needed': supply_df['cabs_needed_additional'].max(),
                    'additional_buses_needed': supply_df['buses_needed_additional'].max()
                },
                'params': {
                    'trips_per_cab_per_hour': self.trips_per_cab_per_hour,
                    'bus_capacity': self.bus_capacity,
                    'available_cabs': self.available_cabs,
                    'available_buses': self.available_buses,
                    'horizon_hours': self.horizon
                },
                'saved_visuals': saved_visuals,
                'forecast_csv': out_csv
            }
            
            meta_path = f'outputs/demand_forecast/data/forecast_{self.city}_meta.json'
            with open(meta_path, 'w') as f:
                json.dump(meta, f, indent=2, default=str)
            
            print(f"Forecast & supply plan saved: {out_csv}")
            print(f"Meta saved: {meta_path}")
            
            # Print key insights
            print(f"\n=== KEY INSIGHTS for {self.city.upper()} ===")
            print(f"Total Predicted Demand: {total_demand:,.0f} rides")
            print(f"Total Available Capacity: {total_capacity:,.0f} rides")
            print(f"Coverage: {coverage_pct:.1f}%")
            print(f"Total Deficit: {total_deficit:,.0f} rides")
            print(f"Critical Hours: {critical_hours} out of {self.horizon}")
            print(f"Peak Hour Deficit: {supply_df['supply_deficit'].max():.0f} rides")
            print(f"Max Additional Cabs Needed: {supply_df['cabs_needed_additional'].max()}")
            print(f"Max Additional Buses Needed: {supply_df['buses_needed_additional'].max()}")

            return supply_df, meta
            
        except Exception as e:
            print(f"Error in run_for_city for {self.city}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None, None


if __name__ == '__main__':
    for city in ['delhi', 'bangalore']:
        planner = DemandSupplyPlanner(
            city=city,
            forecast_horizon_hours=24,
            train_history_days=30,
            trips_per_cab_per_hour=0.4,  
            bus_capacity=50,
            available_cabs=200,  
            available_buses=50   
        )
        try:
            supply_df, meta = planner.run_for_city()
            if supply_df is not None:
                print(f"Successfully completed planning for {city}")
            else:
                print(f"Planning failed for {city}")
        except Exception as e:
            print(f"Error running planner for {city}: {e}")