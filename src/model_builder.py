import os
import json
from datetime import datetime
import traceback
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession
import platform
import joblib



class ModelBuilder:
    """Build and evaluate predictive models for surge pricing"""

    def __init__(self,spark):
        self.spark = (
            SparkSession.builder
            .appName("Urban Mobility Analysis - ModelBuilder")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
            .getOrCreate()
        )


    def split_data(self, data, train_ratio=0.8, seed=42):
        splits = data.randomSplit([train_ratio, 1 - train_ratio], seed=seed)
        train_df, test_df = splits[0], splits[1]
        print(f"Training set: {train_df.count()} records")
        print(f"Test set: {test_df.count()} records")
        return train_df, test_df

    def train_models(self, train_df, problem_type='regression'):
        """Train multiple models and return dictionary of trained models"""
        print(f"Training {problem_type} models...")
        trained_models = {}
        try:
            if problem_type == 'regression':
                models = {
                    'LinearRegression': LinearRegression(featuresCol="scaled_features", labelCol="label", maxIter=50),
                    'RandomForestRegressor': RandomForestRegressor(featuresCol="scaled_features", labelCol="label", numTrees=50, maxDepth=8),
                    'GradientBoostingRegressor': GBTRegressor(featuresCol="scaled_features", labelCol="label", maxIter=50, maxDepth=6)
                }
            else:
                models = {
                    'LogisticRegression': LogisticRegression(featuresCol="scaled_features", labelCol="label", maxIter=50),
                    'RandomForestClassifier': RandomForestClassifier(featuresCol="scaled_features", labelCol="label", numTrees=50, maxDepth=8),
                    'GradientBoostingClassifier': GBTClassifier(featuresCol="scaled_features", labelCol="label", maxIter=50, maxDepth=6)
                }

            for name, model in models.items():
                print(f"Training {name} ...")
                try:
                    trained = model.fit(train_df)
                    trained_models[name] = trained
                    print(f" {name} trained")
                except Exception as e:
                    print(f" Error training {name}: {e}")
                    traceback.print_exc()
            return trained_models
        except Exception as e:
            print(f"Training pipeline failed: {e}")
            traceback.print_exc()
            return {}

    def evaluate_models(self, models, test_df, problem_type='regression'):
        """Evaluate and return results dict"""
        print(f"Evaluating {problem_type} models...")
        results = {}
        try:
            if problem_type == 'regression':
                evaluators = {
                    'RMSE': RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse"),
                    'MAE' : RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae"),
                    'R2'  : RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
                }
            else:
                evaluators = {
                    'AUC': BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"),
                    'Accuracy': MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy"),
                    'F1': MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
                }

            for name, model in models.items():
                try:
                    preds = model.transform(test_df)
                    model_results = {}
                    for metric_name, evaluator in evaluators.items():
                        try:
                            score = evaluator.evaluate(preds)
                            model_results[metric_name] = float(score)
                        except Exception as me:
                            model_results[metric_name] = None
                            print(f"Could not compute {metric_name} for {name}: {me}")
                    results[name] = model_results
                    print(f"{name}: {model_results}")
                except Exception as e:
                    print(f"Error evaluating {name}: {e}")
                    traceback.print_exc()
            return results
        except Exception as e:
            print(f"Evaluation pipeline failed: {e}")
            traceback.print_exc()
            return {}

    def save_models(self, models, problem_type):
        """Save models safely — use Spark save on Linux/macOS, joblib on Windows."""
        os.makedirs('models', exist_ok=True)

        is_windows = platform.system().lower().startswith('win')

        for name, model in models.items():
            try:
                model_path = os.path.join('models', f"{problem_type}_{name.lower()}")

                if is_windows:
                    # Fallback: use joblib for Windows
                    pkl_path = model_path + ".pkl"
                    joblib.dump(model, pkl_path)
                    print(f" Saved {name} (joblib) to {pkl_path}")
                else:
                    # Use native Spark model save
                    model.write().overwrite().save(model_path)
                    print(f" Saved {name} (Spark) to {model_path}")

            except Exception as e:
                print(f"Error saving {name}: {e}")
                traceback.print_exc()


    def create_model_comparison(self, results, problem_type):
        """Persist results JSON and a small barplot for quick visual check (PNG)"""
        os.makedirs('outputs/data', exist_ok=True)
        os.makedirs('outputs/visuals', exist_ok=True)
        comparison = {
            'timestamp': datetime.now().isoformat(),
            'problem_type': problem_type,
            'results': results
        }
        outpath = f'outputs/data/model_comparison_{problem_type}.json'
        with open(outpath, 'w') as f:
            json.dump(comparison, f, indent=2)
        print(f" Saved model comparison JSON: {outpath}")

        # Create a visual summary (for first metric)
        if not results:
            return
        model_names = list(results.keys())
        first_metric = next(iter(next(iter(results.values())).keys()))
        metric_vals = [results[m].get(first_metric, 0.0) or 0.0 for m in model_names]

        plt.figure(figsize=(8,5))
        bars = plt.bar(model_names, metric_vals)
        plt.title(f'{first_metric} Comparison ({problem_type})')
        plt.xticks(rotation=45)
        for bar, v in zip(bars, metric_vals):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01, f'{v:.3f}', ha='center')
        plt.tight_layout()
        imgpath = f'outputs/visuals/model_comparison_{problem_type}.png'
        plt.savefig(imgpath, dpi=200, bbox_inches='tight')
        plt.close()
        print(f" Saved model comparison plot: {imgpath}")
