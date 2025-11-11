import os
import json
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import traceback

class ModelAnalyzer:
    """Analyze model results and generate insights"""

    def __init__(self, spark):
        self.spark = spark

    def analyze_feature_importance(self, models, feature_cols, problem_type):
        """Gather feature importances from tree models. Return dict."""
        print("Analyzing feature importance...")
        importance_results = {}
        for name, model in models.items():
            try:
                # RandomForest/GBT in Spark expose featureImportances
                if hasattr(model, "featureImportances"):
                    vector_importances = model.featureImportances
                    try:
                        importances = vector_importances.toArray().tolist()
                    except Exception:
                        # sometimes it's already a list
                        importances = list(vector_importances)
                    # pair with feature names (feature_cols assumed to align with assembler order)
                    pairs = list(zip(feature_cols, importances))
                    pairs.sort(key=lambda x: x[1], reverse=True)
                    importance_results[name] = pairs
                    print(f"{name} top features:")
                    for f, s in pairs[:10]:
                        print(f" {f}: {s:.4f}")
                    self._create_feature_importance_plot(pairs, name, problem_type)
            except Exception as e:
                print(f"Could not extract importances for {name}: {e}")
                traceback.print_exc()
        return importance_results

    def _create_feature_importance_plot(self, pairs, model_name, problem_type):
        if not pairs:
            return
        top = pairs[:15]
        features, scores = zip(*top)
        y = np.arange(len(features))
        plt.figure(figsize=(10,6))
        plt.barh(y, scores)
        plt.yticks(y, features)
        plt.gca().invert_yaxis()
        plt.title(f'Feature Importance - {model_name} ({problem_type})')
        plt.tight_layout()
        os.makedirs('outputs/visuals', exist_ok=True)
        imgpath = f'outputs/visuals/feature_importance_{model_name}_{problem_type}.png'
        plt.savefig(imgpath, dpi=200, bbox_inches='tight')
        plt.close()
        print(f" Saved feature importance plot: {imgpath}")

    def generate_insights(self, modeling_df, feature_importance, problem_type):
        """Create an insights JSON and short recommendations"""
        print("Generating insights...")
        insights = {
            'timestamp': datetime.now().isoformat(),
            'problem_type': problem_type,
            'feature_analysis': {},
            'recommendations': []
        }
        if feature_importance:
            # pick the model with highest sum importance on top features
            top_model = next(iter(feature_importance.keys()))
            top_feats = feature_importance[top_model][:5]
            insights['feature_analysis']['top_features'] = [{'feature': f, 'score': float(s)} for f, s in top_feats]
            # recommendations
            feat_names = [f for f, _ in top_feats]
            insights['recommendations'].append("Top predictive features: " + ", ".join(feat_names))

        # city-level patterns from modeling_df (if it's a Spark DataFrame)
        try:
            if hasattr(modeling_df, "groupBy"):
                df = modeling_df.select('city', 'avg_surge_multiplier', 'ride_count').groupBy('city').agg({'avg_surge_multiplier':'mean','ride_count':'sum'})
                pandas = df.toPandas()
                insights['city_summary'] = pandas.to_dict(orient='records')
        except Exception as e:
            print(f"City-level summary generation failed: {e}")

        os.makedirs('outputs/data', exist_ok=True)
        outjson = f"outputs/data/model_insights_{problem_type}.json"
        with open(outjson, 'w') as f:
            json.dump(insights, f, indent=2)
        print(f" Saved insights JSON: {outjson}")
        return insights
