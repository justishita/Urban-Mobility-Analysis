"""
Plotting functions for Urban Mobility Analysis.

This module contains functions for creating various visualizations
used in the urban mobility analysis pipeline.
"""

import os
from typing import Dict, List, Optional, Union, Tuple
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set style
plt.style.use('seaborn')
sns.set_palette("viridis")

# Create output directory for visualizations
OUTPUT_DIR = Path("visualizations")
OUTPUT_DIR.mkdir(exist_ok=True)

def plot_time_series(
    data: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    save_path: Optional[Union[str, Path]] = None,
    figsize: Tuple[int, int] = (12, 6)
) -> plt.Figure:
    """Plot time series data.
    
    Args:
        data: DataFrame containing the data
        x_col: Column name for x-axis (time)
        y_col: Column name for y-axis (metric)
        title: Plot title
        x_label: Label for x-axis (defaults to x_col)
        y_label: Label for y-axis (defaults to y_col)
        save_path: Path to save the plot (optional)
        figsize: Figure size (width, height) in inches
        
    Returns:
        Matplotlib Figure object"""
    plt.figure(figsize=figsize)
    sns.lineplot(data=data, x=x_col, y=y_col)
    
    plt.title(title, fontsize=14)
    plt.xlabel(x_label or x_col)
    plt.ylabel(y_label or y_col)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    if save_path:
        save_path = Path(save_path)
        if save_path.suffix == '':
            save_path = save_path.with_suffix('.png')
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved plot to {save_path}")
    
    return plt.gcf()

def plot_geo_distribution(
    data: pd.DataFrame,
    lat_col: str = 'latitude',
    lon_col: str = 'longitude',
    color_col: Optional[str] = None,
    title: str = 'Geographical Distribution',
    save_path: Optional[Union[str, Path]] = None,
    **kwargs
) -> go.Figure:
    """Create a geographical distribution plot.
    
    Args:
        data: DataFrame containing location data
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        color_col: Column to use for coloring points
        title: Plot title
        save_path: Path to save the plot (optional)
        **kwargs: Additional arguments to pass to px.scatter_mapbox
        
    Returns:
        Plotly Figure object
    """
    fig = px.scatter_mapbox(
        data,
        lat=lat_col,
        lon=lon_col,
        color=color_col,
        title=title,
        zoom=10,
        mapbox_style="open-street-map",
        **kwargs
    )
    
    fig.update_layout(
        margin={"r": 0, "t": 30, "l": 0, "b": 0},
        height=600
    )
    
    if save_path:
        save_path = Path(save_path)
        if save_path.suffix == '':
            save_path = save_path.with_suffix('.html')
        save_path.parent.mkdir(parents=True, exist_ok=True)
        fig.write_html(str(save_path))
        logger.info(f"Saved geo plot to {save_path}")
    
    return fig

def plot_model_comparison(
    metrics: Dict[str, Dict[str, float]],
    metric_name: str,
    title: str = 'Model Comparison',
    save_path: Optional[Union[str, Path]] = None,
    figsize: Tuple[int, int] = (12, 6)
) -> plt.Figure:
    """Plot comparison of model metrics.
    
    Args:
        metrics: Dictionary of model metrics
        metric_name: Name of the metric to compare
        title: Plot title
        save_path: Path to save the plot (optional)
        figsize: Figure size (width, height) in inches
        
    Returns:
        Matplotlib Figure object
    """
    models = list(metrics.keys())
    values = [metrics[model][metric_name] for model in models]
    
    plt.figure(figsize=figsize)
    ax = sns.barplot(x=models, y=values)
    
    # Add value labels on top of bars
    for p in ax.patches:
        ax.annotate(
            f"{p.get_height():.3f}",
            (p.get_x() + p.get_width() / 2., p.get_height()),
            ha='center', va='center',
            xytext=(0, 10),
            textcoords='offset points'
        )
    
    plt.title(title, fontsize=14)
    plt.ylabel(metric_name)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    if save_path:
        save_path = Path(save_path)
        if save_path.suffix == '':
            save_path = save_path.with_suffix('.png')
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved model comparison plot to {save_path}")
    
    return plt.gcf()

def plot_city_comparison(
    city_data: Dict[str, pd.DataFrame],
    metric_col: str,
    title: str = 'City Comparison',
    save_path: Optional[Union[str, Path]] = None,
    figsize: Tuple[int, int] = (12, 6)
) -> plt.Figure:
    """Compare metrics across different cities.
    
    Args:
        city_data: Dictionary mapping city names to DataFrames
        metric_col: Column containing the metric to compare
        title: Plot title
        save_path: Path to save the plot (optional)
        figsize: Figure size (width, height) in inches
        
    Returns:
        Matplotlib Figure object
    """
    comparison_data = []
    
    for city, df in city_data.items():
        if metric_col in df.columns:
            comparison_data.append({
                'city': city,
                'value': df[metric_col].mean(),
                'std': df[metric_col].std()
            })
    
    if not comparison_data:
        logger.warning(f"No data found for metric: {metric_col}")
        return plt.gcf()
        
    df = pd.DataFrame(comparison_data)
    
    plt.figure(figsize=figsize)
    ax = sns.barplot(data=df, x='city', y='value', yerr=df['std'])
    
    # Add value labels on top of bars
    for p in ax.patches:
        ax.annotate(
            f"{p.get_height():.2f}",
            (p.get_x() + p.get_width() / 2., p.get_height()),
            ha='center', va='center',
            xytext=(0, 10),
            textcoords='offset points'
        )
    
    plt.title(title, fontsize=14)
    plt.xlabel('City')
    plt.ylabel(metric_col)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    if save_path:
        save_path = Path(save_path)
        if save_path.suffix == '':
            save_path = save_path.with_suffix('.png')
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved city comparison plot to {save_path}")
    
    return plt.gcf()

def plot_correlation_matrix(
    data: pd.DataFrame,
    title: str = 'Correlation Matrix',
    save_path: Optional[Union[str, Path]] = None,
    figsize: Tuple[int, int] = (12, 10)
) -> plt.Figure:
    """Plot correlation matrix for the given data.
    
    Args:
        data: DataFrame containing numerical columns
        title: Plot title
        save_path: Path to save the plot (optional)
        figsize: Figure size (width, height) in inches
        
    Returns:
        Matplotlib Figure object
    """
    plt.figure(figsize=figsize)
    
    # Calculate correlation matrix
    corr = data.select_dtypes(include=[np.number]).corr()
    
    # Create mask for upper triangle
    mask = np.triu(np.ones_like(corr, dtype=bool))
    
    # Plot heatmap
    sns.heatmap(
        corr,
        mask=mask,
        annot=True,
        fmt=".2f",
        cmap='coolwarm',
        center=0,
        square=True,
        cbar_kws={"shrink": 0.8}
    )
    
    plt.title(title, fontsize=14, pad=20)
    plt.tight_layout()
    
    if save_path:
        save_path = Path(save_path)
        if save_path.suffix == '':
            save_path = save_path.with_suffix('.png')
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved correlation matrix to {save_path}")
    
    return plt.gcf()

def plot_rideshare_demand(
    data: pd.DataFrame,
    time_col: str = 'hour',
    demand_col: str = 'demand',
    title: str = 'Rideshare Demand by Hour',
    save_path: Optional[Union[str, Path]] = None,
    figsize: Tuple[int, int] = (14, 6)
) -> plt.Figure:
    """Plot rideshare demand over time.
    
    Args:
        data: DataFrame containing time and demand data
        time_col: Column containing time information
        demand_col: Column containing demand values
        title: Plot title
        save_path: Path to save the plot (optional)
        figsize: Figure size (width, height) in inches
        
    Returns:
        Matplotlib Figure object
    """
    plt.figure(figsize=figsize)
    
    # Plot demand
    sns.lineplot(
        data=data,
        x=time_col,
        y=demand_col,
        label='Rideshare Demand',
        linewidth=2.5
    )
    
    # Add horizontal line for average demand
    avg_demand = data[demand_col].mean()
    plt.axhline(
        y=avg_demand,
        color='r',
        linestyle='--',
        label=f'Avg Demand: {avg_demand:.1f}'
    )
    
    # Add peak hour annotation
    peak_idx = data[demand_col].idxmax()
    peak_hour = data.loc[peak_idx, time_col]
    peak_demand = data.loc[peak_idx, demand_col]
    
    plt.annotate(
        f'Peak: {peak_hour}h',
        xy=(peak_hour, peak_demand),
        xytext=(peak_hour + 0.5, peak_demand * 0.9),
        arrowprops=dict(facecolor='black', shrink=0.05)
    )
    
    # Customize plot
    plt.title(title, fontsize=16, pad=20)
    plt.xlabel('Hour of Day', fontsize=12)
    plt.ylabel('Demand (normalized)', fontsize=12)
    plt.xticks(range(0, 25, 2))
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    
    if save_path:
        save_path = Path(save_path)
        if save_path.suffix == '':
            save_path = save_path.with_suffix('.png')
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved rideshare demand plot to {save_path}")
    
    return plt.gcf()

def plot_bus_vs_rideshare(
    bus_data: pd.DataFrame,
    rideshare_data: pd.DataFrame,
    time_col: str = 'hour',
    bus_col: str = 'bus_service',
    rideshare_col: str = 'demand',
    title: str = 'Bus Service vs Rideshare Demand',
    save_path: Optional[Union[str, Path]] = None,
    figsize: Tuple[int, int] = (14, 6)
) -> plt.Figure:
    """Compare bus service frequency with rideshare demand.
    
    Args:
        bus_data: DataFrame containing bus service data
        rideshare_data: DataFrame containing rideshare demand data
        time_col: Column containing time information
        bus_col: Column containing bus service metrics
        rideshare_col: Column containing rideshare demand
        title: Plot title
        save_path: Path to save the plot (optional)
        figsize: Figure size (width, height) in inches
        
    Returns:
        Matplotlib Figure object
    """
    fig, ax1 = plt.subplots(figsize=figsize)
    
    # Plot bus service frequency (left y-axis)
    color = 'tab:blue'
    ax1.set_xlabel('Hour of Day', fontsize=12)
    ax1.set_ylabel('Bus Service Frequency', color=color, fontsize=12)
    ax1.plot(
        bus_data[time_col],
        bus_data[bus_col],
        color=color,
        label='Bus Service',
        linewidth=2.5
    )
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.set_xticks(range(0, 25, 2))
    
    # Create second y-axis for rideshare demand
    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel('Rideshare Demand', color=color, fontsize=12)
    ax2.plot(
        rideshare_data[time_col],
        rideshare_data[rideshare_col],
        color=color,
        label='Rideshare Demand',
        linewidth=2.5,
        linestyle='--'
    )
    ax2.tick_params(axis='y', labelcolor=color)
    
    # Add title and legend
    plt.title(title, fontsize=16, pad=20)
    
    # Combine legends from both axes
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(
        lines1 + lines2,
        labels1 + labels2,
        loc='upper left',
        bbox_to_anchor=(0.1, -0.15),
        ncol=2
    )
    
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    if save_path:
        save_path = Path(save_path)
        if save_path.suffix == '':
            save_path = save_path.with_suffix('.png')
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved bus vs rideshare plot to {save_path}")
    
    return plt.gcf()
