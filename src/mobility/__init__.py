""
Mobility Insights module for Urban Mobility Analysis.

This package provides tools for analyzing and generating insights
from urban mobility data.
"""

from .insights import (
    detect_peak_hours,
    compare_transport_modes,
    identify_underserved_areas,
    calculate_delay_metrics,
    generate_insights_report,
    MobilityAnalyzer
)

__all__ = [
    'detect_peak_hours',
    'compare_transport_modes',
    'identify_underserved_areas',
    'calculate_delay_metrics',
    'generate_insights_report',
    'MobilityAnalyzer'
]
