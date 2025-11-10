from setuptools import setup, find_packages

setup(
    name="urban-mobility-analysis",
    version="1.0.0",
    description="Urban Mobility Analysis",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.0.0",
        "pandas>=1.3.0",
        "pymongo>=4.0.0",
        "python-dotenv>=0.19.0",
        "matplotlib>=3.5.0",
        "seaborn>=0.11.0",
    ],
    python_requires=">=3.8",
)