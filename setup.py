from setuptools import setup, find_packages

setup(
    name="datatransformutils",
    version="0.1.0",
    description="Reusable data transformation utilities for AWS Glue using PySpark",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/datatransformutils",
    packages=find_packages(),
    install_requires=[
        'pyspark',
        'awsglue',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)

