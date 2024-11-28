from setuptools import setup, find_packages

setup(
    name="glueetlutils",  # Updated name to match the new package structure and purpose.
    version="0.1.0",
    description="Reusable utilities for data transformation, quality checks, and loading in AWS Glue using PySpark",
    author="Anup Kumar Mridha",
    author_email="anupkumarmridha.net@gmail.com",
    url="https://github.com/anupkumarmridha/glueetlutils",  # Update if the repo URL changes.
    packages=find_packages(include=["glueetlutils", "glueetlutils.*"]),  # Includes all submodules.
    install_requires=[
        'pyspark',  # Required for Spark transformations.
        'boto3',    # Required for AWS Secrets Manager integration.
        'botocore', # Dependency for AWS SDK.
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",  # Ensures compatibility with Python 3.7+.
    keywords="aws-glue etl pyspark data-transformation",  # Makes it easier to find.
)
