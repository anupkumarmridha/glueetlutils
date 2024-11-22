from setuptools import setup, find_packages

setup(
    name="datatransformutils",
    version="0.1.0",
    description="Reusable data transformation utilities for AWS Glue using PySpark",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Anup Kumar Mridha",
    author_email="your.email@example.com",
    url="https://github.com/anupkumarmridha/datatransformutils",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.3.0",
        "aws-glue-sdk>=1.0.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
