from setuptools import setup, find_packages

setup(
    name="datatransformutils",
    version="0.1.1",
    description="Reusable data transformation utilities for AWS Glue using PySpark",
    author="Anup Kumar Mridha",
    author_email="anupkumarmridha.net@gmail.com",
    url="https://github.com/anupkumarmridha/datatransformutils",
    packages=find_packages(),
    install_requires=[
        'pyspark',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)

