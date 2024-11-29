# **GlueETLUtils - AWS Glue ETL Utility**

## **Overview**
`GlueETLUtils` is a comprehensive Python package designed to simplify common ETL (Extract, Transform, Load) tasks in AWS Glue jobs using PySpark. It provides reusable utilities for data loading, transformation, quality validation, and other shared functions, streamlining AWS Glue workflows.

## **Key Features**
- **Data Loading**: Streamlined methods for loading data from S3, databases, or other sources.
- **Data Transformation**: Utilities for renaming columns, handling null values, dropping duplicates, and more.
- **Data Quality Validation**: Tools to enforce data quality rules such as non-null checks or distinct value constraints.
- **Common Utilities**: Shared helpers like execution time logging and AWS Secrets Manager integration.

---

### **Package Structure**

```
glueetlutils/
├── __init__.py              # Entry point for the package.
├── core/
│   ├── __init__.py          # Initialize submodule imports.
│   ├── load_utils.py        # Loading and extraction-related tasks.
│   ├── quality_utils.py     # Data quality validation tasks.
│   ├── transform_utils.py   # Transformation and enrichment tasks.
│   ├── common.py            # Shared utilities, e.g., helper methods.
├── setup.py                 # Package setup script for installation.
└── README.md                # Documentation about the package.
```

---

### **1. Installation in AWS Glue**

#### **Set Job Parameters**
When creating or updating your AWS Glue job, configure the following parameters:

1. **Parameter:** `--additional-python-modules`
   - **Value:** `glueetlutils==0.1.1`

2. **Parameter:** `--python-modules-installer-option`
   - **Value:**
     ```
     --no-cache-dir --verbose --index-url https://aws:<CODEARTIFACT-AUTH-TOKEN>@<DOMAIN-NAME>-<ACCOUNT-ID>.d.codeartifact.<REGION-NAME>.amazonaws.com/pypi/pypi-store/simple/
     ```

---

### **2. Generate `--python-modules-installer-option` Value**

#### **Step 1: Authenticate with AWS CLI**
1. Authenticate with AWS CodeArtifact:
   ```bash
   aws codeartifact login --tool pip --repository <REPO-NAME> --domain <DOMAIN-NAME> --domain-owner <ACCOUNT-ID>
   ```

2. Use the **`generate_codeartifact_url.sh`** script to create the installer URL:
   - Place the script in your repository and execute it:
     ```bash
     ./generate_codeartifact_url.sh
     ```
   - The URL will be saved in the `pip_codeartifact_url.txt` file.

3. Extract the URL from `pip_codeartifact_url.txt` and use it in your Glue job configuration.

---

### **3. Network Configuration for Glue Jobs**
If your Glue job runs in a VPC, set up a Glue network connection to allow access to the CodeArtifact repository:

1. Navigate to **AWS Glue Console** → **Connections** → **Create Connection**.
2. Select **VPC** as the connection type.
3. Provide the required subnet and security group details for accessing the CodeArtifact endpoint.
4. Attach the connection to your Glue job under **Network Options**.

---

### **4. Using the Package in Python**
Here’s how to use the `GlueETLUtils` package in your Glue job:

#### **Importing and Initialization**
```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from glueetlutils.core import LoadUtils, TransformUtils, QualityUtils, CommonUtils

sc = SparkContext()
glueContext = GlueContext(sc)

# Initialize utilities
load_utils = LoadUtils(glueContext)
transform_utils = TransformUtils(glueContext)
quality_utils = QualityUtils(glueContext)
common_utils = CommonUtils()
```

---

### **Key Dependencies**
- `PySpark`: Required for Spark transformations.
- `boto3`: For AWS Secrets Manager integration.
- `botocore`: A dependency for `boto3`.

---

<!-- ### **Tests**
Unit tests are located in the `tests/` directory. Use `pytest` to run them:
```bash
pytest tests/
``` -->

---

### **Development Workflow**
1. **Install Locally for Testing**
   ```bash
   pip install -e .
   ```

2. **Build Distribution**
   ```bash
   python setup.py sdist bdist_wheel
   ```

3. **Publish to CodeArtifact or PyPI**
   ```bash
   twine upload dist/*
   ```

---
