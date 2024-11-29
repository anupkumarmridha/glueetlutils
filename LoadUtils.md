Here’s a comprehensive documentation on using the `LoadUtils` class from the `glueetlutils` package. The examples showcase different imports and provide scenarios for usage across multiple AWS Glue jobs.

---

# **Documentation: Using `LoadUtils` from `glueetlutils`**

## **1. Overview**
The `LoadUtils` class in the `glueetlutils` package provides reusable utilities for loading data from S3, writing data to Redshift, and other data operations. Designed for multiple AWS Glue jobs, it supports:
- Loading data from S3 into Glue DynamicFrames.
- Writing Glue DynamicFrames to Redshift.
- Custom retry mechanisms with exponential backoff for resilient data operations.

---

## **2. Installation**
Ensure the `glueetlutils` package is installed in your environment. If using a local version:
```bash
pip install -e .
```

---

## **3. Imports**
### **3.1 Importing `LoadUtils` Directly**
The most common way to import:
```python
from glueetlutils import LoadUtils
```

### **3.2 Importing from the Core Module**
To import directly from the `core` submodule:
```python
from glueetlutils.core.load_utils import LoadUtils
```

### **3.3 Importing Alongside Other Utilities**
If using multiple utilities from the `glueetlutils` package:
```python
from glueetlutils import LoadUtils, QualityUtils
```

### **3.4 Importing with Logger and Timing**
For advanced use cases where logging and timing are needed:
```python
from glueetlutils import LoadUtils, logger, log_time
```

---

## **4. Examples of Usage**

### **4.1 Basic Usage: Loading Data from S3**
This example demonstrates how to load data from S3 into an AWS Glue DynamicFrame.

```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from glueetlutils import LoadUtils

# Initialize Spark and Glue contexts
sc = SparkContext()
glue_context = GlueContext(sc)

# Initialize LoadUtils
load_utils = LoadUtils(glue_context)

# Load data from S3
s3_path = "s3://your-bucket/your-folder/"
transformation_ctx = "s3_load_context"
data = load_utils.load_data_from_s3(s3_path, transformation_ctx)

# Print the schema or count of the loaded data
if data:
    print(f"Schema: {data.schema()}")
    print(f"Row count: {data.count()}")
else:
    print("No data found.")
```

---

### **4.2 Writing Data to Redshift**
This example shows how to write an AWS Glue DynamicFrame to a Redshift table.

```python
from glueetlutils import LoadUtils

# Initialize LoadUtils with Glue context
load_utils = LoadUtils(glue_context)

# Connection options for Redshift
connection_options = {
    "url": "jdbc:redshift://example-cluster:5439/dev",
    "user": "username",
    "password": "password",
}

# Define the table name
table_name = "target_table"

# Write data to Redshift
load_utils.write_data_to_redshift(connection_options, data, table_name)
```

---

### **4.3 Retry Logic with Custom Decorators**
For operations prone to transient errors, `LoadUtils` includes a retry mechanism with exponential backoff.

```python
from glueetlutils import LoadUtils

# Example function to demonstrate retry mechanism
@LoadUtils._retry_with_backoff(max_attempts=5, initial_delay=2, max_delay=20, exponential_base=3)
def process_data():
    # Simulate a transient error
    raise Exception("Simulated transient error")

try:
    process_data()
except Exception as e:
    print(f"Failed after retries: {e}")
```

---

### **4.4 Loading Data from Redshift**
You can load data from a Redshift table into an AWS Glue DynamicFrame.

```python
from glueetlutils import LoadUtils

# Initialize LoadUtils with Glue context
load_utils = LoadUtils(glue_context)

# Connection options for Redshift
connection_options = {
    "url": "jdbc:redshift://example-cluster:5439/dev",
    "user": "username",
    "password": "password",
}

# Load data from a Redshift table
table_name = "source_table"
redshift_data = load_utils.load_data_from_redshift(connection_options, table_name)

if redshift_data:
    print(f"Loaded {redshift_data.count()} rows from {table_name}.")
```

---

### **4.5 Handling Empty Data**
The `load_data_from_s3` method gracefully handles empty datasets and returns `None`.

```python
from glueetlutils import LoadUtils

# Load data from S3
data = load_utils.load_data_from_s3("s3://empty-bucket/", "empty_data_context")

if data is None:
    print("No data found in the specified S3 path.")
else:
    print(f"Loaded {data.count()} rows.")
```

---

### **4.6 Logging and Timing in Custom Functions**
Use the `log_time` decorator and `logger` for custom Glue operations.

```python
from glueetlutils import LoadUtils, logger, log_time

# Custom function to process data
@log_time
def process_and_write_data(glue_context, s3_path, redshift_options, table_name):
    load_utils = LoadUtils(glue_context)
    
    # Load data from S3
    data = load_utils.load_data_from_s3(s3_path, "load_context")
    if data:
        # Write to Redshift
        load_utils.write_data_to_redshift(redshift_options, data, table_name)
    else:
        logger.warning("No data found to process.")

# Example usage
s3_path = "s3://your-bucket/data/"
redshift_options = {
    "url": "jdbc:redshift://example-cluster:5439/dev",
    "user": "username",
    "password": "password",
}
table_name = "processed_data"

process_and_write_data(glue_context, s3_path, redshift_options, table_name)
```

---

## **5. Handling Errors**

### Logging Errors
All errors are logged using `logger.exception`, providing full tracebacks for debugging:
```plaintext
2024-11-30 10:00:00 - glueetlutils - ERROR - Error loading data from S3: [Error Message]
Traceback (most recent call last):
  File "path/to/file", line X, in function
    ...
```

### Custom Exception Handling
You can handle exceptions explicitly in your Glue job:
```python
try:
    data = load_utils.load_data_from_s3(s3_path, "context_name")
except ValueError as ve:
    print(f"ValueError: {ve}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

---

## **6. Summary of Methods**

### **`load_data_from_s3`**
**Description**: Loads data from S3 into an AWS Glue DynamicFrame.
- **Args**:
  - `s3_path` (str): S3 path to the data.
  - `transformation_ctx` (str): Unique string for the transformation context.
  - `format` (str, default="csv"): Data format.
  - `format_options` (dict, optional): Additional format options.
- **Returns**: DynamicFrame or `None` if no data is found.

---

### **`write_data_to_redshift`**
**Description**: Writes an AWS Glue DynamicFrame to a Redshift table.
- **Args**:
  - `connection_options` (dict): Redshift connection parameters.
  - `dyf` (DynamicFrame): DynamicFrame to write.
  - `table_name` (str): Target table in Redshift.

---

### **`load_data_from_redshift`**
**Description**: Loads data from a Redshift table into a Glue DynamicFrame.
- **Args**:
  - `connection_options` (dict): Redshift connection parameters.
  - `table_name` (str): Source table in Redshift.
- **Returns**: DynamicFrame with the data.

---

### **`_retry_with_backoff`**
**Description**: A decorator that implements retry logic with exponential backoff for transient errors.
- **Args**:
  - `max_attempts` (int): Maximum retries.
  - `initial_delay` (int): Initial wait time between retries.
  - `max_delay` (int): Maximum wait time.
  - `exponential_base` (int): Multiplier for exponential backoff.

---

## **7. Conclusion**
`LoadUtils` is a powerful and reusable utility for handling data operations in AWS Glue jobs. Its modular design and built-in logging make it ideal for use across multiple jobs, ensuring consistency and error resilience.