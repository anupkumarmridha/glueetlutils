Here’s an example documentation for using `QualityUtils` from the `glueetlutils` package. This documentation includes different types of imports, example use cases, and descriptions for each.

---

# **Documentation: Using `QualityUtils` from `glueetlutils`**

## **1. Overview**
The `QualityUtils` class in the `glueetlutils` package provides tools to evaluate data quality for AWS Glue DynamicFrames using data quality rulesets. It is designed to work in Glue jobs and other Spark environments.

---

## **2. Installation**
Make sure the `glueetlutils` package is installed. If you're using a local version, install it with:
```bash
pip install -e .
```

---

## **3. Imports**
### **3.1 Importing `QualityUtils` Directly**
You can import `QualityUtils` directly from the core module:
```python
from glueetlutils.core.quality_utils import QualityUtils
```

### **3.2 Importing from the Top-Level Package**
If `QualityUtils` is exposed via the package's `__init__.py`, you can import it directly:
```python
from glueetlutils import QualityUtils
```

### **3.3 Importing Along with Other Utilities**
You can import `QualityUtils` alongside other utilities from the package:
```python
from glueetlutils.core import QualityUtils, LoadUtils
```

### **3.4 Importing the Logger and Timing Utilities**
If you also need the `logger` and `log_time` utilities:
```python
from glueetlutils.logger import logger
from glueetlutils.timing import log_time
```

---

## **4. Examples**

### **4.1 Basic Example: Using `QualityUtils` in a Glue Job**
This example demonstrates how to use `QualityUtils` to evaluate data quality in an AWS Glue job.

```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from glueetlutils.core.quality_utils import QualityUtils

# Initialize Spark and Glue contexts
sc = SparkContext()
glue_context = GlueContext(sc)

# Initialize QualityUtils
quality_utils = QualityUtils(glue_context)

# Define parameters
dynamic_frame = ...  # AWS Glue DynamicFrame to evaluate
ruleset = "s3://path-to-your-ruleset/ruleset.json"  # Path to your ruleset JSON
context_name = "example_quality_check"

# Evaluate data quality
try:
    summary = quality_utils.evaluate_data_quality(dynamic_frame, ruleset, context_name)
    print(f"Data Quality Summary: {summary}")
except Exception as e:
    print(f"Data quality evaluation failed: {e}")
```

---

### **4.2 Advanced Example: Custom Publishing and Additional Options**
You can customize publishing and additional options when evaluating data quality.

```python
from glueetlutils.core.quality_utils import QualityUtils

# Initialize QualityUtils without GlueContext (for testing or non-Glue environments)
quality_utils = QualityUtils()

# Define parameters
dynamic_frame = ...  # AWS Glue DynamicFrame to evaluate
ruleset = "path/to/local-ruleset.json"  # Local or S3 path to your ruleset
context_name = "custom_options_example"

# Custom publishing options
publishing_options = {
    "dataQualityEvaluationContext": context_name,
    "enableDataQualityCloudWatchMetrics": False,
    "enableDataQualityResultsPublishing": False,
}

# Custom additional options
additional_options = {
    "observations.scope": "FAILED_ONLY",
    "performanceTuning.caching": "CACHE_EVERYTHING",
}

# Evaluate data quality
summary = quality_utils.evaluate_data_quality(
    dynamic_frame,
    ruleset,
    context_name,
    publishing_options=publishing_options,
    additional_options=additional_options,
)

print(f"Custom Data Quality Summary: {summary}")
```

---

### **4.3 Using `QualityUtils` for Local Testing**
This example shows how to use `QualityUtils` for testing outside of AWS Glue.

```python
from glueetlutils.core.quality_utils import QualityUtils

# Initialize QualityUtils without Glue context
quality_utils = QualityUtils()

# Define parameters
dynamic_frame = ...  # A Spark DataFrame converted to a DynamicFrame
ruleset = "path/to/local-ruleset.json"
context_name = "local_test_example"

# Evaluate data quality
summary = quality_utils.evaluate_data_quality(dynamic_frame, ruleset, context_name)
print(f"Local Data Quality Summary: {summary}")
```

---

### **4.4 Logging and Timing**
The `QualityUtils` methods automatically log execution time and results. You can also use the global logger and `log_time` decorator.

```python
from glueetlutils import QualityUtils, logger, log_time

@log_time
def evaluate_quality_example(dynamic_frame, ruleset, context_name):
    quality_utils = QualityUtils()
    summary = quality_utils.evaluate_data_quality(dynamic_frame, ruleset, context_name)
    logger.info(f"Summary: {summary}")
    return summary

# Example usage
dynamic_frame = ...  # AWS Glue DynamicFrame
ruleset = "path/to/ruleset.json"
context_name = "timed_quality_check"
evaluate_quality_example(dynamic_frame, ruleset, context_name)
```

---

## **5. Data Quality Summary Output**
The `evaluate_data_quality` method returns a summary dictionary with the following structure:
```python
{
    "context_name": "example_quality_check",
    "total_rules": 5,
    "failed_rules": 1,
    "passed_rules": 4,
}
```

---

## **6. Error Handling**
- Logs exceptions with detailed traceback using `logger.exception`.
- Raises exceptions to allow the Glue job to fail gracefully or retry.

Example:
```python
try:
    summary = quality_utils.evaluate_data_quality(dynamic_frame, ruleset, context_name)
except Exception as e:
    logger.error(f"Data quality evaluation failed: {e}")
```

---

## **7. Notes**
- Ensure the `ruleset` JSON is properly formatted and accessible (e.g., in S3 or local).
- The DynamicFrame should have the required schema for the ruleset.

---

## **8. Conclusion**
`QualityUtils` is a reusable tool for evaluating data quality in AWS Glue jobs or PySpark environments. Its modular design ensures flexibility, and built-in logging and timing simplify debugging and performance tracking.
