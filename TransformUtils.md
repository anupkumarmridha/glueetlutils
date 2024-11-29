# **Documentation: Using `TransformUtils` from `glueetlutils`**

## **1. Overview**

The `TransformUtils` class provides a comprehensive set of reusable data transformation utilities for AWS Glue DynamicFrames. These methods enable efficient column management, row filtering, data cleaning, and advanced data manipulation, designed specifically for Glue jobs.

---

## **2. Installation**

Ensure the `glueetlutils` package is installed. If using a local version:
```bash
pip install -e .
```

---

## **3. Imports**

### **3.1 Importing `TransformUtils` Directly**
The most common way to use the utility:
```python
from glueetlutils import TransformUtils
```

### **3.2 Importing from the Core Module**
For more explicit imports, use:
```python
from glueetlutils.core.transform_utils import TransformUtils
```

### **3.3 Importing Alongside Other Utilities**
If using multiple utilities together:
```python
from glueetlutils import TransformUtils, LoadUtils, QualityUtils
```

---

## **4. Key Methods in `TransformUtils`**

### **4.1 Column Operations**
- **`rename_columns`**: Rename columns.
- **`add_columns_with_defaults`**: Add new columns with default values.
- **`drop_columns`**: Drop specified columns.
- **`reorder_columns`**: Reorder columns in a specific order.
- **`change_column_types`**: Change the data types of columns.

### **4.2 Row Operations**
- **`filter_rows`**: Filter rows based on SQL-like conditions.
- **`remove_duplicates`**: Remove duplicate rows based on specific columns.
- **`filter_distinct_values`**: Filter distinct rows.

### **4.3 Data Cleaning**
- **`handle_null_values`**: Replace null values in specified columns.
- **`apply_string_transforms`**: Apply string transformations (e.g., upper/lower case).
- **`clean_column_values`**: Clean column values using regex replacements.

### **4.4 Data Manipulation**
- **`aggregate_data`**: Perform aggregations grouped by specific columns.
- **`pivot_data`**: Pivot data based on a specific column.
- **`normalize_numeric_columns`**: Normalize numeric columns to a range between 0 and 1.

### **4.5 Advanced Operations**
- **`apply_custom_udf`**: Apply a user-defined function (UDF) to a column.
- **`add_missing_columns`**: Add missing columns with default values.

---

## **5. Usage Examples**

### **5.1 Initialization**
Initialize `TransformUtils` with a Glue context:
```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from glueetlutils import TransformUtils

# Initialize GlueContext
sc = SparkContext()
glue_context = GlueContext(sc)

# Initialize TransformUtils
transform_utils = TransformUtils(glue_context)
```

---
Here’s a comprehensive list of use cases for all the functions in `TransformUtils`:

---

## **4.1 Column Operations**

### **1. `rename_columns`: Rename Columns**
Rename columns in a DynamicFrame using a dictionary of mappings.

**Example Use Case:**
```python
column_mapping = {"old_name": "new_name", "old_col2": "new_col2"}
renamed_frame = transform_utils.rename_columns(dynamic_frame, column_mapping)
```
- **Scenario**: Standardizing column names for downstream processing.

---

### **2. `add_columns_with_defaults`: Add New Columns**
Add new columns with specified default values.

**Example Use Case:**
```python
columns_with_defaults = {"new_col1": "default_value", "new_col2": 100}
new_frame = transform_utils.add_columns_with_defaults(dynamic_frame, columns_with_defaults)
```
- **Scenario**: Adding metadata columns or filling missing schema fields.

---

### **3. `drop_columns`: Drop Specified Columns**
Remove unnecessary columns from a DynamicFrame.

**Example Use Case:**
```python
columns_to_drop = ["unwanted_col1", "unwanted_col2"]
filtered_frame = transform_utils.drop_columns(dynamic_frame, columns_to_drop)
```
- **Scenario**: Simplifying the dataset by removing redundant or unused columns.

---

### **4. `reorder_columns`: Reorder Columns**
Reorder columns to a specific sequence.

**Example Use Case:**
```python
column_order = ["col1", "col2", "col3"]
ordered_frame = transform_utils.reorder_columns(dynamic_frame, column_order)
```
- **Scenario**: Preparing data for export or aligning with a predefined schema.

---

### **5. `change_column_types`: Change Column Data Types**
Convert columns to specified data types.

**Example Use Case:**
```python
column_types = {"col1": "string", "col2": "int"}
typed_frame = transform_utils.change_column_types(dynamic_frame, column_types)
```
- **Scenario**: Ensuring compatibility with target systems or performing type-sensitive operations.

---

## **4.2 Row Operations**

### **1. `filter_rows`: Filter Rows Based on Conditions**
Filter rows using SQL-like conditions.

**Example Use Case:**
```python
condition = "age > 18 AND status == 'active'"
filtered_frame = transform_utils.filter_rows(dynamic_frame, condition)
```
- **Scenario**: Filtering active customers above a certain age.

---

### **2. `remove_duplicates`: Remove Duplicate Rows**
Remove duplicates based on specified columns.

**Example Use Case:**
```python
subset = ["customer_id", "order_id"]
unique_frame = transform_utils.remove_duplicates(dynamic_frame, subset)
```
- **Scenario**: Ensuring unique records for reporting or analysis.

---

### **3. `filter_distinct_values`: Filter Distinct Rows**
Retrieve only distinct rows based on specific columns or all columns.

**Example Use Case:**
```python
distinct_frame = transform_utils.filter_distinct_values(dynamic_frame, subset=["col1", "col2"])
```
- **Scenario**: Identifying unique combinations of key columns.

---

## **4.3 Data Cleaning**

### **1. `handle_null_values`: Replace Null Values**
Replace `NULL` or missing values with default values.

**Example Use Case:**
```python
default_values = {"col1": "N/A", "col2": 0}
cleaned_frame = transform_utils.handle_null_values(dynamic_frame, default_values)
```
- **Scenario**: Preparing data for machine learning by handling missing values.

---

### **2. `apply_string_transforms`: Transform String Columns**
Apply transformations like `upper`, `lower`, or `trim` to columns.

**Example Use Case:**
```python
transforms = {"name": "upper", "address": "trim"}
transformed_frame = transform_utils.apply_string_transforms(dynamic_frame, transforms)
```
- **Scenario**: Standardizing text data for consistent analysis.

---

### **3. `clean_column_values`: Clean Column Values**
Apply regex replacements to clean up column values.

**Example Use Case:**
```python
regex_mapping = {"email": (r"\s+", ""), "phone": (r"[^0-9]", "")}
cleaned_frame = transform_utils.clean_column_values(dynamic_frame, regex_mapping)
```
- **Scenario**: Removing whitespace and non-numeric characters from phone numbers.

---

## **4.4 Data Manipulation**

### **1. `aggregate_data`: Aggregate Data**
Perform grouped aggregations.

**Example Use Case:**
```python
group_by_columns = ["category"]
agg_expressions = {"sales": "sum", "profit": "avg"}
aggregated_frame = transform_utils.aggregate_data(dynamic_frame, group_by_columns, agg_expressions)
```
- **Scenario**: Summarizing sales and profit by category.

---

### **2. `pivot_data`: Pivot Data**
Pivot data to reshape it into a wider format.

**Example Use Case:**
```python
pivoted_frame = transform_utils.pivot_data(dynamic_frame, pivot_column="month", value_column="sales")
```
- **Scenario**: Transforming monthly sales data into a columnar format for each month.

---

### **3. `normalize_numeric_columns`: Normalize Numeric Columns**
Normalize numeric columns to a range between 0 and 1.

**Example Use Case:**
```python
normalized_frame = transform_utils.normalize_numeric_columns(dynamic_frame, columns=["col1", "col2"])
```
- **Scenario**: Preparing numeric data for machine learning algorithms.

---

## **4.5 Advanced Operations**

### **1. `apply_custom_udf`: Apply Custom UDFs**
Apply user-defined transformations to a column.

**Example Use Case:**
```python
def custom_udf(value):
    return value.lower()

custom_frame = transform_utils.apply_custom_udf(
    dynamic_frame, column="name", udf_function=custom_udf, output_column="name_transformed"
)
```
- **Scenario**: Applying custom logic not supported by default Spark transformations.

---

### **2. `add_missing_columns`: Add Missing Columns**
Ensure all required columns are present, adding defaults where missing.

**Example Use Case:**
```python
required_columns = {"new_col1": "default", "new_col2": 0}
updated_frame = transform_utils.add_missing_columns(dynamic_frame, required_columns)
```
- **Scenario**: Aligning schema with downstream processing requirements.

---

## **6. Logging and Timing**

Each method logs:
- **Start of execution**: Logs the beginning of the operation.
- **Completion**: Logs success with details of the operation.
- **Errors**: Logs exceptions with detailed messages.
- **Execution time**: Measured and logged via `@log_time`.

---

## **7. Summary Table**

| **Category**            | **Function**                | **Description**                                             |
|--------------------------|-----------------------------|-------------------------------------------------------------|
| **Column Operations**    | `rename_columns`           | Rename columns.                                             |
|                          | `add_columns_with_defaults`| Add columns with default values.                           |
|                          | `drop_columns`             | Remove specified columns.                                   |
|                          | `reorder_columns`          | Reorder columns in a specified order.                      |
|                          | `change_column_types`      | Change data types of columns.                              |
| **Row Operations**       | `filter_rows`              | Filter rows based on conditions.                           |
|                          | `remove_duplicates`        | Remove duplicate rows.                                      |
|                          | `filter_distinct_values`   | Retain only distinct rows.                                  |
| **Data Cleaning**        | `handle_null_values`       | Replace null values.                                        |
|                          | `apply_string_transforms`  | Transform string columns (e.g., upper, trim).              |
|                          | `clean_column_values`      | Apply regex to clean column values.                        |
| **Data Manipulation**    | `aggregate_data`           | Group and aggregate data.                                   |
|                          | `pivot_data`               | Pivot data for columnar structure.                         |
|                          | `normalize_numeric_columns`| Normalize numeric columns to 0-1 range.                    |
| **Advanced Operations**  | `apply_custom_udf`         | Apply user-defined transformations.                        |
|                          | `add_missing_columns`      | Add missing columns with default values.                   |

---

## **8. Conclusion**

`TransformUtils` is a robust library for AWS Glue jobs, supporting a wide range of transformations for DynamicFrames. Each method is reusable, well-documented, and integrates logging and timing to ensure reliability.