# DataTransformUtils - AWS Glue Data Transformation Utility

## Overview
`DataTransformUtils` is a comprehensive utility class designed for reusable data transformation functions using AWS Glue and Spark. It simplifies common data transformation tasks on `DynamicFrame` and `DataFrame` objects in Spark, providing a range of utility functions for data cleaning, transformation, validation, and aggregation.

### Prerequisites
- AWS Glue
- Apache Spark
- PySpark
- Python

## Functions

### 1. `dynamicframe_to_dataframe(dynamic_frame: DynamicFrame) -> DataFrame`
Converts a Glue `DynamicFrame` to a Spark `DataFrame`.
- **Parameters**: `dynamic_frame` - Glue `DynamicFrame` to convert.
- **Returns**: Converted `DataFrame`.

### 2. `dataframe_to_dynamicframe(data_frame: DataFrame, name: str) -> DynamicFrame`
Converts a Spark `DataFrame` back to a Glue `DynamicFrame`.
- **Parameters**:
  - `data_frame`: `DataFrame` to convert.
  - `name`: Name for the resulting `DynamicFrame`.
- **Returns**: Converted `DynamicFrame`.

### 3. `rename_columns(dynamic_frame: DynamicFrame, column_mapping: dict) -> DynamicFrame`
Renames columns in a `DynamicFrame`.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `column_mapping`: Dictionary mapping old column names to new column names.
- **Returns**: `DynamicFrame` with renamed columns.

### 4. `handle_null_values(dynamic_frame: DynamicFrame, default_values: dict) -> DynamicFrame`
Replaces null values in specified columns with default values.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `default_values`: Dictionary mapping columns to default values.
- **Returns**: `DynamicFrame` with null values handled.

### 5. `select_columns(dynamic_frame: DynamicFrame, columns: list) -> DynamicFrame`
Selects specific columns from a `DynamicFrame`.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `columns`: List of columns to select.
- **Returns**: `DynamicFrame` with selected columns.

### 6. `drop_columns(dynamic_frame: DynamicFrame, columns: list) -> DynamicFrame`
Drops specified columns from a `DynamicFrame`.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `columns`: List of columns to drop.
- **Returns**: `DynamicFrame` with columns dropped.

### 7. `filter_rows(dynamic_frame: DynamicFrame, condition: str) -> DynamicFrame`
Filters rows based on a given condition.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `condition`: SQL-like condition to filter rows.
- **Returns**: Filtered `DynamicFrame`.

### 8. `remove_duplicates(dynamic_frame: DynamicFrame, subset: list = None) -> DynamicFrame`
Removes duplicate rows based on specific columns.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `subset`: List of columns to consider for identifying duplicates.
- **Returns**: `DynamicFrame` with duplicates removed.

### 9. `reorder_columns(dynamic_frame: DynamicFrame, column_order: list) -> DynamicFrame`
Reorders columns in the `DynamicFrame`.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `column_order`: List defining the desired column order.
- **Returns**: `DynamicFrame` with reordered columns.

### 10. `add_columns_with_defaults(dynamic_frame: DynamicFrame, columns_with_defaults: dict) -> DynamicFrame`
Adds new columns with default values to a `DynamicFrame`.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `columns_with_defaults`: Dictionary mapping column names to default values.
- **Returns**: `DynamicFrame` with added columns.

### 11. `change_column_types(dynamic_frame: DynamicFrame, column_types: dict) -> DynamicFrame`
Changes the data types of specified columns.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `column_types`: Dictionary mapping column names to desired data types.
- **Returns**: `DynamicFrame` with updated column types.

### 12. `add_missing_columns(dynamic_frame: DynamicFrame, required_columns: dict) -> DynamicFrame`
Adds missing columns with default values if they do not exist in the schema.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `required_columns`: Dictionary mapping column names to default values.
- **Returns**: `DynamicFrame` with missing columns added.

### 13. `aggregate_data(dynamic_frame: DynamicFrame, group_by_columns: list, agg_expressions: dict) -> DynamicFrame`
Performs aggregations grouped by specific columns.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `group_by_columns`: List of columns to group by.
  - `agg_expressions`: Dictionary mapping columns to aggregation functions.
- **Returns**: Aggregated `DynamicFrame`.

### 14. `apply_string_transforms(dynamic_frame: DynamicFrame, transforms: dict) -> DynamicFrame`
Applies string transformations (uppercasing, lowercasing, trimming) to specified columns.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `transforms`: Dictionary mapping column names to transformation actions (`"upper"`, `"lower"`, `"trim"`).
- **Returns**: `DynamicFrame` with string transformations applied.

### 15. `clean_column_values(dynamic_frame: DynamicFrame, column_regex_mapping: dict) -> DynamicFrame`
Cleans column values by applying regex replacements.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `column_regex_mapping`: Dictionary mapping columns to regex patterns and replacements.
- **Returns**: `DynamicFrame` with cleaned columns.

### 16. `join_frames(dynamic_frame1: DynamicFrame, dynamic_frame2: DynamicFrame, join_columns: list, join_type: str = "inner") -> DynamicFrame`
Joins two `DynamicFrames` based on specified columns and join type.
- **Parameters**:
  - `dynamic_frame1`, `dynamic_frame2`: Glue `DynamicFrames` to join.
  - `join_columns`: List of columns to join on.
  - `join_type`: Type of join (`"inner"`, `"left"`, etc.).
- **Returns**: Joined `DynamicFrame`.

### 17. `calculate_distinct_counts(dynamic_frame: DynamicFrame, columns: list) -> DynamicFrame`
Calculates distinct counts for specified columns.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `columns`: List of columns to calculate distinct counts for.
- **Returns**: `DynamicFrame` with distinct counts.

### 18. `filter_distinct_values(dynamic_frame: DynamicFrame, subset: list = None) -> DynamicFrame`
Filters and returns only distinct rows based on specific columns.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `subset`: List of columns to identify distinct values.
- **Returns**: `DynamicFrame` with only distinct rows.

### 19. `apply_custom_udf(dynamic_frame: DynamicFrame, column: str, udf_function, output_column: str, output_type=StringType()) -> DynamicFrame`
Applies a custom user-defined function (UDF) to a column.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `column`: Column to apply the UDF.
  - `udf_function`: Python function used as a UDF.
  - `output_column`: Name of the new column with the applied UDF.
  - `output_type`: Data type of the output column.
- **Returns**: `DynamicFrame` with the UDF applied.

### 20. `standardize_date_format(dynamic_frame: DynamicFrame, date_columns: list, date_format: str) -> DynamicFrame`
Standardizes the date format for specified columns.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `date_columns`: List of date columns.
  - `date_format`: Desired date format.
- **Returns**: `DynamicFrame` with standardized date formats.

### 21. `replace_column_values(dynamic_frame: DynamicFrame, column: str, replacements: dict) -> DynamicFrame`
Replaces values in a specific column.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `column`: Column with values to replace.
  - `replacements`: Dictionary mapping old values to new values.
- **Returns**: `DynamicFrame` with replaced column values.

### 22. `normalize_numeric_columns(dynamic_frame: DynamicFrame, columns: list) -> DynamicFrame`
Normalizes specified numeric columns to a range between 0 and 1.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `columns`: List of columns to normalize.
- **Returns**: `DynamicFrame` with normalized columns.

### 23. `pivot_data(dynamic_frame: DynamicFrame, pivot_column: str, value_column: str) -> DynamicFrame`
Pivots data based on a specific column.
- **Parameters**:
  - `dynamic_frame`: Glue `DynamicFrame` to transform.
  - `pivot_column`: Column to pivot.
  - `value_column`: Column to use for values.
- **Returns**: Pivoted `DynamicFrame`.
- 
## Usage
```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from DataTransformUtils import DataTransformUtils

sc = SparkContext()
glueContext = GlueContext(sc)

# Initialize DataTransformUtils
data_transform_utils = DataTransformUtils(glueContext)
```

## Notes
- Each method includes appropriate error handling, ensuring that errors are raised with descriptive messages.
- The methods support operations for both `DataFrame` and `DynamicFrame`, facilitating easier transformations in AWS Glue ETL workflows.

Feel free to modify the utility functions as per your specific use case or add more functionalities to extend the capabilities of this class.

