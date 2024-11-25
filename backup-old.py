from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, regexp_replace, upper, lower, trim, countDistinct
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, row_number
        
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import mean
from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import concat_ws

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

class DataTransformUtils:
    """
    A comprehensive utility class for reusable data transformation functions in AWS Glue using Spark.
    """

    def __init__(self, glue_context: GlueContext):
        """
        Initialize the utility class with a GlueContext.
        
        :param glue_context: GlueContext object to manage DynamicFrame operations.
        """
        self.glue_context = glue_context

    def dynamicframe_to_dataframe(self, dynamic_frame: DynamicFrame) -> DataFrame:
        """
        Convert a Glue DynamicFrame to a Spark DataFrame.
        
        :param dynamic_frame: Glue DynamicFrame to be converted.
        :return: Converted Spark DataFrame.
        """
        try:
            return dynamic_frame.toDF()
        except Exception as e:
            raise ValueError(f"Error converting DynamicFrame to DataFrame: {e}")

    def dataframe_to_dynamicframe(self, data_frame: DataFrame, name: str) -> DynamicFrame:
        """
        Convert a Spark DataFrame back to a Glue DynamicFrame.
        
        :param data_frame: Spark DataFrame to be converted.
        :param name: Name for the resulting DynamicFrame.
        :return: Converted Glue DynamicFrame.
        """
        try:
            return DynamicFrame.fromDF(data_frame, self.glue_context, name)
        except Exception as e:
            raise ValueError(f"Error converting DataFrame to DynamicFrame: {e}")

    def rename_columns(self, dynamic_frame: DynamicFrame, column_mapping: dict) -> DynamicFrame:
        """
        Rename columns in a DynamicFrame.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param column_mapping: Dictionary with old column names as keys and new column names as values.
        :return: DynamicFrame with renamed columns.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for old_col, new_col in column_mapping.items():
                df = df.withColumnRenamed(old_col, new_col)
            return self.dataframe_to_dynamicframe(df, "renamed_columns")
        except Exception as e:
            raise ValueError(f"Error renaming columns: {e}")

    def handle_null_values(self, dynamic_frame: DynamicFrame, default_values: dict) -> DynamicFrame:
        """
        Replace null values in specified columns with default values.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param default_values: Dictionary with column names as keys and default values to replace nulls.
        :return: DynamicFrame with null values handled.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for col_name, default_value in default_values.items():
                df = df.withColumn(
                    col_name,
                    when(col(col_name).isNotNull(), col(col_name)).otherwise(lit(default_value))
                )
            return self.dataframe_to_dynamicframe(df, "handled_nulls")
        except Exception as e:
            raise ValueError(f"Error handling null values: {e}")

    def select_columns(self, dynamic_frame: DynamicFrame, columns: list) -> DynamicFrame:
        """
        Select specific columns from a DynamicFrame.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param columns: List of columns to select.
        :return: DynamicFrame with selected columns.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            df = df.select(*columns)
            return self.dataframe_to_dynamicframe(df, "selected_columns")
        except Exception as e:
            raise ValueError(f"Error selecting columns: {e}")

    def drop_columns(self, dynamic_frame: DynamicFrame, columns: list) -> DynamicFrame:
        """
        Drop specified columns from a DynamicFrame.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param columns: List of columns to drop.
        :return: DynamicFrame with specified columns dropped.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            df = df.drop(*columns)
            return self.dataframe_to_dynamicframe(df, "dropped_columns")
        except Exception as e:
            raise ValueError(f"Error dropping columns: {e}")

    def filter_rows(self, dynamic_frame: DynamicFrame, condition: str) -> DynamicFrame:
        """
        Filter rows based on a given condition.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param condition: SQL-like condition to filter rows.
        :return: Filtered DynamicFrame.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            df = df.filter(condition)
            return self.dataframe_to_dynamicframe(df, "filtered_rows")
        except Exception as e:
            raise ValueError(f"Error filtering rows: {e}")

    def remove_duplicates(self, dynamic_frame: DynamicFrame, subset: list = None) -> DynamicFrame:
        """
        Remove duplicate rows based on specific columns.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param subset: List of columns to consider for identifying duplicates.
        :return: DynamicFrame with duplicates removed.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            df = df.dropDuplicates(subset)
            return self.dataframe_to_dynamicframe(df, "removed_duplicates")
        except Exception as e:
            raise ValueError(f"Error removing duplicates: {e}")

    def reorder_columns(self, dynamic_frame: DynamicFrame, column_order: list) -> DynamicFrame:
        """
        Reorder columns in the DynamicFrame.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param column_order: List defining the desired column order.
        :return: DynamicFrame with reordered columns.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            df = df.select(*column_order)
            return self.dataframe_to_dynamicframe(df, "reordered_columns")
        except Exception as e:
            raise ValueError(f"Error reordering columns: {e}")

    def add_columns_with_defaults(self, dynamic_frame: DynamicFrame, columns_with_defaults: dict) -> DynamicFrame:
        """
        Add new columns with default values to a DynamicFrame.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param columns_with_defaults: Dictionary with column names as keys and default values as values.
        :return: DynamicFrame with added columns and default values.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for column_name, default_value in columns_with_defaults.items():
                df = df.withColumn(column_name, lit(default_value))
            return self.dataframe_to_dynamicframe(df, "added_columns_with_defaults")
        except Exception as e:
            raise ValueError(f"Error adding columns with defaults: {e}")

    def change_column_types(self, dynamic_frame: DynamicFrame, column_types: dict) -> DynamicFrame:
        """
        Change the data types of specified columns.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param column_types: Dictionary with column names as keys and desired data types as values.
        :return: DynamicFrame with updated column types.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for column_name, data_type in column_types.items():
                df = df.withColumn(column_name, col(column_name).cast(data_type))
            return self.dataframe_to_dynamicframe(df, "changed_column_types")
        except Exception as e:
            raise ValueError(f"Error changing column types: {e}")

    def add_missing_columns(self, dynamic_frame: DynamicFrame, required_columns: dict) -> DynamicFrame:
        """
        Add missing columns with default values if they do not exist in the schema.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param required_columns: Dictionary with column names as keys and default values as values.
        :return: DynamicFrame with missing columns added.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for column_name, default_value in required_columns.items():
                if column_name not in df.columns:
                    df = df.withColumn(column_name, lit(default_value))
            return self.dataframe_to_dynamicframe(df, "added_missing_columns")
        except Exception as e:
            raise ValueError(f"Error adding missing columns: {e}")

    def aggregate_data(self, dynamic_frame: DynamicFrame, group_by_columns: list, agg_expressions: dict) -> DynamicFrame:
        """
        Perform aggregations grouped by specific columns.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param group_by_columns: List of columns to group by.
        :param agg_expressions: Dictionary with column names as keys and aggregation functions as values.
        :return: Aggregated DynamicFrame.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            agg_exprs = [getattr(col(column_name), agg_function)().alias(column_name)
                         for column_name, agg_function in agg_expressions.items()]
            df = df.groupBy(*group_by_columns).agg(*agg_exprs)
            return self.dataframe_to_dynamicframe(df, "aggregated_data")
        except Exception as e:
            raise ValueError(f"Error aggregating data: {e}")

    def apply_string_transforms(self, dynamic_frame: DynamicFrame, transforms: dict) -> DynamicFrame:
        """
        Apply string transformations like uppercasing, lowercasing, and trimming to specified columns.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param transforms: Dictionary with column names as keys and transformation actions ("upper", "lower", "trim").
        :return: DynamicFrame with string transformations applied.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for column, action in transforms.items():
                if action == "upper":
                    df = df.withColumn(column, upper(col(column)))
                elif action == "lower":
                    df = df.withColumn(column, lower(col(column)))
                elif action == "trim":
                    df = df.withColumn(column, trim(col(column)))
            return self.dataframe_to_dynamicframe(df, "string_transforms")
        except Exception as e:
            raise ValueError(f"Error applying string transformations: {e}")

    def clean_column_values(self, dynamic_frame: DynamicFrame, column_regex_mapping: dict) -> DynamicFrame:
        """
        Clean column values by applying regex replacements.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param column_regex_mapping: Dictionary with column names as keys and tuples (regex pattern, replacement) as values.
        :return: DynamicFrame with cleaned column values.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for column, regex_replacement in column_regex_mapping.items():
                df = df.withColumn(column, regexp_replace(col(column), regex_replacement[0], regex_replacement[1]))
            return self.dataframe_to_dynamicframe(df, "cleaned_columns")
        except Exception as e:
            raise ValueError(f"Error cleaning column values: {e}")

    def join_frames(self, dynamic_frame1: DynamicFrame, dynamic_frame2: DynamicFrame, 
                    join_columns: list, join_type: str = "inner") -> DynamicFrame:
        """
        Join two DynamicFrames based on specified columns and join type.
        
        :param dynamic_frame1: First Glue DynamicFrame.
        :param dynamic_frame2: Second Glue DynamicFrame.
        :param join_columns: List of columns to join on.
        :param join_type: Type of join to perform (e.g., "inner", "left", "right", "outer").
        :return: Joined DynamicFrame.
        """
        try:
            df1 = self.dynamicframe_to_dataframe(dynamic_frame1)
            df2 = self.dynamicframe_to_dataframe(dynamic_frame2)
            df = df1.join(df2, join_columns, join_type)
            return self.dataframe_to_dynamicframe(df, "joined_frames")
        except Exception as e:
            raise ValueError(f"Error joining frames: {e}")

    def calculate_distinct_counts(self, dynamic_frame: DynamicFrame, columns: list) -> DynamicFrame:
        """
        Calculate distinct counts for specified columns.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param columns: List of columns for which to calculate distinct counts.
        :return: DynamicFrame with distinct counts for specified columns.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for column in columns:
                df = df.withColumn(f"{column}_distinct_count", countDistinct(col(column)))
            return self.dataframe_to_dynamicframe(df, "distinct_counts")
        except Exception as e:
            raise ValueError(f"Error calculating distinct counts: {e}")

    def filter_distinct_values(self, dynamic_frame: DynamicFrame, subset: list = None) -> DynamicFrame:
        """
        Filter and return only distinct rows based on specific columns or the entire dataset.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param subset: List of columns to consider for identifying distinct values. If None, considers all columns.
        :return: DynamicFrame with only distinct rows.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            if subset:
                df = df.dropDuplicates(subset)
            else:
                df = df.dropDuplicates()
            return self.dataframe_to_dynamicframe(df, "filtered_distinct_values")
        except Exception as e:
            raise ValueError(f"Error filtering distinct values: {e}")


    def apply_custom_udf(self, dynamic_frame: DynamicFrame, column: str, udf_function, output_column: str, output_type=StringType()) -> DynamicFrame:
        """
        Apply a custom user-defined function (UDF) to a column.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param column: Column to apply the UDF.
        :param udf_function: Python function to be used as UDF.
        :param output_column: Name of the new column with the applied UDF.
        :param output_type: Data type of the output column.
        :return: DynamicFrame with the UDF applied.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            custom_udf = udf(udf_function, output_type)
            df = df.withColumn(output_column, custom_udf(col(column)))
            return self.dataframe_to_dynamicframe(df, "custom_udf_applied")
        except Exception as e:
            raise ValueError(f"Error applying custom UDF: {e}")

        
    def standardize_date_format(self, dynamic_frame: DynamicFrame, date_columns: list, date_format: str) -> DynamicFrame:
        """
        Standardize the date format for specified columns.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param date_columns: List of columns containing dates.
        :param date_format: Desired format for the dates.
        :return: DynamicFrame with standardized date formats.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for column in date_columns:
                df = df.withColumn(column, col(column).cast("timestamp").alias(column, date_format))
            return self.dataframe_to_dynamicframe(df, "standardized_date_format")
        except Exception as e:
            raise ValueError(f"Error standardizing date format: {e}")
            
    def replace_column_values(self, dynamic_frame: DynamicFrame, column: str, replacements: dict) -> DynamicFrame:
        """
        Replace values in a specific column.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param column: Column in which values need to be replaced.
        :param replacements: Dictionary with old values as keys and new values as values.
        :return: DynamicFrame with replaced column values.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for old_value, new_value in replacements.items():
                df = df.withColumn(column, when(col(column) == old_value, lit(new_value)).otherwise(col(column)))
            return self.dataframe_to_dynamicframe(df, "replaced_column_values")
        except Exception as e:
            raise ValueError(f"Error replacing column values: {e}")

    def normalize_numeric_columns(self, dynamic_frame: DynamicFrame, columns: list) -> DynamicFrame:
        """
        Normalize specified numeric columns to a range between 0 and 1.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param columns: List of columns to normalize.
        :return: DynamicFrame with normalized columns.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for column in columns:
                min_value = df.agg({column: "min"}).collect()[0][0]
                max_value = df.agg({column: "max"}).collect()[0][0]
                df = df.withColumn(column, (col(column) - min_value) / (max_value - min_value))
            return self.dataframe_to_dynamicframe(df, "normalized_columns")
        except Exception as e:
            raise ValueError(f"Error normalizing columns: {e}")
        
    def pivot_data(self, dynamic_frame: DynamicFrame, pivot_column: str, value_column: str) -> DynamicFrame:
        """
        Pivot data based on a specific column.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param pivot_column: Column to pivot.
        :param value_column: Column to use for values.
        :return: Pivoted DynamicFrame.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            df = df.groupBy().pivot(pivot_column).agg({value_column: "first"})
            return self.dataframe_to_dynamicframe(df, "pivoted_data")
        except Exception as e:
            raise ValueError(f"Error pivoting data: {e}")
        
    def unpivot_data(self, dynamic_frame: DynamicFrame, unpivot_columns: list, key_column: str, value_column: str) -> DynamicFrame:
        """
        Unpivot specified columns to rows.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param unpivot_columns: List of columns to unpivot.
        :param key_column: Name of the new key column.
        :param value_column: Name of the new value column.
        :return: Unpivoted DynamicFrame.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            df = df.selectExpr(f"stack({len(unpivot_columns)}, {', '.join([f'\"{col}\", {col}' for col in unpivot_columns])}) as ({key_column}, {value_column})")
            return self.dataframe_to_dynamicframe(df, "unpivoted_data")
        except Exception as e:
            raise ValueError(f"Error unpivoting data: {e}")
        
        


    def apply_window_function(self, dynamic_frame: DynamicFrame, partition_by: list, order_by: str, function: str) -> DynamicFrame:
        """
        Apply a window function (e.g., rank, row_number) to a DynamicFrame.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param partition_by: List of columns to partition by.
        :param order_by: Column to order by.
        :param function: Window function to apply (e.g., "rank", "row_number").
        :return: DynamicFrame with the window function applied.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            window_spec = Window.partitionBy(*partition_by).orderBy(order_by)

            if function == "rank":
                df = df.withColumn("rank", rank().over(window_spec))
            elif function == "row_number":
                df = df.withColumn("row_number", row_number().over(window_spec))
            else:
                raise ValueError(f"Unsupported window function: {function}")

            return self.dataframe_to_dynamicframe(df, "window_function_applied")
        except Exception as e:
            raise ValueError(f"Error applying window function: {e}")


    def encode_categorical_columns(self, dynamic_frame: DynamicFrame, columns: list) -> DynamicFrame:
        """
        Convert categorical columns to numeric representations.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param columns: List of categorical columns to encode.
        :return: DynamicFrame with categorical columns encoded.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for column in columns:
                indexer = StringIndexer(inputCol=column, outputCol=f"{column}_index")
                df = indexer.fit(df).transform(df)
            return self.dataframe_to_dynamicframe(df, "encoded_categorical_columns")
        except Exception as e:
            raise ValueError(f"Error encoding categorical columns: {e}")



    def impute_missing_values(self, dynamic_frame: DynamicFrame, columns: list, method: str = "mean") -> DynamicFrame:
        """
        Impute missing values in specified columns using a statistical method.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param columns: List of columns to impute missing values.
        :param method: Method to use for imputation (e.g., "mean", "median").
        :return: DynamicFrame with missing values imputed.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            for column in columns:
                if method == "mean":
                    mean_value = df.select(mean(col(column))).collect()[0][0]
                    df = df.fillna({column: mean_value})
                else:
                    raise ValueError(f"Unsupported imputation method: {method}")
            return self.dataframe_to_dynamicframe(df, "imputed_missing_values")
        except Exception as e:
            raise ValueError(f"Error imputing missing values: {e}")
        


    def bucketize_column(self, dynamic_frame: DynamicFrame, column: str, splits: list, output_column: str) -> DynamicFrame:
        """
        Bucketize a numerical column into bins.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param column: Column to bucketize.
        :param splits: List of split points defining the bins.
        :param output_column: Name for the output bucketized column.
        :return: DynamicFrame with the bucketized column.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            bucketizer = Bucketizer(splits=splits, inputCol=column, outputCol=output_column)
            df = bucketizer.transform(df)
            return self.dataframe_to_dynamicframe(df, "bucketized_column")
        except Exception as e:
            raise ValueError(f"Error bucketizing column: {e}")
        


    def concatenate_columns(self, dynamic_frame: DynamicFrame, columns: list, output_column: str, separator: str = " ") -> DynamicFrame:
        """
        Concatenate multiple columns into a single column.
        
        :param dynamic_frame: Glue DynamicFrame to transform.
        :param columns: List of columns to concatenate.
        :param output_column: Name of the new concatenated column.
        :param separator: Separator to use between concatenated values.
        :return: DynamicFrame with concatenated columns.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            df = df.withColumn(output_column, concat_ws(separator, *[col(column) for column in columns]))
            return self.dataframe_to_dynamicframe(df, "concatenated_columns")
        except Exception as e:
            raise ValueError(f"Error concatenating columns: {e}")
        
    def validate_schema(self, dynamic_frame: DynamicFrame, expected_schema: dict) -> bool:
        """
        Validate if the schema matches an expected structure.
        
        :param dynamic_frame: Glue DynamicFrame to validate.
        :param expected_schema: Dictionary with column names as keys and expected types as values.
        :return: Boolean indicating if schema matches expected schema.
        """
        try:
            df = self.dynamicframe_to_dataframe(dynamic_frame)
            actual_schema = {field.name: field.dataType for field in df.schema.fields}
            for column, data_type in expected_schema.items():
                if column not in actual_schema or str(actual_schema[column]) != str(data_type):
                    return False
            return True
        except Exception as e:
            raise ValueError(f"Error validating schema: {e}")










