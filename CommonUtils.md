# **Documentation: Using `CommonUtils` from `glueetlutils`**

## **1. Overview**
The `CommonUtils` class in the `glueetlutils` package provides reusable utility functions. One key function is `get_secret`, which retrieves secrets from AWS Secrets Manager.

This utility is designed to:
- Simplify interaction with AWS Secrets Manager.
- Provide consistent logging and timing for secret retrieval.
- Handle exceptions gracefully and raise errors for further debugging.

---

## **2. Installation**
Ensure the `glueetlutils` package is installed in your environment. If using a local version:
```bash
pip install -e .
```

---

## **3. Imports**

### **3.1 Importing `CommonUtils` Directly**
You can directly import `CommonUtils` for simplicity:
```python
from glueetlutils import CommonUtils
```

### **3.2 Importing from the Core Module**
For more explicit imports, you can import directly from the `core` module:
```python
from glueetlutils.core.common import CommonUtils
```

### **3.3 Importing Alongside Other Utilities**
To use multiple utilities from `glueetlutils`, you can import them together:
```python
from glueetlutils import CommonUtils, LoadUtils, QualityUtils
```

### **3.4 Importing with Logger and Timing**
If you also need the `logger` and `log_time` decorators:
```python
from glueetlutils import CommonUtils, logger, log_time
```

---

## **4. Examples of Usage**

### **4.1 Basic Usage: Retrieving Secrets**
The `get_secret` function retrieves secrets from AWS Secrets Manager and returns them as a dictionary.

```python
from glueetlutils import CommonUtils

# Retrieve a secret
secret_name = "my-secret-name"
region_name = "us-east-1"

try:
    secret = CommonUtils.get_secret(secret_name, region_name)
    print("Retrieved secret:", secret)
except Exception as e:
    print(f"Failed to retrieve secret: {e}")
```

---

### **4.2 Using with Logger**
`get_secret` automatically logs the start, completion, and any errors during execution.

Example log output:
```plaintext
2024-11-30 10:00:00 - glueetlutils - INFO - Starting function: get_secret
2024-11-30 10:00:00 - glueetlutils - INFO - Loading secrets from Secrets Manager...
2024-11-30 10:00:01 - glueetlutils - INFO - Function get_secret completed in 1.12 seconds.
```

In case of an error:
```plaintext
2024-11-30 10:00:01 - glueetlutils - ERROR - An error occurred while loading secrets: An error occurred (AccessDeniedException)
```

---

### **4.3 Using with Timing**
The `@log_time` decorator automatically measures and logs the execution time of `get_secret`.

```python
from glueetlutils import CommonUtils

secret_name = "my-secret-name"
region_name = "us-east-1"

# Retrieve the secret and measure execution time
try:
    secret = CommonUtils.get_secret(secret_name, region_name)
    print("Secret retrieved successfully.")
except Exception as e:
    print(f"Failed to retrieve secret: {e}")
```

Example log output:
```plaintext
2024-11-30 10:00:00 - glueetlutils - INFO - Starting function: get_secret
2024-11-30 10:00:00 - glueetlutils - INFO - Loading secrets from Secrets Manager...
2024-11-30 10:00:01 - glueetlutils - INFO - Function get_secret completed in 1.05 seconds.
```

---

### **4.4 Handling Exceptions**
The `get_secret` method raises exceptions for unrecoverable errors. You can handle them explicitly in your Glue job or application.

```python
from glueetlutils import CommonUtils

secret_name = "nonexistent-secret"
region_name = "us-east-1"

try:
    secret = CommonUtils.get_secret(secret_name, region_name)
except ClientError as e:
    print(f"AWS ClientError: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

---

### **4.5 Using with Multiple Utilities**
Combine `CommonUtils` with other utilities like `LoadUtils` and `QualityUtils` for more complex workflows.

```python
from glueetlutils import CommonUtils, LoadUtils

# Retrieve a database credential from Secrets Manager
secret_name = "database-credentials"
region_name = "us-east-1"
secret = CommonUtils.get_secret(secret_name, region_name)

# Use the secret to load data from S3
load_utils = LoadUtils(glue_context)
s3_path = "s3://your-bucket/data/"
data = load_utils.load_data_from_s3(s3_path, "s3_load_context")

print("Data loaded successfully.")
```

---

## **5. Best Practices**

### **5.1 Secrets Storage**
- Ensure sensitive secrets like database credentials are stored securely in AWS Secrets Manager.
- Use resource-based policies to restrict access to secrets.

### **5.2 IAM Permissions**
- The AWS Glue job or user accessing the `get_secret` function must have `secretsmanager:GetSecretValue` permissions for the secret.

### **5.3 Error Handling**
- Handle `ClientError` explicitly to identify permission issues or nonexistent secrets.

### **5.4 Logging**
- Use the built-in `logger` for consistent logging and debugging.

---

## **6. Summary of `get_secret`**

### **Function Signature**
```python
@staticmethod
@log_time
def get_secret(secret_name: str, region_name: str) -> dict:
    """
    Retrieve secrets from AWS Secrets Manager.

    :param secret_name: The name of the secret in Secrets Manager.
    :param region_name: The AWS region of the Secrets Manager.
    :return: The secret as a dictionary.
    """
```

### **Key Features**
1. **AWS Secrets Manager Integration**:
   - Fetches secrets using `boto3` with `secretsmanager:GetSecretValue`.

2. **Automatic Logging**:
   - Logs start, success, and errors with contextual information.

3. **Execution Timing**:
   - Measures and logs execution time using the `@log_time` decorator.

4. **Error Handling**:
   - Raises exceptions for permission issues or other AWS Secrets Manager errors.

---

## **7. Example Logs**

### Successful Retrieval:
```plaintext
2024-11-30 10:00:00 - glueetlutils - INFO - Starting function: get_secret
2024-11-30 10:00:00 - glueetlutils - INFO - Loading secrets from Secrets Manager...
2024-11-30 10:00:01 - glueetlutils - INFO - Function get_secret completed in 1.15 seconds.
```

### Error Case:
```plaintext
2024-11-30 10:00:00 - glueetlutils - INFO - Starting function: get_secret
2024-11-30 10:00:00 - glueetlutils - INFO - Loading secrets from Secrets Manager...
2024-11-30 10:00:01 - glueetlutils - ERROR - An error occurred while loading secrets: An error occurred (ResourceNotFoundException) when calling the GetSecretValue operation: Secret not found.
```

---

## **8. Conclusion**
The `CommonUtils.get_secret` method simplifies secret management for Glue jobs and other applications by providing:
- Robust integration with AWS Secrets Manager.
- Consistent logging and timing for troubleshooting.
- Clear error handling for unexpected scenarios.

This utility is ideal for managing sensitive credentials and securely accessing them in Glue jobs or other Python-based workflows.