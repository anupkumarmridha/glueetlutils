# **Contributing to GlueETLUtils**

Thank you for considering contributing to **GlueETLUtils**! We welcome all contributions, whether it's bug fixes, new features, documentation improvements, or any other enhancements.

---

## **Getting Started**
To get started with contributing, please follow these steps:

### **1. Fork the Repository**
1. Click the "Fork" button on the repository's GitHub page.
2. Clone your forked repository to your local machine:
   ```bash
   git clone https://github.com/<your-username>/glueetlutils.git
   cd glueetlutils
   ```

### **2. Set Up the Development Environment**
1. Create a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
2. Install the package in editable mode:
   ```bash
   pip install -e .
   ```
3. Install additional development dependencies:
   ```bash
   pip install pytest twine
   ```

---

## **Guidelines for Contributions**

### **1. Code Changes**
- Ensure your changes are backward-compatible with existing functionality.
- Add relevant unit tests for new features or bug fixes in the `tests/` directory.
- Run tests before submitting a pull request:
  ```bash
  pytest tests/
  ```

### **2. Update the `setup.py`**
If your changes introduce significant new functionality or fixes, update the version number in the `setup.py` file. Follow semantic versioning:
- Increment **PATCH** for bug fixes: `x.y.(z+1)`
- Increment **MINOR** for backward-compatible new features: `x.(y+1).z`
- Increment **MAJOR** for backward-incompatible changes: `(x+1).y.z`

Example:
```python
version="0.1.2"  # Update this line in setup.py
```

### **3. Update the Documentation**
If you add new features, update the **`README.md`** file and any other relevant documentation files (e.g., `CONTRIBUTING.md`, `docs/`).

<!-- ### **4. Writing Tests**
1. Write tests for your new functionality in the `tests/` directory.
2. Ensure your test names are descriptive and follow the pattern `test_<functionality>`. -->

---

## **Submitting Your Contribution**

1. Commit your changes with a meaningful commit message:
   ```bash
   git add .
   git commit -m "Add feature: <describe your feature or fix>"
   ```
2. Push your changes to your forked repository:
   ```bash
   git push origin <branch-name>
   ```
3. Open a pull request (PR) to the main repository:
   - Provide a clear description of your changes.
   - Reference any related issues or features.

---

## **Code Style**
- Follow Python's [PEP 8](https://peps.python.org/pep-0008/) guidelines.
- Use type hints where appropriate.
- Format your code using tools like `black`:
  ```bash
  pip install black
  black glueetlutils/
  ```

---

## **Releasing a New Version**
1. After merging changes into the main branch, update the version number in `setup.py`.
2. Build the package:
   ```bash
   python setup.py sdist bdist_wheel
   ```
3. Publish the package to PyPI or AWS CodeArtifact:
   ```bash
   twine upload dist/*
   ```

---

## **Feedback and Issues**
- If you encounter any issues while contributing, feel free to open an issue on the GitHub repository.
- For questions or suggestions, contact us at [anupkumarmridha.net@gmail.com](mailto:anupkumarmridha.net@gmail.com).

---

## **Acknowledgements**
We appreciate all contributions and strive to acknowledge contributors in the **README.md** file. Thank you for your support!

---

This **`CONTRIBUTING.md`** file ensures a consistent contribution process, encouraging collaboration while maintaining the project's quality and structure.