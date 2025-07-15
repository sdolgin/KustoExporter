---
# Kusto Export Scripts

This repo contains Python scripts for exporting Kusto (Azure Data Explorer) table and function schemas to individual KQL files.

## ✅ Enhanced Features

- **Color-coded Logging**: Visual feedback for status, progress, warnings, and errors
- **Robust Error Handling**: Comprehensive exception handling with stack traces
- **KQL Validation**: Syntax validation for generated KQL commands
- **Automatic README Generation**: Each export creates a detailed README.md in the output directory
- **Detailed Progress Tracking**: Real-time feedback on processing status

---

## 📁 File Overview

### Core Scripts

- `export-table-schemas.py` — Exports table CREATE commands (uses shared utilities)
- `export-function-schemas.py` — Exports function CREATE-OR-ALTER commands (uses shared utilities)
- `kusto_export_utils.py` — Shared utilities module

### Utilities

- `run_export_cluster_objects.bat` — Batch script to run exports

---

## 🏗️ Architecture & Extensibility

### Shared Utilities (`kusto_export_utils.py`)

```python
# Base classes and utilities
├── Colors            # ANSI color codes for console output
├── Logger            # Centralized logging with color-coded messages
├── KustoExporter     # Base class with common export functionality
├── ReadmeGenerator   # Consistent README.md generation
└── ExportSummary     # Standardized summary reporting
```

### Table Exporter (`export-table-schemas.py`)

```python
# Inherits from KustoExporter
├── TableExporter
    ├── get_table_create_commands()    # Fetch and format CREATE TABLE commands
    ├── _format_schema_definition()    # Ensure proper KQL syntax formatting
    ├── export_tables()                # Main export orchestration
    └── _export_table_files()          # File writing logic
```

### Function Exporter (`export-function-schemas.py`)

```python
# Inherits from KustoExporter
├── FunctionExporter
    ├── get_functions()                # Fetch function list
    ├── get_function_details()         # Extract function metadata
    ├── parse_parameters_field()       # Parse function parameters
    ├── validate_kql_function()        # KQL syntax validation
    ├── build_function_kql()           # Generate complete KQL function
    ├── export_functions()             # Main export orchestration
    └── _export_function_files()       # File writing logic
```

---

## 🚀 Usage

### Export Table Schemas

```powershell
python export-table-schemas.py -c "https://cluster.kusto.windows.net" -d "DatabaseName" -o "table_exports"
```

### Export Function Schemas

```powershell
python export-function-schemas.py -c "https://cluster.kusto.windows.net" -d "DatabaseName" -o "function_exports"
```

#### Parameters

- `-c, --cluster`: Kusto cluster URL (required)
- `-d, --database`: Database name (required)
- `-o, --output-dir`: Output directory name (optional)

---

## 📦 Output Structure

Each export creates a dedicated directory with:

```text
output_directory/
├── README.md              # Auto-generated documentation
├── ObjectName1.kql        # Individual KQL files
├── ObjectName2.kql
└── ...
```

### Generated KQL Examples

**Table Export:**

```kql
// CREATE TABLE command for MyTable
// Generated on 2025-01-14 15:30:25

.create table MyTable (
    Column1: string,
    Column2: datetime,
    Column3: real
)
```

**Function Export:**

```kql
// CREATE-OR-ALTER FUNCTION command for MyFunction
// Generated on 2025-01-14 15:30:25

.create-or-alter function with (docstring = "Function description", folder = "Analytics") 
MyFunction(param1: string, param2: int = 10) {
    MyTable
    | where Column1 == param1
    | take param2
}
```

---

## 🔧 Technical Details

### Authentication

- Uses Azure CLI authentication (`az login` required)
- Leverages `KustoConnectionStringBuilder.with_az_cli_authentication()`

### Dependencies

```text
azure-kusto-data
argparse (built-in)
re (built-in)
os (built-in)
datetime (built-in)
traceback (built-in)
```

### Error Handling

- **Kusto API Errors**: Handles connection and query issues
- **Schema Validation**: Ensures proper KQL syntax before export
- **File System Errors**: Graceful handling of directory creation and file writing issues
- **Progress Tracking**: Real-time feedback with colored console output

---

## 🔮 Extending the Exporter

The modular architecture makes it easy to add new exporters:

```python
# Example: New Policy Exporter
class PolicyExporter(KustoExporter):
    def export_policies(self):
        # Inherits all base functionality
        # Just implement policy-specific logic
        pass
```

---

## 🐛 Troubleshooting

### Common Issues

1. **Authentication Failed**: Run `az login` and ensure you have access to the cluster
2. **Empty Schema**: Check that tables/functions exist in the specified database
3. **Permission Denied**: Ensure you have read permissions on the cluster/database
4. **File Write Errors**: Check that the output directory is writable

### Debug Mode

Both scripts provide detailed logging. Look for:

- **Blue [INFO]**: General information
- **Cyan [PROGRESS]**: Operation progress
- **Green [SUCCESS]**: Successful operations
- **Yellow [WARNING]**: Non-fatal issues
- **Red [ERROR]**: Fatal errors

