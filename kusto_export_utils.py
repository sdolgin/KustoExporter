"""
Shared utilities for Kusto export scripts.
This module contains common functionality used by both table and function export scripts.
"""

import os
from datetime import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoApiError


# ANSI color codes for console output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Logger:
    """Centralized logging with color-coded output"""
    
    @staticmethod
    def info(message):
        print(f"{Colors.OKBLUE}[INFO]{Colors.ENDC} {message}")

    @staticmethod
    def success(message):
        print(f"{Colors.OKGREEN}[SUCCESS]{Colors.ENDC} {message}")

    @staticmethod
    def warning(message):
        print(f"{Colors.WARNING}[WARNING]{Colors.ENDC} {message}")

    @staticmethod
    def error(message):
        print(f"{Colors.FAIL}[ERROR]{Colors.ENDC} {message}")

    @staticmethod
    def header(message):
        print(f"{Colors.HEADER}{Colors.BOLD}{message}{Colors.ENDC}")

    @staticmethod
    def progress(message):
        print(f"{Colors.OKCYAN}[PROGRESS]{Colors.ENDC} {message}")

    @staticmethod
    def detail(message):
        print(f"  {Colors.OKCYAN}→{Colors.ENDC} {message}")


class KustoExporter:
    """Base class for Kusto exporters with common functionality"""
    
    def __init__(self, cluster, database, output_dir):
        self.cluster = cluster
        self.database = database
        self.output_dir = output_dir
        self.client = None
        self.logger = Logger()
    
    def authenticate(self):
        """Authenticate with Azure CLI and create Kusto client"""
        self.logger.progress("Authenticating with Azure CLI...")
        try:
            kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(self.cluster)
            self.client = KustoClient(kcsb)
            self.logger.success("Successfully authenticated with Azure CLI")
            return True
        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
    
    def create_output_directory(self):
        """Create the output directory if it doesn't exist"""
        self.logger.progress(f"Creating output directory: {self.output_dir}")
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            self.logger.success(f"Output directory ready: {self.output_dir}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create output directory: {e}")
            return False
    
    def write_file(self, filename, content, description="file"):
        """Write content to a file with error handling"""
        filepath = os.path.join(self.output_dir, filename)
        self.logger.progress(f"Writing {description}: {filename}")
        
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            self.logger.success(f"Successfully exported: {filename}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write {description} '{filename}': {e}")
            return False
    
    def generate_file_header(self, object_name, object_type="object"):
        """Generate a standard header comment for exported files"""
        return (
            f"// {object_type.upper()} command for {object_name}\n"
            f"// Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        )


class ReadmeGenerator:
    """Generate README.md files for export output directories"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def generate_readme(self, output_dir, cluster, database, script_name, 
                       object_type, exported_count, failed_count, object_names,
                       additional_sections=None):
        """
        Generate a comprehensive README.md file
        
        Args:
            output_dir: Output directory path
            cluster: Kusto cluster URL
            database: Database name
            script_name: Name of the generator script
            object_type: Type of objects (e.g., "table", "function")
            exported_count: Number of successfully exported objects
            failed_count: Number of failed exports
            object_names: List of exported object names
            additional_sections: Optional dict of additional sections {title: content}
        """
        readme_path = os.path.join(output_dir, "README.md")
        
        try:
            with open(readme_path, "w", encoding="utf-8") as f:
                # Header
                f.write(f"# Auto-Generated KQL {object_type.title()} Schemas\n\n")
                f.write("⚠️ **WARNING: These files are automatically generated. Do not manually edit them.**\n\n")
                
                # Generation Information
                f.write("## Generation Information\n\n")
                f.write(f"- **Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"- **Source Cluster:** {cluster}\n")
                f.write(f"- **Source Database:** {database}\n")
                f.write(f"- **Generator Script:** {script_name}\n\n")
                
                # Summary
                f.write("## Summary\n\n")
                f.write(f"- **Total {object_type}s exported:** {exported_count}\n")
                if failed_count > 0:
                    f.write(f"- **Failed exports:** {failed_count}\n")
                f.write(f"- **Total files generated:** {exported_count + 1} (including this README)\n\n")
                
                # Exported Objects
                if object_names:
                    f.write(f"## Exported {object_type.title()}s\n\n")
                    if object_type == "table":
                        f.write("The following table CREATE commands were successfully exported:\n\n")
                        for obj in sorted(object_names):
                            f.write(f"- `{obj}.kql` - CREATE TABLE command for `{obj}`\n")
                    elif object_type == "function":
                        f.write("The following function CREATE commands were successfully exported:\n\n")
                        for obj in sorted(object_names):
                            f.write(f"- `{obj}.kql` - CREATE-OR-ALTER FUNCTION command for `{obj}()`\n")
                    f.write("\n")
                
                # Usage section
                self._write_usage_section(f, object_type)
                
                # Additional sections
                if additional_sections:
                    for title, content in additional_sections.items():
                        f.write(f"## {title}\n\n")
                        f.write(f"{content}\n\n")
                
                # Regeneration
                f.write("## Regeneration\n\n")
                f.write(f"To regenerate these files with updated {object_type} definitions, run:\n")
                f.write("```bash\n")
                f.write(f'python {script_name} -c "{cluster}" -d "{database}" -o "{os.path.basename(output_dir)}"\n')
                f.write("```\n")
            
            self.logger.success("Generated README.md in output directory")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to generate README.md: {e}")
            return False
    
    def _write_usage_section(self, f, object_type):
        """Write the usage section specific to the object type"""
        f.write("## Usage\n\n")
        
        if object_type == "table":
            f.write("Each `.kql` file contains a complete CREATE TABLE command that can be executed in Kusto/Azure Data Explorer to recreate the table structure.\n\n")
            f.write("Example usage:\n")
            f.write("```kql\n")
            f.write("// Execute the content of any .kql file to create the table\n")
            f.write(".create table MyTable (\n")
            f.write("    Column1: string,\n")
            f.write("    Column2: datetime,\n")
            f.write("    Column3: real\n")
            f.write(")\n")
            f.write("```\n\n")
        
        elif object_type == "function":
            f.write("Each `.kql` file contains a complete `.create-or-alter function` command that can be executed in Kusto/Azure Data Explorer to recreate the function.\n\n")
            f.write("Example usage:\n")
            f.write("```kql\n")
            f.write("// Execute the content of any .kql file to create/update the function\n")
            f.write(".create-or-alter function MyFunction(param1: string, param2: int) {\n")
            f.write("    // Function body here\n")
            f.write("    MyTable\n")
            f.write("    | where Column1 == param1\n")
            f.write("    | take param2\n")
            f.write("}\n")
            f.write("```\n\n")


class ExportSummary:
    """Handle export summary and final reporting"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def print_summary(self, object_type, total_found, exported_count, failed_count, output_dir):
        """Print a comprehensive export summary"""
        self.logger.header("=== Export Summary ===")
        self.logger.info(f"Total {object_type}s found: {total_found}")
        self.logger.success(f"Successfully exported: {exported_count}")
        if failed_count > 0:
            self.logger.error(f"Failed to export: {failed_count}")
        self.logger.info(f"Output directory: {output_dir}")
        
        if exported_count > 0:
            self.logger.header("Export completed successfully!")
            return 0
        else:
            self.logger.error(f"No {object_type}s were exported")
            return 1
