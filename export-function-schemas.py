import argparse
import re
import traceback
from azure.kusto.data.exceptions import KustoApiError
from kusto_export_utils import KustoExporter, ReadmeGenerator, ExportSummary


class FunctionExporter(KustoExporter):
    """Specialized exporter for Kusto function schemas"""
    
    def __init__(self, cluster, database, output_dir):
        super().__init__(cluster, database, output_dir)
        self.readme_generator = ReadmeGenerator(self.logger)
        self.summary = ExportSummary(self.logger)
    
    def get_functions(self):
        """Get list of functions from the database"""
        self.logger.progress("Fetching function list from database...")
        try:
            result = self.client.execute_mgmt(self.database, ".show functions")
            functions = [row["Name"] for row in result.primary_results[0]]
            self.logger.success(f"Found {len(functions)} functions in database '{self.database}'")
            return functions
        except KustoApiError as e:
            self.logger.error(f"Failed to fetch function list: {e}")
            raise
    
    def get_function_details(self, func_name):
        """Get detailed function information including metadata"""
        self.logger.detail(f"Fetching details for function: {func_name}")
        cmd = f'.show function ["{func_name}"]'
        try:
            res = self.client.execute_mgmt(self.database, cmd)
            if res.primary_results[0]:
                row = res.primary_results[0][0]
                
                # Try to_dict() method first
                try:
                    func_dict = row.to_dict()
                    self.logger.detail(f"Successfully converted {func_name} to dict with keys: {list(func_dict.keys())}")
                    return func_dict
                except Exception as e:
                    self.logger.warning(f"to_dict() failed for {func_name}: {e}")
                
                # Fallback: Use indexed access
                try:
                    columns = [col.column_name for col in res.primary_results[0].columns]
                    func_dict = {}
                    for i, col_name in enumerate(columns):
                        if i < len(row):
                            func_dict[col_name] = row[i]
                    self.logger.detail(f"Successfully created dict for {func_name} using indexed access: {list(func_dict.keys())}")
                    return func_dict
                except Exception as e:
                    self.logger.error(f"Indexed access failed for {func_name}: {e}")
                
                self.logger.error(f"All methods failed for {func_name}")
                return None
            else:
                self.logger.warning(f"No results returned for function {func_name}")
                return None
        except KustoApiError as e:
            self.logger.error(f"API error getting details for {func_name}: {e}")
            raise
    
    def extract_function_signature(self, func_body, func_name):
        """Extract function parameters from the body text"""
        # Remove any leading comments or whitespace
        cleaned_body = re.sub(r'^(\s*//.*\n)*\s*', '', func_body, flags=re.MULTILINE)
        
        # Look for the function definition pattern
        pattern = rf'{re.escape(func_name)}\s*\(([^)]*)\)'
        match = re.search(pattern, cleaned_body, re.DOTALL)
        
        if match:
            params = match.group(1).strip()
            return params if params else ""
        
        # Fallback: Look for any parentheses at the beginning
        paren_pattern = r'^\s*[^(]*\(([^)]*)\)'
        paren_match = re.search(paren_pattern, cleaned_body, re.MULTILINE)
        
        if paren_match:
            params = paren_match.group(1).strip()
            return params if params else ""
        
        return ""
    
    def parse_parameters_field(self, parameters_str):
        """Parse the Parameters field which contains the parameter signature"""
        if not parameters_str or parameters_str == "":
            return ""
        
        start_idx = parameters_str.find('(')
        if start_idx == -1:
            return parameters_str.strip()
        
        # Find the matching closing parenthesis
        paren_count = 0
        for i in range(start_idx, len(parameters_str)):
            if parameters_str[i] == '(':
                paren_count += 1
            elif parameters_str[i] == ')':
                paren_count -= 1
                if paren_count == 0:
                    return parameters_str[start_idx + 1:i].strip()
        
        return parameters_str[start_idx + 1:].strip()
    
    def validate_kql_function(self, kql_content, func_name):
        """Basic validation of the generated KQL function"""
        if not kql_content.strip().startswith(".create-or-alter function"):
            return False, "KQL doesn't start with .create-or-alter function"
        
        if func_name not in kql_content:
            return False, f"Function name {func_name} not found in KQL"
        
        open_braces = kql_content.count('{')
        close_braces = kql_content.count('}')
        if open_braces != close_braces:
            return False, f"Unbalanced braces: {open_braces} open, {close_braces} close"
        
        if not kql_content.strip().endswith('}'):
            return False, "KQL doesn't end with closing brace"
        
        return True, "Valid"
    
    def build_function_kql(self, func_details):
        """Build complete KQL function from function details"""
        func_name = func_details.get("Name")
        func_body = func_details.get("Body")
        doc_string = func_details.get("DocString", "")
        folder = func_details.get("Folder", "")
        parameters_field = func_details.get("Parameters", "")
        
        if not func_name or not func_body:
            return None, "Missing required function data"
        
        # Get parameters
        parameters = ""
        try:
            if parameters_field:
                parameters = self.parse_parameters_field(parameters_field)
                self.logger.detail(f"Found Parameters field: {parameters_field}")
                self.logger.detail(f"Parsed parameters: {parameters}")
            else:
                self.logger.detail("No Parameters field found, extracting from body...")
                parameters = self.extract_function_signature(func_body, func_name)
                self.logger.detail(f"Extracted from body: {parameters}")
        except Exception as e:
            self.logger.warning(f"Error processing parameters: {e}")
            parameters = self.extract_function_signature(func_body, func_name)
            self.logger.detail(f"Fallback extraction from body: {parameters}")
        
        # Clean up parameters
        if parameters:
            parameters = re.sub(r'\s*,\s*', ',', parameters.strip())
            parameters = re.sub(r'\s+', ' ', parameters)
        
        # Log metadata details
        if doc_string:
            self.logger.detail(f"DocString: {doc_string[:100]}{'...' if len(doc_string) > 100 else ''}")
        if folder:
            self.logger.detail(f"Folder: {folder}")
        self.logger.detail(f"Final parameters: {parameters}")
        
        # Build the complete .create-or-alter function command
        create_cmd = ".create-or-alter function"
        
        # Add with clause for metadata
        with_clauses = []
        if doc_string:
            escaped_docstring = doc_string.replace('"', '\\"').replace('\n', '\\n').replace('\r', '')
            with_clauses.append(f'docstring = "{escaped_docstring}"')
        if folder:
            with_clauses.append(f'folder = "{folder}"')
        
        if with_clauses:
            create_cmd += f" with ({','.join(with_clauses)})"
        
        # Add function name and parameters
        if parameters:
            create_cmd += f" {func_name}({parameters}) {{\n"
        else:
            create_cmd += f" {func_name}() {{\n"
        
        # Add the function body content
        function_body_content = func_body.strip()
        if function_body_content:
            # Check if the body already starts and ends with braces
            if function_body_content.startswith('{') and function_body_content.endswith('}'):
                # Remove outer braces
                inner_body = function_body_content[1:-1].strip()
                create_cmd += inner_body
            else:
                create_cmd += function_body_content
        
        # Close the function
        create_cmd += "\n}"
        
        return create_cmd, None
    
    def export_functions(self):
        """Main export process for functions"""
        self.logger.header("=== Kusto Function Schema Exporter ===")
        
        # Setup and authentication
        if not self.authenticate():
            return 1
        
        if not self.create_output_directory():
            return 1
        
        # Get functions
        try:
            funcs = self.get_functions()
            if funcs:
                self.logger.info(f"Functions to export: {', '.join(funcs)}")
            else:
                self.logger.warning("No functions found in database")
                return 1
        except KustoApiError as e:
            self.logger.error(f"Error getting functions: {e}")
            return 1
        
        # Export individual function files
        exported_count, failed_count, exported_functions = self._export_function_files(funcs)
        
        # Generate README
        self.logger.progress("Generating README.md...")
        additional_sections = {
            "Function Metadata": (
                "The exported functions include:\n"
                "- Function parameters with types and default values\n"
                "- Documentation strings (docstring)\n"
                "- Folder organization information\n"
                "- Complete function body"
            )
        }
        self.readme_generator.generate_readme(
            self.output_dir, self.cluster, self.database, "export-function-schemas.py",
            "function", exported_count, failed_count, exported_functions,
            additional_sections
        )
        
        # Print summary and return exit code
        return self.summary.print_summary("function", len(funcs), exported_count, 
                                         failed_count, self.output_dir)
    
    def _export_function_files(self, funcs):
        """Export individual KQL files for each function"""
        self.logger.header("Processing functions...")
        exported_count = 0
        failed_count = 0
        exported_functions = []
        
        for i, func in enumerate(funcs, 1):
            try:
                self.logger.progress(f"Processing function {i}/{len(funcs)}: {func}")
                func_details = self.get_function_details(func)
                
                if func_details:
                    # Extract function metadata
                    try:
                        func_name = func_details.get("Name")
                        func_body = func_details.get("Body")
                        
                        self.logger.detail(f"Function name: {func_name}")
                        self.logger.detail(f"Body length: {len(func_body) if func_body else 0} characters")
                        
                        if not func_name or not func_body:
                            self.logger.warning(f"Missing required data for function {func}")
                            failed_count += 1
                            continue
                        
                        # Build KQL content
                        kql_content, error = self.build_function_kql(func_details)
                        if error:
                            self.logger.error(f"Error building KQL for function {func_name}: {error}")
                            failed_count += 1
                            continue
                        
                        # Validate the generated KQL function
                        is_valid, validation_message = self.validate_kql_function(kql_content, func_name)
                        if not is_valid:
                            self.logger.error(f"Validation error for function {func_name}: {validation_message}")
                            failed_count += 1
                            continue
                        
                        # Write the file
                        if self.write_file(f"{func_name}.kql", kql_content, f"function {func_name}"):
                            exported_count += 1
                            exported_functions.append(func_name)
                        else:
                            failed_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error extracting function metadata for {func}: {e}")
                        failed_count += 1
                        continue
                else:
                    self.logger.warning(f"No details found for function {func}")
                    failed_count += 1
                    
            except KustoApiError as e:
                self.logger.error(f"Error getting details for function {func}: {e}")
                failed_count += 1
                continue
            except Exception as e:
                self.logger.error(f"Unexpected error processing function {func}: {e}")
                failed_count += 1
                self.logger.detail(f"Stack trace: {traceback.format_exc()}")
                continue
        
        return exported_count, failed_count, exported_functions


def main():
    parser = argparse.ArgumentParser(description="Export Kusto functions to individual KQL files")
    parser.add_argument("-c", "--cluster", required=True, help="Cluster URL (e.g. https://<name>.kusto.windows.net)")
    parser.add_argument("-d", "--database", required=True, help="Database name")
    parser.add_argument("-o", "--output-dir", default="function_schemas", help="Directory for function KQL files")
    args = parser.parse_args()

    # Create exporter and run export
    exporter = FunctionExporter(args.cluster, args.database, args.output_dir)
    
    # Log configuration
    exporter.logger.info(f"Cluster: {args.cluster}")
    exporter.logger.info(f"Database: {args.database}")
    exporter.logger.info(f"Output directory: {args.output_dir}")
    
    return exporter.export_functions()


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
