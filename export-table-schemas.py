import argparse
import re
from azure.kusto.data.exceptions import KustoApiError
from kusto_export_utils import KustoExporter, ReadmeGenerator, ExportSummary


class TableExporter(KustoExporter):
    """Specialized exporter for Kusto table schemas"""
    
    def __init__(self, cluster, database, output_dir):
        super().__init__(cluster, database, output_dir)
        self.readme_generator = ReadmeGenerator(self.logger)
        self.summary = ExportSummary(self.logger)
    
    def get_table_create_commands(self):
        """Fetch all tables and generate their CREATE commands"""
        self.logger.progress("Fetching table list from database...")
        try:
            tables_result = self.client.execute_mgmt(self.database, ".show tables")
            table_count = len(tables_result.primary_results[0])
            self.logger.success(f"Found {table_count} tables in database '{self.database}'")
        except KustoApiError as e:
            self.logger.error(f"Failed to fetch table list: {e}")
            raise
        
        create_commands = {}
        failed_tables = []
        
        for i, row in enumerate(tables_result.primary_results[0], 1):
            table = row["TableName"]
            self.logger.progress(f"Processing table {i}/{table_count}: {table}")
            
            try:
                # Get the table schema in a format suitable for CREATE TABLE command
                schema_res = self.client.execute_mgmt(self.database, f".show table {table} cslschema")
                schema_definition = schema_res.primary_results[0][0]["Schema"]
                
                if not schema_definition:
                    self.logger.warning(f"Table '{table}' has empty schema definition")
                    failed_tables.append(table)
                    continue
                
                # Ensure the schema definition is properly formatted with parentheses
                schema_definition = self._format_schema_definition(schema_definition)
                    
                # Construct the complete CREATE TABLE command
                create_command = f".create table {table} {schema_definition}"
                create_commands[table] = create_command
                self.logger.success(f"Successfully processed table '{table}'")
                
            except KustoApiError as e:
                self.logger.error(f"Failed to get schema for table '{table}': {e}")
                failed_tables.append(table)
            except Exception as e:
                self.logger.error(f"Unexpected error processing table '{table}': {e}")
                failed_tables.append(table)
        
        if failed_tables:
            self.logger.warning(f"Failed to process {len(failed_tables)} tables: {', '.join(failed_tables)}")
        
        self.logger.success(f"Successfully processed {len(create_commands)} out of {table_count} tables")
        return create_commands, len(failed_tables)
    
    def _format_schema_definition(self, schema_definition):
        """Format schema definition with proper parentheses and spacing"""
        # The cslschema command returns format like: (col1:type1, col2:type2, ...)
        # But sometimes it might not include the outer parentheses
        schema_definition = schema_definition.strip()
        if not schema_definition.startswith('('):
            schema_definition = f"({schema_definition})"
        
        # Also ensure proper spacing after colons
        schema_definition = re.sub(r'(\w+):(\w+)', r'\1: \2', schema_definition)
        return schema_definition
    
    def export_tables(self):
        """Main export process for tables"""
        self.logger.header("=== Kusto Table Schema Exporter ===")
        
        # Setup and authentication
        if not self.authenticate():
            return 1
        
        if not self.create_output_directory():
            return 1
        
        # Get table CREATE commands
        try:
            create_commands, failed_count = self.get_table_create_commands()
        except Exception as e:
            self.logger.error(f"Failed to get table schemas: {e}")
            return 1
        
        if not create_commands:
            self.logger.warning("No tables found or all tables failed to process")
            return 1
        
        # Export individual table files
        exported_count, export_failed_count = self._export_table_files(create_commands)
        total_failed = failed_count + export_failed_count
        
        # Generate README
        self.logger.progress("Generating README.md...")
        self.readme_generator.generate_readme(
            self.output_dir, self.cluster, self.database, "export-table-schemas.py",
            "table", exported_count, total_failed, list(create_commands.keys())
        )
        
        # Print summary and return exit code
        return self.summary.print_summary("table", len(create_commands) + failed_count, 
                                         exported_count, total_failed, self.output_dir)
    
    def _export_table_files(self, create_commands):
        """Export individual KQL files for each table"""
        self.logger.header("Writing KQL files...")
        exported_count = 0
        failed_count = 0
        
        for table, create_command in create_commands.items():
            content = self.generate_file_header(table, "CREATE TABLE") + create_command
            
            if self.write_file(f"{table}.kql", content, f"table {table}"):
                exported_count += 1
            else:
                failed_count += 1
        
        return exported_count, failed_count


def main():
    parser = argparse.ArgumentParser(description="Export Kusto table CREATE commands to individual KQL files")
    parser.add_argument("-c", "--cluster", required=True, help="Cluster URL (e.g. https://<name>.kusto.windows.net)")
    parser.add_argument("-d", "--database", required=True, help="Database name")
    parser.add_argument("-o", "--output-dir", default="table_schemas", help="Directory for individual KQL files")
    args = parser.parse_args()

    # Create exporter and run export
    exporter = TableExporter(args.cluster, args.database, args.output_dir)
    
    # Log configuration
    exporter.logger.info(f"Cluster: {args.cluster}")
    exporter.logger.info(f"Database: {args.database}")
    exporter.logger.info(f"Output directory: {args.output_dir}")
    
    return exporter.export_tables()


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)