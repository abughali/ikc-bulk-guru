from typing import Dict, List, Optional
import os
import csv
import json
from datetime import datetime
from cpd_client import CPDClient
from dotenv import load_dotenv
import re

# Environment variables
load_dotenv(override=True)
project_id = os.environ.get('PROJECT_ID')

# Try to import pyarrow for optional SQL validation
try:
    from pyarrow import flight
    import urllib3
    PYARROW_AVAILABLE = True
    # Disable SSL warnings if pyarrow is available
    urllib3.disable_warnings()
except ImportError:
    PYARROW_AVAILABLE = False
    print("WARNING: pyarrow not installed. SQL validation will be disabled.")

# SQL Validation Configuration
VALIDATE_SQL = False  # Set to False to disable SQL validation

# Constants
INPUT_CONNECTION_ID = "803c91ef-a8b6-49c6-9c89-ee1b96f922d4"  # Connection for SQL queries
OUTPUT_CONNECTION_ID = "acb03b96-1d60-40bc-aae3-46ab56071832"  # Connection for output results
OUTPUT_SCHEMA_NAME = "BIDEMODATA"
OUTPUT_TABLE_NAME = "DQ_OUTPUT_SQL"
FAILED_RECORDS_COUNT = 5

# Global cache for dimensions
_dimensions_cache: Optional[List[Dict]] = None
_flight_client = None  # Global flight client for SQL validation

class TokenClientAuthHandler(flight.ClientAuthHandler):
    """Custom authentication handler for Flight client"""
    
    def __init__(self, token):
        if not PYARROW_AVAILABLE:
            raise ImportError("pyarrow is required for SQL validation")
        super().__init__()
        self.token = str(token).encode('utf-8')
        
    def authenticate(self, outgoing, incoming):
        outgoing.write(self.token)
        self.token = incoming.read()
        
    def get_token(self):
        return self.token

def get_flight_client(cpd_client: CPDClient) -> Optional[object]:
    """
    Get or create authenticated Flight client for SQL validation
    
    Returns:
        Flight client if successful and enabled, None otherwise
    """
    global _flight_client
    
    # Return None if validation is disabled or pyarrow not available
    if not VALIDATE_SQL or not PYARROW_AVAILABLE:
        return None
    
    # Return cached client if already created
    if _flight_client is not None:
        return _flight_client
    
    try:
        print("\nInitializing SQL validation service...")
        
        # Get flight hostname
        flight_hostname = os.environ.get('CPD_FS_HOST')
        if not flight_hostname:
            print("WARNING: CPD_FS_HOST not set. SQL validation disabled.")
            return None
        
        # Get token from CPD client
        auth_header = cpd_client.headers.get('Authorization')
        if not auth_header:
            print("WARNING: No authorization header. SQL validation disabled.")
            return None
        
        # Setup Flight client
        flight_url = f'grpc+tls://{flight_hostname}:443'
        flight_client = flight.FlightClient(
            flight_url,
            override_hostname=flight_hostname,
            disable_server_verification=True
        )
        
        # Authenticate Flight client
        auth_handler = TokenClientAuthHandler(auth_header)
        flight_client.authenticate(auth_handler, 
                                 options=flight.FlightCallOptions(timeout=10.0))
        
        _flight_client = flight_client
        print("SQL validation service initialized successfully")
        return flight_client
        
    except Exception as e:
        print(f"WARNING: Could not initialize SQL validation: {e}")
        print("Continuing without SQL validation...")
        return None


def validate_sql_query(flight_client, sql_query: str) -> Dict:
    """
    Validate a SQL query using Flight service
    
    Returns:
        Dict with validation results
    """
    if not flight_client:
        return {
            'status': 'skipped',
            'message': 'SQL validation not available'
        }
    
    try:
        # Create flight descriptor
        descriptor = {
            'asset_id': INPUT_CONNECTION_ID,
            'project_id': project_id,
            'interaction_properties': {
                'select_statement': sql_query
            }
        }
        
        flight_descriptor = flight.FlightDescriptor.for_command(
            json.dumps(descriptor)
        )
        
        # Get flight info (this validates the query)
        flight_info = flight_client.get_flight_info(flight_descriptor)
        
        # Extract basic schema info
        columns = []
        for field in flight_info.schema:
            columns.append(field.name)
        
        return {
            'status': 'valid',
            'message': f"Query validated successfully ({len(columns)} columns)",
            'column_count': len(columns),
            'columns': columns
        }
        
    except Exception as e:
        error_msg = str(e)
        
        # Parse common error types
        if "Table could not be found" in error_msg:
            return {
                'status': 'invalid',
                'message': 'Table not found',
                'error': error_msg
            }
        elif "SQLCODE=-204" in error_msg:
            return {
                'status': 'invalid',
                'message': 'SQL object not found',
                'error': error_msg
            }
        elif "SQLCODE=-206" in error_msg:
            return {
                'status': 'invalid',
                'message': 'SQL syntax error',
                'error': error_msg
            }        
        elif "not authorized" in error_msg:
            return {
                'status': 'invalid',
                'message': 'Authorization error',
                'error': error_msg
            }
        else:
            return {
                'status': 'invalid',
                'message': 'SQL validation failed',
                'error': error_msg
            }


def get_data_quality_dimensions(client: CPDClient) -> List[Dict]:
    """
    Get all data quality dimensions from the CPD API.
    Returns a list of dimension dictionaries with id, name, description, and is_default fields.
    """
    global _dimensions_cache
    
    # Return cached dimensions if already loaded
    if _dimensions_cache is not None:
        return _dimensions_cache
    
    url = "/data_quality/v4/dimensions"
    params = {"limit": 200}
    
    response = client.get(url, params=params)
    
    if response.status_code != 200:
        print(f"Error getting data quality dimensions: {response.status_code} - {response.text}")
        return []
    
    data = response.json()
    dimensions = data.get("dimensions", [])
    
    # Cache the results
    _dimensions_cache = dimensions
    print(f"Loaded {len(dimensions)} data quality dimensions")
    
    return dimensions


def get_dimension_by_name(client: CPDClient, dimension_name: str) -> Optional[Dict]:
    """
    Get a specific data quality dimension by name.
    Returns the dimension dictionary if found, None otherwise.
    """
    dimensions = get_data_quality_dimensions(client)
    
    for dimension in dimensions:
        if dimension.get("name", "").lower() == dimension_name.lower():
            return dimension
    
    return None


def get_dimension_id_by_name(client: CPDClient, dimension_name: str) -> Optional[str]:
    """
    Get a data quality dimension ID by name.
    Returns the dimension ID if found, None otherwise.
    """
    dimension = get_dimension_by_name(client, dimension_name)
    return dimension.get("id") if dimension else None


def create_sql_dq_rule(client: CPDClient, rule_name: str, description: str, 
                       dimension_id: str, sql_statement: str) -> Dict:
    """
    Create a SQL-based data quality rule.
    Returns result dictionary with status and details.
    """
    url = f"/data_quality/v3/projects/{project_id}/rules"
    
    payload = {
        "name": rule_name,
        "description": description,
        "dimension": {
            "id": dimension_id
        },
        "input": {
            "sql": {
                "connection": {
                    "id": INPUT_CONNECTION_ID
                },
                "select_statement": sql_statement
            }
        },
        "output": {
            "database": {
                "location": {
                    "connection": {
                        "id": OUTPUT_CONNECTION_ID
                    },
                    "schema_name": OUTPUT_SCHEMA_NAME,
                    "table_name": OUTPUT_TABLE_NAME
                },
                "records_type": "failing_records",
                "update_type": "append"
            },
            "maximum_record_count": FAILED_RECORDS_COUNT,
            "columns": [
                {
                    "name": "Failing_rules",
                    "type": "metric",
                    "metric": "failing_rules"
                },
                {
                    "name": "Job_ID",
                    "type": "metric",
                    "metric": "job_id"
                },
                {
                    "name": "Job_run_ID",
                    "type": "metric",
                    "metric": "job_run_id"
                },
                {
                    "name": "Passing_rules",
                    "type": "metric",
                    "metric": "passing_rules"
                },
                {
                    "name": "Percent_failing_rules",
                    "type": "metric",
                    "metric": "percent_failing_rules"
                },
                {
                    "name": "Percent_passing_rules",
                    "type": "metric",
                    "metric": "percent_passing_rules"
                },
                {
                    "name": "Project_ID",
                    "type": "metric",
                    "metric": "project_id"
                },
                {
                    "name": "Record_ID",
                    "type": "metric",
                    "metric": "record_id"
                },
                {
                    "name": "Rule_ID",
                    "type": "metric",
                    "metric": "rule_id"
                },
                {
                    "name": "Rule_name",
                    "type": "metric",
                    "metric": "rule_name"
                },
                {
                    "name": "System_date",
                    "type": "metric",
                    "metric": "system_date"
                },
                {
                    "name": "System_time",
                    "type": "metric",
                    "metric": "system_time"
                }
            ],
            "inherit_project_level_output_setting": False,
            "create_table_only_when_issues_are_found": False,
            "import_table_in_project": True
        },
        "apply_all_present_dimensions": False
    }
    
    response = client.post(url, json=payload)
    
    result = {
        "status": "ERROR",
        "rule_id": None,
        "is_valid": False,
        "error_message": ""
    }
    
    if response.status_code == 201:
        data = response.json()
        result["status"] = "SUCCESS"
        result["rule_id"] = data.get("id")
        result["is_valid"] = data.get("is_valid", False)
        
        print(f"  ✓ Created SQL rule '{rule_name}' with ID: {result['rule_id']}")
        print(f"    • Is Valid: {result['is_valid']}")
        
    else:
        # Parse error response to extract concise error message
        error_msg = f"{response.status_code}"
        try:
            error_data = response.json()
            if "errors" in error_data and error_data["errors"]:
                error_info = error_data["errors"][0]
                error_code = error_info.get("code", "")
                error_message = error_info.get("message", "")
                if error_code and error_message:
                    error_msg = f"{response.status_code} - {error_code}: {error_message}"
                elif error_message:
                    error_msg = f"{response.status_code} - {error_message}"
        except:
            # If JSON parsing fails, use original text but truncated
            error_text = response.text
            if len(error_text) > 200:
                error_msg = f"{response.status_code} - {error_text[:200]}..."
            else:
                error_msg = f"{response.status_code} - {error_text}"
        
        result["error_message"] = error_msg
        print(f"  ✗ Error creating SQL rule '{rule_name}': {error_msg}")
    
    return result


def process_sql_dq_rules_csv(client: CPDClient, input_file: str):
    """
    Process CSV file to create SQL-based data quality rules.
    Expected CSV columns:
    0: Data quality rule name
    1: Description  
    2: Data quality dimension
    3: SQL SELECT statement
    """

    # Create output directory
    output_dir = "out"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename based on input filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_file = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(output_dir, f"{base_file}_{timestamp}.csv")
    
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"SQL Validation: {'ENABLED' if VALIDATE_SQL and PYARROW_AVAILABLE else 'DISABLED'}")
    
    # Preload dimensions
    get_data_quality_dimensions(client)
    
    # Initialize flight client if validation is enabled
    flight_client = get_flight_client(client) if VALIDATE_SQL else None
    
    results_data = []
    
    try:
        with open(input_file, encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, skipinitialspace=True, delimiter=',', 
                              quotechar='"', doublequote=True)
            next(reader)  # Skip header row
            for row_num, row in enumerate(reader, 1):
                if len(row) < 4:  # Ensure we have all required columns
                    print(f"WARNING: Row {row_num} has insufficient columns, skipping")
                    continue
                
                rule_name = row[0].strip()
                description = row[1].strip()
                dimension_name = row[2].strip()
                sql_statement = row[3].strip()
                
                print(f"\nProcessing row {row_num}: {rule_name}")
                
                # Initialize result tracking
                result_row = {
                    'original_row': row,
                    'row_number': row_num,
                    'rule_name': rule_name,
                    'sql_validation': '',
                    'validation_message': '',
                    'rule_status': '',
                    'rule_id': '',
                    'is_valid': '',
                    'error_message': ''
                }
                
                try:
                    # Get dimension ID
                    dimension_id = get_dimension_id_by_name(client, dimension_name)
                    if not dimension_id:
                        error_msg = f"Dimension '{dimension_name}' not found"
                        print(f"  ✗ {error_msg}")
                        result_row['rule_status'] = f"ERROR: {error_msg}"
                        result_row['error_message'] = error_msg
                        results_data.append(result_row)
                        continue
                    
                    # Validate SQL if enabled
                    if flight_client:
                        print(f"  → Validating SQL statement...")
                        validation_result = validate_sql_query(flight_client, sql_statement)
                        
                        result_row['sql_validation'] = validation_result['status'].upper()
                        result_row['validation_message'] = validation_result['message']
                        
                        if validation_result['status'] == 'valid':
                            print(f"    ✓ SQL validation passed: {validation_result['message']}")
                            print(f"    ✓ Columns: {validation_result['columns']}")

                            # Run DQ validation on resulting column names
                            invalid_columns = []
                            for col in validation_result['columns']:
                                if not re.match(r'^[A-Za-z][A-Za-z0-9_]*$', col):
                                    invalid_columns.append(col)

                            if invalid_columns:
                                error_msg = f"Invalid column name(s): {', '.join(invalid_columns)}. Columns must start with a letter and only contain letters, numbers, or underscores."
                                print(f"    ✗ DQ Rule Violation: {error_msg}")
                                result_row['rule_status'] = "SKIPPED: Invalid column names"
                                result_row['error_message'] = error_msg
                                results_data.append(result_row)
                                continue

                        else:
                            print(f"    ⚠ SQL validation failed: {validation_result['message']}")
                            if 'error' in validation_result:
                                print(f"      Details: {validation_result['error']}")

                            # Skip rule creation if validation failed
                            result_row['rule_status'] = "SKIPPED: Validation failed"
                            result_row['error_message'] = validation_result.get('error', validation_result['message'])
                            results_data.append(result_row)
                            continue
                    else:
                        result_row['sql_validation'] = 'SKIPPED'
                        result_row['validation_message'] = 'Validation disabled'
                    
                    # Create SQL-based rule
                    print(f"  → Creating SQL rule: {rule_name}")
                    print(f"    • SQL: {sql_statement}")
                    
                    rule_result = create_sql_dq_rule(client, rule_name, description, 
                                                   dimension_id, sql_statement)
                    
                    result_row['rule_status'] = rule_result['status']
                    result_row['rule_id'] = rule_result['rule_id'] or ''
                    result_row['is_valid'] = str(rule_result['is_valid'])
                    result_row['error_message'] = rule_result['error_message']
                
                except Exception as e:
                    error_msg = f"Processing error: {e}"
                    print(f"  ✗ {error_msg}")
                    result_row['rule_status'] = f"ERROR: {error_msg}"
                    result_row['error_message'] = error_msg
                
                results_data.append(result_row)
    
    except FileNotFoundError:
        print(f"ERROR: {input_file} file not found")
        return
    except Exception as e:
        print(f"ERROR reading CSV file: {e}")
        return
    
    # Write results CSV
    print(f"\nWriting results to: {output_file}")
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            header = [
                'Data Quality Rule', 'Description', 'Data Quality Dimension',
                'SQL Statement',
                'SQL Validation', 'Validation Message',
                'Rule Status', 'Rule ID', 'Is Valid', 'Error Message'
            ]
            writer.writerow(header)
            
            # Write data rows
            for result_row in results_data:
                row = result_row['original_row']
                    
                output_row = row + [
                    result_row['sql_validation'],
                    result_row['validation_message'],
                    result_row['rule_status'],
                    result_row['rule_id'],
                    result_row['is_valid'],
                    result_row['error_message']
                ]
                writer.writerow(output_row)
        
        # Print summary
        total_rows = len(results_data)
        successful_rules = sum(1 for r in results_data if r['rule_status'] == "SUCCESS")
        
        # SQL validation summary if enabled
        if flight_client:
            valid_sql = sum(1 for r in results_data if r['sql_validation'] == "VALID")
            invalid_sql = sum(1 for r in results_data if r['sql_validation'] == "INVALID")
            print(f"\nSQL VALIDATION SUMMARY:")
            print(f"Valid SQL queries: {valid_sql}")
            print(f"Invalid SQL queries: {invalid_sql}")
        
        print(f"\nRULE CREATION SUMMARY:")
        print(f"Total rows processed: {total_rows}")
        print(f"Successful SQL rules created: {successful_rules}")
        print(f"Failed rules: {total_rows - successful_rules}")
        print(f"Results saved to: {output_file}")
        
    except Exception as e:
        print(f"ERROR writing results CSV: {e}")


def main_sql_dq_rules(input_file):
    """Main function to process SQL-based data quality rules CSV"""
    print("="*60)
    print("SQL-BASED DATA QUALITY RULES PROCESSING")
    print("="*60)
    
    with CPDClient() as client:
        # Preload dimensions
        get_data_quality_dimensions(client)
        
        print("\n" + "="*60)
        print("PROCESSING SQL DATA QUALITY RULES CSV")
        print("="*60)
        
        process_sql_dq_rules_csv(client, input_file)
        
        print("\n" + "="*60)
        print("SQL DATA QUALITY RULES PROCESS COMPLETED")
        print("="*60)


# Example usage
if __name__ == "__main__":
    # Expected CSV format (with header - skipped):
    # DQ Rule Name,Description,DQ Dimension,SQL Statement
    # Example rows:
    # Check_Recent_Orders,Orders from last 30 days,Timeliness,"SELECT ORDER_ID FROM ORDERS WHERE ORDER_DATE < CURRENT_DATE - 30 DAYS"
    # Validate_Customer_Age,Customers with invalid age,Validity,"SELECT CUSTOMER_ID FROM CUSTOMERS WHERE AGE < 0 OR AGE > 150"
    # Duplicate_Emails,Find duplicate emails,Uniqueness,"SELECT EMAIL FROM CUSTOMERS GROUP BY EMAIL HAVING COUNT(*) > 1"
    #
    # For SQL with quoted identifiers, use double quotes in CSV:
    # Check_Schema_Table,Check specific schema,Validity,"SELECT CLIENT_ID FROM ""ADMIN"".""T_BANK_CLIENTS"" WHERE STATUS IS NULL"

    main_sql_dq_rules('sql_dq_rules.csv')