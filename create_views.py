from pyarrow import flight
import json
import urllib3
import os
import csv
from cpd_client import CPDClient
from dotenv import load_dotenv
from datetime import datetime
from typing import List, Dict

# Disable SSL warnings
urllib3.disable_warnings()

load_dotenv(override=True)

PROJECT_ID = os.environ.get('PROJECT_ID')
CONNECTION_ID = '803c91ef-a8b6-49c6-9c89-ee1b96f922d4'

# Configurable global constant for view suffix
VIEW_SUFFIX = '_VW'

class TokenClientAuthHandler(flight.ClientAuthHandler):
    """Custom authentication handler for Flight client"""
    
    def __init__(self, token):
        super().__init__()
        self.token = str(token).encode('utf-8')
        
    def authenticate(self, outgoing, incoming):
        outgoing.write(self.token)
        self.token = incoming.read()
        
    def get_token(self):
        return self.token


def authenticate_flight_client(flight_hostname=None, cpd_client=None, config_file=None):
    """
    Authenticate with CPD and setup Flight client
    
    Returns:
        flight_client if successful, None if failed
    """
    try:
        print("Starting authentication...")
        
        # Use provided CPD client or create new one
        if cpd_client is None:
            cpd_client = CPDClient(config_file)
        
        # Get flight hostname from parameter or environment
        if flight_hostname is None:
            flight_hostname = os.environ.get('CPD_FS_HOST')
        
        if not flight_hostname:
            raise ValueError("CPD_FS_HOST must be provided or set in environment")
        
        # Use CPDClient for authentication
        if not cpd_client._authenticated:
            cpd_client.authenticate()
        
        # Get the token from CPDClient headers
        auth_header = cpd_client.headers.get('Authorization')
        if not auth_header:
            print("ERROR: No authorization header found in CPD client")
            return None
            
        token = auth_header  # Already in "Bearer token" format
        print("Authentication token obtained from CPD client")
        
        # Setup Flight client
        flight_url = f'grpc+tls://{flight_hostname}:443'
        flight_client = flight.FlightClient(
            flight_url,
            override_hostname=flight_hostname,
            disable_server_verification=True
        )
        
        # Authenticate Flight client
        auth_handler = TokenClientAuthHandler(token)
        flight_client.authenticate(auth_handler, options=flight.FlightCallOptions(timeout=10.0))
        
        print("Flight client authenticated successfully")
        return flight_client
        
    except Exception as e:
        print(f"ERROR: Authentication failed: {e}")
        return None


def is_signed(type_code):
    """
    Determine if a SQL type is signed based on type code
    
    Returns:
        bool: True if signed, False otherwise
    """
    # Signed numeric types
    signed_types = {
        -5,  # BIGINT
        4,   # INTEGER
        5,   # SMALLINT
        6,   # FLOAT
        7,   # REAL
        8,   # DOUBLE
        2,   # NUMERIC
        3,   # DECIMAL
    }
    
    return type_code in signed_types


def process_schema_for_asset(schema):
    """
    Process Arrow schema and convert to data asset column format
    Only uses actual metadata from Flight client - no fallbacks or defaults
    
    Returns:
        List of column dictionaries for data asset creation
    """
    columns = []
    
    for field in schema:
        # Only process fields that have metadata
        if not field.metadata:
            print(f"WARNING: No metadata found for field {field.name}, skipping")
            continue
            
        try:
            metadata = {k.decode(): v.decode() for k, v in field.metadata.items()}
            
            # Only proceed if we have the required type information
            if 'columnType' not in metadata or 'columnNativeType' not in metadata:
                print(f"WARNING: Missing required type metadata for field {field.name}, skipping")
                continue
                
            type_code = int(metadata['columnType'])
            native_type = metadata['columnNativeType']
            length = int(metadata.get('columnLength', '0'))
            scale = int(metadata.get('columnScale', '0'))
            
            # Use native type in lowercase
            asset_type = native_type.lower()
            
            column_info = {
                'name': field.name,
                'type': {
                    'type': asset_type,
                    'native_type': native_type,
                    'length': length,
                    'scale': scale,
                    'signed': is_signed(type_code),
                    'nullable': True
                }
            }
            
            columns.append(column_info)
            
        except Exception as e:
            print(f"ERROR: Failed to process metadata for field {field.name}: {e}")
            continue
    
    return columns


def get_schema_from_query(flight_client, asset_id, project_id, sql_query):
    """
    Get schema information from SQL query using Flight client
    
    Returns:
        List of column dictionaries if successful, None if failed
    """
    if not flight_client:
        print("ERROR: Flight client not initialized")
        return None
        
    try:
        print(f"Getting schema for query: {sql_query}")
        
        # Create flight descriptor
        descriptor = {
            'asset_id': asset_id,
            'project_id': project_id,
            'interaction_properties': {
                'select_statement': sql_query
            }
        }
        
        flight_descriptor = flight.FlightDescriptor.for_command(json.dumps(descriptor))
        
        # Get flight info (this validates the query and returns schema)
        flight_info = flight_client.get_flight_info(flight_descriptor)
        
        print("Schema extraction successful")
        
        # Process schema into data asset format
        columns = process_schema_for_asset(flight_info.schema)
        
        return columns
        
    except Exception as e:
        print(f"ERROR: Failed to get schema: {e}")
        return None


def create_dynamic_data_asset_payload(asset_name: str, sql_query: str, columns: list) -> dict:
    """
    Create payload for data asset creation using discovered schema
    """
    return {
        "metadata": {
            "project_id": PROJECT_ID,
            "name": asset_name,
            "description": "Query - View",
            "asset_type": "data_asset",
            "tags": ["connected-data"],
            "asset_attributes": [
                "data_asset",
                "discovered_asset"
            ],
        },
        "entity": {
            "data_asset": {
                "mime_type": "application/x-ibm-rel-table",
                "columns": columns,
                "properties": [
                    {
                        "name": "select_statement",
                        "value": sql_query
                    }
                ],
                "query_properties": [
                    {
                        "name": "sample_value_set",
                        "value": "{}"
                    }
                ]                
            },
            "discovered_asset": {
                "properties": {},
                "connection_id": CONNECTION_ID,
                "connection_path": "",
                "extended_metadata": [
                    {
                        "name": "table_type",
                        "value": "SQL_QUERY"
                    }
                ]
            }
        },
        "attachments": [
            {
                "connection_id": CONNECTION_ID,
                "mime": "application/x-ibm-rel-table",
                "asset_type": "data_asset",
                "name": asset_name,
                "description": f"Asset for {asset_name}",
                "private_url": False,
                "connection_path": "/",
                "data_partitions": 1
            }
        ]
    }


def create_data_asset_from_query(cpd_client: CPDClient, flight_client, asset_name: str, sql_query: str) -> tuple:
    """
    Create a data asset using dynamic schema discovery from SQL query
    
    Returns:
        tuple: (status_message, asset_id, columns_info, error_message)
    """
    # Step 1: Get schema from query
    print(f"Step 1: Discovering schema for query...")
    columns = get_schema_from_query(flight_client, CONNECTION_ID, PROJECT_ID, sql_query)
    
    if not columns:
        return "ERROR: Failed to discover schema", None, None, "Failed to discover schema from query"
    
    print(f"Discovered {len(columns)} columns:")
    for col in columns:
        print(f"  - {col['name']}: {col['type']['type']} (length: {col['type']['length']})")
    
    # Step 2: Create data asset with discovered schema
    print(f"Step 2: Creating data asset with discovered schema...")
    url = "/v2/data_assets"
    params = {
        "project_id": PROJECT_ID,
        "duplicate_action": "REPLACE"
    }
    
    payload = create_dynamic_data_asset_payload(asset_name, sql_query, columns)
    
    try:
        response = cpd_client.post(url, json=payload, params=params)
        
        if response.status_code in [200, 201]:
            try:
                response_data = response.json()
                asset_id = response_data.get('metadata', {}).get('asset_id', 'N/A')
                asset_name_returned = response_data.get('metadata', {}).get('name', 'N/A')
                print(f"✓ Successfully created asset: {asset_name_returned} - ID: {asset_id}")
                return "SUCCESS", asset_id, columns, ""
            except Exception as e:
                print(f"✓ Asset created but response parsing failed: {e}")
                return "SUCCESS: response parsing failed", None, columns, f"Response parsing failed: {e}"
        else:
            try:
                response_data = response.json()
                message = response_data.get('message', response.text)
                error_msg = f"HTTP {response.status_code} - {message}"
                print(f"✗ Failed to create asset {asset_name}: {error_msg}")
                return f"ERROR: {error_msg}", None, columns, error_msg
            except:
                error_msg = f"HTTP {response.status_code} - {response.text}"
                print(f"✗ Failed to create asset {asset_name}: {error_msg}")
                return f"ERROR: {error_msg}", None, columns, error_msg
            
    except Exception as e:
        error_msg = f"Request failed: {e}"
        print(f"✗ Error creating asset {asset_name}: {error_msg}")
        return f"ERROR: {error_msg}", None, columns, error_msg


def read_csv_config(csv_file_path: str) -> List[Dict[str, str]]:
    """
    Read CSV configuration file and return list of table configurations
    
    Returns:
        List of dictionaries with keys: table_name, columns, where_clause
    """
    configurations = []
    
    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 because of header
                # Validate required columns
                if 'TABLE_NAME' not in row or 'COLUMNS' not in row:
                    print(f"WARNING: Row {row_num} missing required columns, skipping")
                    continue
                
                table_name = row['TABLE_NAME'].strip()
                columns = row['COLUMNS'].strip()
                where_clause = row.get('WHERE_CLAUSE', '').strip()
                
                if not table_name or not columns:
                    print(f"WARNING: Row {row_num} has empty table_name or columns, skipping")
                    continue
                
                configurations.append({
                    'table_name': table_name,
                    'columns': columns,
                    'where_clause': where_clause
                })
                
        print(f"Successfully read {len(configurations)} configurations from {csv_file_path}")
        return configurations
        
    except FileNotFoundError:
        print(f"ERROR: CSV file not found: {csv_file_path}")
        return []
    except Exception as e:
        print(f"ERROR: Failed to read CSV file {csv_file_path}: {e}")
        return []


def build_sql_query(table_name: str, columns: str, where_clause: str) -> str:
    """
    Build SQL query from CSV configuration
    
    Args:
        table_name: Name of the table
        columns: Either '*' or pipe-separated column names like 'CLIENT_ID|CLIENT_NAME'
        where_clause: Optional WHERE clause (can be empty)
    
    Returns:
        Complete SQL SELECT statement
    """
    # Handle columns
    if columns == '*':
        select_columns = '*'
    else:
        # Split by pipe and join with commas
        column_list = [col.strip() for col in columns.split('|') if col.strip()]
        select_columns = ', '.join(column_list)
    
    # Build base query
    sql_query = f"SELECT {select_columns} FROM {table_name}"
    
    # Add WHERE clause if provided
    if where_clause:
        # Clean up WHERE clause - remove 'WHERE' if it's already there
        where_clause = where_clause.strip()
        if where_clause.upper().startswith('WHERE '):
            where_clause = where_clause[6:]  # Remove 'WHERE '
        sql_query += f" WHERE {where_clause}"
    
    return sql_query


def generate_asset_name(table_name: str, columns: str, where_clause: str, timestamp: str = None) -> str:
    """
    Generate asset name from table configuration
    
    Args:
        table_name: Name of the table
        columns: Column specification  
        where_clause: WHERE clause (optional)
        timestamp: Not used (kept for compatibility)
    
    Returns:
        Generated asset name
    """
    # Remove quotes from table name if present
    clean_table_name = table_name.strip('"\'')
    
    # Use full table name including schema
    asset_name = f"{clean_table_name}{VIEW_SUFFIX}"
    
    return asset_name


def process_csv_configurations(cpd_client: CPDClient, flight_client, configurations: List[Dict[str, str]]) -> List[Dict]:
    """
    Process all CSV configurations and create data assets
    
    Returns:
        List of results for each configuration
    """
    results = []
    
    for i, config in enumerate(configurations, 1):
        print(f"\n{'='*70}")
        print(f"PROCESSING LINE {i}/{len(configurations)}")
        print(f"{'='*70}")
        
        table_name = config['table_name']
        columns = config['columns']
        where_clause = config['where_clause']
        
        print(f"Table: {table_name}")
        print(f"Columns: {columns}")
        print(f"Where: {where_clause if where_clause else '(none)'}")
        
        # Build SQL query
        sql_query = build_sql_query(table_name, columns, where_clause)
        print(f"SQL Query: {sql_query}")
        
        # Generate asset name
        asset_name = generate_asset_name(table_name, columns, where_clause)
        print(f"Asset Name: {asset_name}")
        
        # Create data asset
        status, asset_id, columns_info, error_message = create_data_asset_from_query(
            cpd_client, flight_client, asset_name, sql_query
        )
        
        result = {
            'table_name': table_name,
            'columns': columns,
            'where_clause': where_clause,
            'asset_name': asset_name,
            'sql_query': sql_query,
            'status': status,
            'asset_id': asset_id if asset_id else '',
            'columns_count': len(columns_info) if columns_info else 0,
            'error_message': error_message if error_message else ''
        }
        
        results.append(result)
        
        print(f"Result: {status}")
        if asset_id:
            print(f"Asset ID: {asset_id}")
    
    return results


def write_results_to_csv(results: List[Dict], input_csv_path: str) -> str:
    """
    Write results to CSV file with timestamp suffix
    
    Args:
        results: List of result dictionaries
        input_csv_path: Path to the input CSV file
    
    Returns:
        Path to the output CSV file
    """
    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Generate output filename
    base_name = os.path.splitext(input_csv_path)[0]
    output_csv_path = f"{base_name}_out_{timestamp}.csv"
    
    # Define CSV headers
    headers = [
        'table_name',
        'columns',
        'where_clause',
        'asset_name',
        'sql_query',
        'status',
        'asset_id',
        'columns_count',
        'error_message'
    ]
    
    try:
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            
            # Write header
            writer.writeheader()
            
            # Write results
            for result in results:
                writer.writerow(result)
        
        print(f"\n{'='*70}")
        print(f"Results written to: {output_csv_path}")
        print(f"Total rows: {len(results)}")
        print(f"{'='*70}")
        
        return output_csv_path
        
    except Exception as e:
        print(f"ERROR: Failed to write CSV output: {e}")
        return None


def print_summary(results: List[Dict]):
    """Print summary of all processed configurations"""
    print(f"\n{'SUMMARY':-^70}")
    print(f"Total configurations processed: {len(results)}")
    
    successful = [r for r in results if r['status'].startswith('SUCCESS')]
    failed = [r for r in results if not r['status'].startswith('SUCCESS')]
    
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    
    if successful:
        print(f"\n{'SUCCESSFUL ASSETS':-^70}")
        for result in successful:
            print(f"✓ {result['asset_name']} - {result['table_name']} ({result['columns_count']} columns)")
    
    if failed:
        print(f"\n{'FAILED ASSETS':-^70}")
        for result in failed:
            print(f"✗ {result['asset_name']} - {result['status']}")


def main(csv_file_path):
    """
    Main function to process CSV configurations and create data assets
    """
    # Configuration
    
    print("="*70)
    print("BATCH DATA ASSET CREATION FROM CSV CONFIGURATION")
    print("="*70)
    print(f"Project ID: {PROJECT_ID}")
    print(f"Connection ID: {CONNECTION_ID}")
    print(f"View Suffix: {VIEW_SUFFIX}")
    print(f"CSV File: {csv_file_path}")
    
    try:
        # Step 1: Read CSV configurations
        print(f"\n{'STEP 1: Reading CSV Configuration':-^70}")
        configurations = read_csv_config(csv_file_path)
        
        if not configurations:
            print("ERROR: No valid configurations found in CSV file")
            return
        
        # Step 2: Authenticate with Flight
        print(f"\n{'STEP 2: Flight Authentication':-^70}")
        flight_client = authenticate_flight_client()
        
        if not flight_client:
            print("ERROR: Flight authentication failed")
            return
        
        # Step 3: Process all configurations
        print(f"\n{'STEP 3: Processing Configurations':-^70}")
        with CPDClient() as cpd_client:
            results = process_csv_configurations(cpd_client, flight_client, configurations)
            
            # Step 4: Write results to CSV
            print(f"\n{'STEP 4: Writing Results to CSV':-^70}")
            output_csv_path = write_results_to_csv(results, csv_file_path)
            
            # Step 5: Print summary
            print_summary(results)
            
            successful_count = len([r for r in results if r['status'].startswith('SUCCESS')])
            if successful_count == len(results):
                print(f"\nAll {len(results)} data assets created successfully!")
            elif successful_count > 0:
                print(f"\n✅ {successful_count}/{len(results)} data assets created successfully!")
            else:
                print(f"\n❌ No data assets were created successfully!")
            
            if output_csv_path:
                print(f"\nDetailed results saved to: {output_csv_path}")
                
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":

    # Expected CSV format (with header):
    # TABLE_NAME,COLUMNS,WHERE_CLAUSE
    # T_BANK_CLIENTS,CLIENT_ID|CLIENT_NAME,
    # T_BANK_CLIENTS,*,CLIENT_ID > 1000
    # T_BANK_CLIENTS,CLIENT_NAME|EMAIL,CLIENT_NAME LIKE '%Smith%'
    main(csv_file_path='views_to_create.csv')