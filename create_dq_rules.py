from typing import Dict, List, Optional, Tuple
import os
import csv
from datetime import datetime
from cpd_client import CPDClient
from dotenv import load_dotenv

# Constants
OUTPUT_CONNECTION_ID="d12b74c3-746d-4956-bfd6-8e21f170f2b8"
OUTPUT_SCHEMA_NAME="BIDEMODATA"
OUTPUT_TABLE_NAME="DQ_OUTPUT"
FAILED_RECORDS_COUNT=5

# Environment variables
load_dotenv(override=True)
project_id = os.environ.get('PROJECT_ID')

# Global cache for dimensions and definitions
_dimensions_cache: Optional[List[Dict]] = None
_definitions_cache: Optional[List[Dict]] = None

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


def get_all_definitions(client: CPDClient, force_refresh: bool = False) -> List[Dict]:
    """
    Get all data quality definitions from the CPD API.
    Returns a list of definition dictionaries with id, name, description, expression, etc.
    
    Args:
        client: CPDClient instance
        force_refresh: If True, bypass cache and fetch fresh data
    """
    global _definitions_cache
    
    # Return cached definitions if already loaded and not forcing refresh
    if _definitions_cache is not None and not force_refresh:
        return _definitions_cache
    
    url = f"/data_quality/v3/projects/{project_id}/definitions"
    params = {"limit": 200}
    
    all_definitions = []
    start_token = None
    
    while True:
        if start_token:
            params["start"] = start_token
        
        response = client.get(url, params=params)
        
        if response.status_code != 200:
            print(f"Error getting data quality definitions: {response.status_code} - {response.text}")
            return []
        
        data = response.json()
        definitions = data.get("definitions", [])
        all_definitions.extend(definitions)
        
        # Check if there are more pages
        next_page = data.get("next")
        if next_page and "start" in next_page:
            start_token = next_page["start"]
        else:
            break
    
    # Cache the results
    _definitions_cache = all_definitions
    print(f"Loaded {len(all_definitions)} data quality definitions")
    
    return all_definitions


def get_definition_by_name(client: CPDClient, definition_name: str) -> Optional[Dict]:
    """
    Get a specific data quality definition by name (case sensitive).
    Returns the definition dictionary if found, None otherwise.
    """
    definitions = get_all_definitions(client)
    
    for definition in definitions:
        if definition.get("name", "") == definition_name:
            return definition
    
    return None


def getAssetByName(client: CPDClient, name: str) -> str:
    """
    This function retrieves the ID of an asset in a project based on its name.
    """
    url = f"/v2/asset_types/data_asset/search?project_id={project_id}&allow_metadata_on_dpr_deny=true"
    
    payload = {
        "query": f"asset.name:{name}",
        "limit": 1
    }
    
    response = client.post(url, json=payload)
    
    if response.status_code != 200:
        raise ValueError(f"Error scanning project: {response.text}")
    else:
        response_data = response.json()
        if response_data['total_rows'] < 1:
            raise AssertionError(f'Asset {name} is not found')
        return response_data['results'][0]['metadata']['asset_id']


def validateColumn(client: CPDClient, asset_id: str, column_name: str) -> bool:
    """This function validates that a column exists in the asset metadata."""
    url = f"/v2/assets/{asset_id}?project_id={project_id}&allow_metadata_on_dpr_deny=true"
    
    response = client.get(url)
    
    if response.status_code != 200:
        print(f"Error getting asset details: {response.text}")
        return False
    
    asset_data = response.json()
    
    if 'entity' in asset_data and 'data_asset' in asset_data['entity'] and 'columns' in asset_data['entity']['data_asset']:
        columns = asset_data['entity']['data_asset']['columns']
        column_names = {col['name'] for col in columns}
        return column_name in column_names
    
    return False

def parse_bound_expressions(bound_expr: str) -> List[str]:
    """
    Parse bound expressions string to extract column names.
    Handles both single fields and multiple fields separated by + or |
    """
    expr = bound_expr.strip()

    if not expr:
        return []

    # Replace + ; | with a common separator
    expr = expr.replace('+', ',').replace('|', ',').replace(';', ',')

    # Split by comma and strip each part
    columns = [col.strip() for col in expr.split(',') if col.strip()]

    return columns

def create_column_bindings(asset_id: str, field_names: List[str], column_names: List[str]) -> List[Dict]:
    """
    Create column bindings for the rule.
    Maps field1, field2, etc. to actual column names.
    """
    bindings = []
    
    for i, column_name in enumerate(column_names):
        if i < len(field_names):
            binding = {
                "variable_name": field_names[i],
                "target": {
                    "type": "column",
                    "data_asset": {
                        "id": asset_id
                    },
                    "column_name": column_name
                }
            }
            bindings.append(binding)
    
    return bindings


def create_dq_definition(client: CPDClient, name: str, description: str, expression: str, dimension_id: str) -> Optional[str]:
    """
    Create a data quality definition.
    Returns the definition ID if successful, None otherwise.
    """
    global _definitions_cache
    
    url = f"/data_quality/v3/projects/{project_id}/definitions"
    
    payload = {
        "name": name,
        "description": description,
        "expression": expression,
        "dimension": {
            "id": dimension_id
        }
    }
    
    response = client.post(url, json=payload)
    
    if response.status_code == 201:
        data = response.json()
        definition_id = data.get("id")
        print(f"  ✓ Created definition '{name}' with ID: {definition_id}")
        
        # Update the cache with the newly created definition
        if _definitions_cache is not None:
            new_definition = {
                "id": definition_id,
                "name": name,
                "description": description,
                "expression": expression,
                "dimension": {"id": dimension_id}
            }
            # Add any other fields that might be in the response
            new_definition.update(data)
            _definitions_cache.append(new_definition)
            print(f"  ✓ Updated definitions cache with new definition")
        
        return definition_id
    else:
        print(f"  ✗ Error creating definition '{name}': {response.status_code} - {response.text}")
        return None


def get_or_create_dq_definition(client: CPDClient, name: str, description: str, expression: str, dimension_id: str) -> Tuple[Optional[str], bool]:
    """
    Get existing definition by name or create a new one if it doesn't exist.
    Returns tuple of (definition_id, was_existing) where:
    - definition_id: the ID if found/created successfully, None otherwise
    - was_existing: True if definition already existed, False if newly created
    """
    # First check if definition already exists
    existing_definition = get_definition_by_name(client, name)
    
    if existing_definition:
        definition_id = existing_definition.get("id")
        print(f"  ✓ Found existing definition '{name}' with ID: {definition_id}")
        return definition_id, True
    else:
        # Create new definition if it doesn't exist
        print(f"  → Definition '{name}' not found, creating new one...")
        definition_id = create_dq_definition(client, name, description, expression, dimension_id)
        if definition_id:
            return definition_id, False
        else:
            return None, False
    

def create_dq_rule(client: CPDClient, rule_name: str, description: str, dimension_id: str, 
                   definition_id: str, asset_id: str, column_names: List[str]) -> Dict:
    """
    Create a data quality rule with column bindings.
    Returns result dictionary with status and details.
    """
    url = f"/data_quality/v3/projects/{project_id}/rules"
    
    # Create field names - use "field" for single column, numbered for multiple
    if len(column_names) == 1:
        field_names = ["field"]  # Single column uses "field"
    else:
        field_names = [f"field{i+1}" for i in range(len(column_names))]  # Multiple columns use field1, field2, etc.
    
    # Create column bindings
    bindings = create_column_bindings(asset_id, field_names, column_names)
    
    payload = {
        "name": rule_name,
        "description": description,
        "dimension": {
            "id": dimension_id
        },
        "input": {
            "definitions": [
                {
                    "definition": {
                        "id": definition_id
                    },
                    "disambiguator": 0,
                    "bindings": bindings
                }
            ]
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
                    "name": "Bound_to_Asset_ID",
                    "type": "metric",
                    "metric": "definition_bound_asset_id"
                },
                {
                    "name": "Data_quality_definition_ID",
                    "type": "metric",
                    "metric": "definition_id"
                },
              #  {
              #      "name": "Bound_to_column",
              #      "type": "metric",
              #      "metric": "definition_bound_column"
              #  },
                {
                    "name": "Data_quality_definition",
                    "type": "metric",
                    "metric": "definition_name"
                },
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
        "bound_expression": "",
        "error_message": ""
    }
    
    if response.status_code == 201:
        data = response.json()
        result["status"] = "SUCCESS"
        result["rule_id"] = data.get("id")
        result["is_valid"] = data.get("is_valid", False)
        
        # Extract bound expression from response
        definitions = data.get("input", {}).get("definitions", [])
        if definitions:
            result["bound_expression"] = definitions[0].get("bound_expression", "")
        
        print(f"  ✓ Created rule '{rule_name}' with ID: {result['rule_id']}")
        print(f"    • Is Valid: {result['is_valid']}")
        print(f"    • Bound Expression: {result['bound_expression']}")
        
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
        print(f"  ✗ Error creating rule '{rule_name}': {error_msg}")
    
    return result


def process_dq_rules_csv(client: CPDClient, input_file: str):
    """
    Process CSV file to create data quality definitions and rules.
    Expected CSV columns:
    0: Data quality rule
    1: Description  
    2: Data quality dimension
    3: Data quality definitions
    4: Data quality definitions rule expression
    5: Asset name
    6: Fields to bind
    """
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{input_file.replace('.csv', '')}_out_{timestamp}.csv"   
    
    print(f"Input file: {input_file}")
    print(f"Output file: {output_filename}")
    
    # Preload dimensions and definitions
    get_data_quality_dimensions(client)
    get_all_definitions(client)
    
    results_data = []
    
    try:
        with open(input_file) as csvfile:
            reader = csv.reader(csvfile, skipinitialspace=True, delimiter=',')
            next(reader)  # Skip header row
            for row_num, row in enumerate(reader, 1):
                if len(row) < 7:  # Ensure we have all required columns
                    print(f"WARNING: Row {row_num} has insufficient columns, skipping")
                    continue
                
                rule_name = row[0].strip()
                description = row[1].strip()
                dimension_name = row[2].strip()
                definition_name = row[3].strip()
                rule_expression = row[4].strip()
                asset_name = row[5].strip()
                fields = row[6].strip()
                
                print(f"\nProcessing row {row_num}: {rule_name}")
                
                # Initialize result tracking
                result_row = {
                    'original_row': row,
                    'row_number': row_num,
                    'rule_name': rule_name,
                    'asset_name': asset_name,
                    'definition_status': '',
                    'definition_id': '',
                    'rule_status': '',
                    'rule_id': '',
                    'is_valid': '',
                    'bound_expression': '',
                    'error_message': ''
                }
                
                try:
                    # Get dimension ID
                    dimension_id = get_dimension_id_by_name(client, dimension_name)
                    if not dimension_id:
                        error_msg = f"Dimension '{dimension_name}' not found"
                        print(f"  ✗ {error_msg}")
                        result_row['definition_status'] = f"ERROR: {error_msg}"
                        result_row['rule_status'] = f"ERROR: {error_msg}"
                        result_row['error_message'] = error_msg
                        results_data.append(result_row)
                        continue
                    
                    # Get asset ID
                    asset_id = getAssetByName(client, asset_name)
                    
                    # Parse bound expressions to get column names
                    column_names = parse_bound_expressions(fields)
                    if not column_names:
                        error_msg = "No valid column names found in bound expressions"
                        print(f"  ✗ {error_msg}")
                        result_row['definition_status'] = f"ERROR: {error_msg}"
                        result_row['rule_status'] = f"ERROR: {error_msg}"
                        result_row['error_message'] = error_msg
                        results_data.append(result_row)
                        continue
                    
                    # Validate all columns exist in the asset
                    print(f"  → Validating columns: {column_names}")
                    invalid_columns = []
                    for column_name in column_names:
                        if not validateColumn(client, asset_id, column_name):
                            invalid_columns.append(column_name)
                    
                    if invalid_columns:
                        error_msg = f"Column(s) not found in asset '{asset_name}': {', '.join(invalid_columns)}"
                        print(f"  ✗ {error_msg}")
                        result_row['definition_status'] = f"ERROR: {error_msg}"
                        result_row['rule_status'] = f"ERROR: {error_msg}"
                        result_row['error_message'] = error_msg
                        results_data.append(result_row)
                        continue
                    
                    print(f"  ✓ All columns validated in asset '{asset_name}'")
                    
                    # Use the DQ expression directly from the CSV file
                    dq_expression = rule_expression.strip()
                    
                    # Step 1: Get or create definition
                    print(f"  → Getting or creating definition: {definition_name}")
                    definition_result = get_or_create_dq_definition(client, definition_name, definition_name, 
                                                                   dq_expression, dimension_id)
                    
                    if definition_result[0]:  # definition_id exists
                        definition_id, was_existing = definition_result
                        if was_existing:
                            result_row['definition_status'] = "SUCCESS - Found existing"
                        else:
                            result_row['definition_status'] = "SUCCESS - Created new"
                        result_row['definition_id'] = definition_id
                        
                        # Step 2: Create rule
                        print(f"  → Creating rule: {rule_name}")
                        rule_result = create_dq_rule(client, rule_name, description, dimension_id,
                                                   definition_id, asset_id, column_names)
                        
                        result_row['rule_status'] = rule_result['status']
                        result_row['rule_id'] = rule_result['rule_id'] or ''
                        result_row['is_valid'] = str(rule_result['is_valid'])
                        result_row['bound_expression'] = rule_result['bound_expression']
                        result_row['error_message'] = rule_result['error_message']
                        
                    else:
                        result_row['definition_status'] = "ERROR: Failed to get/create definition"
                        result_row['rule_status'] = "SKIPPED: Definition get/create failed"
                        result_row['error_message'] = "Definition get/create failed"
                
                except AssertionError as msg:
                    error_msg = f"Asset error: {msg}"
                    print(f"  ✗ {error_msg}")
                    result_row['definition_status'] = f"ERROR: {error_msg}"
                    result_row['rule_status'] = f"ERROR: {error_msg}"
                    result_row['error_message'] = error_msg
                
                except Exception as e:
                    error_msg = f"Processing error: {e}"
                    print(f"  ✗ {error_msg}")
                    result_row['definition_status'] = f"ERROR: {error_msg}"
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
    print(f"\nWriting results to: {output_filename}")
    try:
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            header = [
                'Data Quality Rule', 'Description', 'Data Quality Dimension',
                'Data Quality Definitions', 'Data Quality Definitions Rule Expression',
                'Asset Name', 'Bound Expressions',
                'Definition Status', 'Definition ID', 'Rule Status', 'Rule ID',
                'Is Valid', 'Bound Expression', 'Error Message'
            ]
            writer.writerow(header)
            
            # Write data rows
            for result_row in results_data:
                row = result_row['original_row']
                output_row = row + [
                    result_row['definition_status'],
                    result_row['definition_id'],
                    result_row['rule_status'],
                    result_row['rule_id'],
                    result_row['is_valid'],
                    result_row['bound_expression'],
                    result_row['error_message']
                ]
                writer.writerow(output_row)
        
        # Print summary
        total_rows = len(results_data)
        successful_definitions = sum(1 for r in results_data if r['definition_status'].startswith("SUCCESS"))
        successful_rules = sum(1 for r in results_data if r['rule_status'] == "SUCCESS")
        existing_definitions = sum(1 for r in results_data if "Found existing" in r.get('definition_status', ''))
        new_definitions = sum(1 for r in results_data if "Created new" in r.get('definition_status', ''))
        
        print(f"\nSUMMARY:")
        print(f"Total rows processed: {total_rows}")
        print(f"Successful definitions (found/created): {successful_definitions}")
        print(f"  - Existing definitions found: {existing_definitions}")
        print(f"  - New definitions created: {new_definitions}")
        print(f"Successful rules: {successful_rules}")
        print(f"Results saved to: {output_filename}")
        
    except Exception as e:
        print(f"ERROR writing results CSV: {e}")


def main_dq_rules(input_file):
    """Main function to process data quality rules CSV"""
    print("="*60)
    print("DATA QUALITY RULES PROCESSING")
    print("="*60)
    
    with CPDClient() as client:
        # Preload dimensions and definitions
        get_data_quality_dimensions(client)
        get_all_definitions(client)
        
        print("\n" + "="*60)
        print("PROCESSING DATA QUALITY RULES CSV")
        print("="*60)
        
        process_dq_rules_csv(client, input_file)
        
        print("\n" + "="*60)
        print("DATA QUALITY RULES PROCESS COMPLETED")
        print("="*60)


# Example usage
if __name__ == "__main__":
    # Expected CSV format (with header - skipped):
    # DQ Rule,DQ Rule Description,DQ Dimension,DQ Definition Name,DQ Definition Expression,Asset Name to Bind,Field(s) to Bind
    # Example:
    # DQR for Bank Clients,No duplicates allowed,Uniqueness,DQD Uniqueness Single ID,"field UNIQUE",T_BANK_CLIENTS_VW,CLIENT_ID
    #
    # Fields to Bind can be separated by + or ; or | 
    #
    # DQ Definition Expression comes directly from the CSV file, examples:
    # - "field = 'Y' or field = 'N'"
    # - "trim(field) in_reference_list {'Block bounce', 'Hard bounce'}"
    # - "field in_reference_list {'F','D'}"
    # - "val(trim(field)) >= 26668"
    # - "field1 + '|' + field2 UNIQUE"
    #
    main_dq_rules('dq_rules.csv')