import os
import csv
from dotenv import load_dotenv
from datetime import datetime
from cpd_client import CPDClient
from typing import Dict, Optional

load_dotenv(override=True)

# Configuration - Set the custom property group to work with
CUSTOM_PROP_GROUP_ID = "data_quality"

# Environment variables
catalog_id = os.environ.get('CATALOG_ID')

def getAssetByName(client: CPDClient, name: str) -> str:
    """
    This function retrieves the ID of an asset in a catalog based on its name.
    """
    url = f"/v2/asset_types/data_asset/search?catalog_id={catalog_id}&allow_metadata_on_dpr_deny=true"
    
    payload = {
        "query": f"asset.name:{name}",
        "limit": 2
    }
    
    response = client.post(url, json=payload)
    
    if response.status_code != 200:
        raise ValueError(f"Error scanning catalog: {response.text}")
    else:
        response_data = response.json()
        if response_data['total_rows'] != 1:
            raise AssertionError(f'Asset {name} is either not found or duplicated')
        return response_data['results'][0]['metadata']['asset_id']


def getAssetData(client: CPDClient, asset_id: str) -> Optional[Dict]:
    """Get asset data once and cache it. Returns None if error."""
    url = f"/v2/assets/{asset_id}?catalog_id={catalog_id}&allow_metadata_on_dpr_deny=true"
    
    response = client.get(url)
    
    if response.status_code != 200:
        print(f"Error getting asset details: {response.text}")
        return None
    
    return response.json()


def analyzeAssetData(asset_data: Dict, custom_prop_group_id: str, column_name: str) -> Dict:
    """Analyze asset data and return all the info we need in one pass."""
    entity = asset_data.get('entity', {})
    
    # Check if column exists in data_asset.columns
    data_asset_columns = entity.get('data_asset', {}).get('columns', [])
    column_exists_in_asset = any(col.get('name') == column_name for col in data_asset_columns)
    
    # Check custom property group
    custom_group = entity.get(custom_prop_group_id, {})
    group_exists = custom_prop_group_id in entity
    
    # Find column in custom property group
    custom_columns = custom_group.get('columns', [])
    column_in_group_index = None
    column_in_group_data = None
    
    for i, column in enumerate(custom_columns):
        if column.get('name') == column_name:
            column_in_group_index = i
            column_in_group_data = column
            break
    
    return {
        'column_exists_in_asset': column_exists_in_asset,
        'group_exists': group_exists,
        'column_in_group_exists': column_in_group_index is not None,
        'column_in_group_index': column_in_group_index,
        'column_in_group_data': column_in_group_data
    }


def updateCustomProperty(client: CPDClient, asset_id: str, asset_name: str, custom_prop_group_id: str, column_name: str, new_properties: Dict, asset_analysis: Dict):
    """Update custom properties."""

    url = f"/v2/assets/bulk_patch?catalog_id={catalog_id}"
    
    print(f"  → Custom column properties update for {asset_name}.{column_name}")
    
    operations = []
    
    if not asset_analysis['group_exists']:
        # Case 1: Custom property group doesn't exist - create it with the column
        column_data = {"name": column_name}
        column_data.update(new_properties)
        
        operations.append({
            "op": "add",
            "path": f"/entity/{custom_prop_group_id}",
            "value": {
                "columns": [column_data]
            }
        })
        print(f"    • Creating {custom_prop_group_id} group with column {column_name}")
    else:
        # Case 2: Custom property group exists
        if not asset_analysis['column_in_group_exists']:
            # Case 2a: Group exists but column doesn't - append the column
            column_data = {"name": column_name}
            column_data.update(new_properties)
            
            operations.append({
                "op": "add",
                "path": f"/entity/{custom_prop_group_id}/columns/-",
                "value": column_data
            })
            print(f"    • Appending new column {column_name} to existing {custom_prop_group_id}")
        else:
            # Case 2b: Both group and column exist - update individual properties
            print(f"    • Updating existing column {column_name}")
            
            column_index = asset_analysis['column_in_group_index']
            
            # Update each property individually
            for property_name, property_value in new_properties.items():
                operations.append({
                    "op": "add",  # "add" operation replaces if exists, creates if doesn't
                    "path": f"/entity/{custom_prop_group_id}/columns/{column_index}/{property_name}",
                    "value": property_value
                })
                print(f"      - {property_name}: {property_value}")
    
    # Build the payload with all operations
    payload = {
        "resources": [
            {
                "asset_id": asset_id,
                "operations": operations
            }
        ]
    }
    
    response = client.post(url, json=payload)
    
    if response.status_code == 200:
        # Parse the response to check individual resource status
        try:
            response_data = response.json()
            resources = response_data.get('resources', [])
            
            if resources and len(resources) > 0:
                resource = resources[0]  # Should only be one resource in our case
                resource_status = resource.get('status', 500)
                
                if resource_status == 200:
                    print(f"✓ Successfully updated {custom_prop_group_id} for {asset_name}.{column_name}")
                    return "SUCCESS"
                else:
                    # Extract error details
                    errors = resource.get('errors', [])
                    error_messages = []
                    for error in errors:
                        error_messages.append(f"{error.get('code', 'unknown')}: {error.get('message', 'unknown error')}")
                    
                    error_summary = "; ".join(error_messages) if error_messages else "Unknown error"
                    print(f"✗ Resource error for {asset_name}.{column_name}: Status {resource_status} - {error_summary}")
                    return f"ERROR: Status {resource_status} - {error_summary}"
            else:
                print(f"✗ No resources in response for {asset_name}.{column_name}")
                return "ERROR: No resources in response"
                
        except Exception as e:
            print(f"✗ Error parsing response for {asset_name}.{column_name}: {e}")
            return f"ERROR: Response parsing failed - {e}"
    else:
        print(f"✗ HTTP error updating {asset_name}.{column_name}: {response.status_code} - {response.text}")
        return f"ERROR: HTTP {response.status_code}"


def getCustomAttributeSchemas(client: CPDClient) -> Dict:
    """Fetch all custom attribute schemas from the CPD instance."""

    url = f"/v2/asset_types?bss_account_id=999&custom_attributes=true"
    
    print(f"Fetching custom attribute groups")
    response = client.get(url)
    
    if response.status_code != 200:
        raise ValueError(f"Error fetching custom attribute schemas: {response.status_code} - {response.text}")
    
    schema_data = response.json()
    
    # Build a lookup dictionary: group_id -> schema
    schemas = {}
    
    for resource in schema_data.get('resources', []):
        group_id = resource.get('name')
        if group_id and resource.get('is_column_custom_attribute', False):
            schemas[group_id] = resource
            
            # Get display name from localized_metadata_attributes
            localized_attrs = resource.get('localized_metadata_attributes', {})
            name_attrs = localized_attrs.get('name', {})
            name = name_attrs.get('default', group_id)
            
            print(f"Found: {group_id} (name: {name})")
    
    return schemas


def validateCustomAttribute(custom_prop_group_id: str, schemas: Dict) -> Dict:
    """Validate that the custom attribute group exists and return its schema."""
    if custom_prop_group_id not in schemas:
        available_groups = list(schemas.keys())
        raise ValueError(f"Custom attribute group '{custom_prop_group_id}' not found. Available: {available_groups}")
    
    return schemas[custom_prop_group_id]


def getFieldSchema(custom_attr_schema: Dict, custom_prop_id: str) -> Dict:
    """Get the schema for a specific custom property ID within the custom attribute."""
    properties = custom_attr_schema.get('properties', {})
    
    # Look for the property by custom_prop_id
    for prop_key, prop_schema in properties.items():
        if prop_key == custom_prop_id:
            return prop_schema
    
    # If not found, check if any fields have a custom_prop_id field
    fields = custom_attr_schema.get('fields', [])
    for field in fields:
        if field.get('key') == custom_prop_id:
            # Found the field, now get its property schema
            if custom_prop_id in properties:
                return properties[custom_prop_id]
    
    available_field_keys = [f.get('key') for f in fields]
    raise ValueError(f"Custom property ID '{custom_prop_id}' not found. Available field keys: {available_field_keys}")


def parseValueFromSchema(value_str: str, custom_prop_id: str, field_schema: Dict):
    """Parse string value to appropriate type based on the actual schema definition."""
    if not value_str.strip():
        return None
    
    value_str = value_str.strip()
    field_type = field_schema.get('type', 'string').lower()
    
    try:
        if field_type == 'integer':
            return int(value_str)
        
        elif field_type == 'number':
            return float(value_str)
        
        elif field_type == 'date':
            # If it's already in ISO format, use as-is
            if 'T' in value_str and value_str.endswith('Z'):
                return value_str
            # Otherwise try to parse and convert to ISO format
            from datetime import datetime
            parsed_date = datetime.strptime(value_str, '%Y-%m-%d')
            return parsed_date.strftime('%Y-%m-%dT00:00:00.000')
        
        elif field_type == 'enumeration':
            # Check if value is in allowed values
            allowed_values = [v.get('name') for v in field_schema.get('values', [])]
            if value_str in allowed_values:
                return {"name": value_str, "description": ""}
            else:
                print(f"    ! Warning: '{value_str}' is not a valid {custom_prop_id} value. Allowed: {allowed_values}")
                return None
            
        elif field_type == 'user_group':
            return {"id": value_str, "type": "user"}
            
        else:  # string, text, or unknown types
            return value_str
            
    except ValueError as e:
        print(f"    ! Warning: Cannot parse '{value_str}' as {field_type} for {custom_prop_id}: {e}")
        return None


def main(input_filename):
    """Main execution function"""
    
    # Create output directory
    output_dir = "out"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename based on input filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_filename = os.path.splitext(os.path.basename(input_filename))[0]
    output_filename = os.path.join(output_dir, f"{base_filename}_{timestamp}.csv")
    
    print(f"Input file: {input_filename}")
    print(f"Output file: {output_filename}")
    print(f"Target custom property group: {CUSTOM_PROP_GROUP_ID}")
    print("Validating against defined custom attribute from CPD")
    
    with CPDClient() as client:
        print("\n" + "="*60)
        print("LOADING CUSTOM ATTRIBUTE SCHEMAS")
        print("="*60)
        
        # Fetch all custom attribute schemas
        try:
            custom_attr_schemas = getCustomAttributeSchemas(client)
            print(f"Loaded {len(custom_attr_schemas)} custom attribute schemas")            
            # Validate the configured custom property group
            custom_attr_schema = validateCustomAttribute(CUSTOM_PROP_GROUP_ID, custom_attr_schemas)
        except Exception as e:
            print(f"ERROR: {e}")
            print(f"Please check CUSTOM_PROP_GROUP_ID = '{CUSTOM_PROP_GROUP_ID}' in script configuration")
            return
        
        print("\n" + "="*60)
        print(f"PROCESSING CSV FILE FOR '{CUSTOM_PROP_GROUP_ID}' PROPERTIES")
        print("="*60)
        
        # Read CSV and process each row
        results_data = []
        
        try:
            with open(input_filename) as csv_file:
                reader = csv.reader(csv_file, skipinitialspace=True, delimiter=',')
                
                # Read and validate header
                try:
                    header_row = next(reader)
                    print(f"CSV Columns: {header_row}")
                    
                    if len(header_row) < 2:
                        print(f"ERROR: Header has only {len(header_row)} columns, need at least 2 (Asset Name, Column Name)")
                        return
                    
                    # Expected format: Asset Name, Column Name, {custom_prop_id1}, {custom_prop_id2}, ...
                    expected_start = ["Asset Name", "Column Name"]
                    if header_row[:2] != expected_start:
                        print(f"ERROR: First two columns must be 'Asset Name' and 'Column Name'")
                        print(f"Found: {header_row[:2]}")
                        return
                    
                    # Extract custom property IDs from remaining columns
                    custom_prop_ids = []
                    for col in header_row[2:]:
                        col = col.strip()
                        if col:
                            custom_prop_ids.append(col)
                        else:
                            print(f"WARNING: Empty column header found, ignoring")
                    
                    if not custom_prop_ids:
                        print("ERROR: No custom property ID columns found")
                        return
                    
                    print(f"Working with custom property group: {CUSTOM_PROP_GROUP_ID}")
                    print(f"Custom property IDs to process: {custom_prop_ids}")
                    
                    # Validate all custom property IDs exist in the target schema
                    for custom_prop_id in custom_prop_ids:
                        try:
                            field_schema = getFieldSchema(custom_attr_schema, custom_prop_id)
                            field_type = field_schema.get('type', 'unknown')
                            print(f"  ✓ Property '{custom_prop_id}' ({field_type})")
                        except ValueError as e:
                            print(f"  ✗ {e}")
                            return
                        
                except StopIteration:
                    print("ERROR: File is empty or has no header")
                    return
                
                # Process each row
                for row_num, row in enumerate(reader, 2):  # Start from row 2 since row 1 is header
                    if len(row) < 2:
                        print(f"WARNING: Row {row_num} has insufficient columns ({len(row)}), skipping")
                        results_data.append(row + ["SKIPPED - Insufficient columns"] + ["SKIPPED" for _ in custom_prop_ids])
                        continue
                    
                    asset_name = row[0].strip()
                    column_name = row[1].strip()
                    
                    if not asset_name or not column_name:
                        print(f"WARNING: Row {row_num} missing asset name or column name, skipping")
                        results_data.append(row + ["SKIPPED - Missing asset/column name"] + ["SKIPPED" for _ in custom_prop_ids])
                        continue
                    
                    print(f"\nProcessing Row {row_num}: {asset_name}.{column_name}")
                    
                    # Build new properties dictionary for this column
                    new_properties = {}
                    field_results = []
                    has_valid_properties = False
                    
                    # Process each property field
                    for i, custom_prop_id in enumerate(custom_prop_ids):
                        col_index = 2 + i
                        if col_index < len(row):
                            raw_value = row[col_index].strip()
                            if raw_value:
                                # Get the field schema for this custom property
                                try:
                                    field_schema = getFieldSchema(custom_attr_schema, custom_prop_id)
                                    parsed_value = parseValueFromSchema(raw_value, custom_prop_id, field_schema)
                                    if parsed_value is not None:
                                        new_properties[custom_prop_id] = parsed_value
                                        field_results.append("SUCCESS")
                                        has_valid_properties = True
                                    else:
                                        field_results.append("FAILED - Invalid value")
                                except ValueError as e:
                                    field_results.append(f"FAILED - Schema error: {e}")
                            else:
                                field_results.append("SKIPPED - Empty")
                        else:
                            field_results.append("SKIPPED - No column")
                    
                    # Only proceed if we have at least one valid property to set
                    if not has_valid_properties:
                        print(f"  → No valid properties to set for {asset_name}.{column_name}")
                        results_data.append(row + ["SKIPPED - No valid properties"] + field_results)
                        continue
                    
                    try:
                        # Get asset ID
                        asset_id = getAssetByName(client, asset_name)
                        
                        # Get asset data once and analyze everything
                        asset_data = getAssetData(client, asset_id)
                        if asset_data is None:
                            results_data.append(row + ["FAILED - Could not retrieve asset data"] + field_results)
                            continue
                        
                        asset_analysis = analyzeAssetData(asset_data, CUSTOM_PROP_GROUP_ID, column_name)
                        
                        # Validate column exists in the asset
                        if not asset_analysis['column_exists_in_asset']:
                            print(f"  ✗ Column {column_name} not found in asset {asset_name}")
                            results_data.append(row + ["FAILED - Column not found"] + field_results)
                            continue
                        
                        print(f"  ✓ Column {column_name} exists in asset {asset_name}")
                        
                        update_status = updateCustomProperty(client, asset_id, asset_name, CUSTOM_PROP_GROUP_ID, column_name, new_properties, asset_analysis)
                        results_data.append(row + [update_status] + field_results)
                                                
                    except Exception as e:
                        error_msg = f"Processing error: {e}"
                        print(f"  ✗ {error_msg}")
                        results_data.append(row + [f"FAILED: {error_msg}"] + ["ERROR" for _ in custom_prop_ids])
        
        except FileNotFoundError:
            print(f"ERROR: {input_filename} file not found")
            return
        except Exception as e:
            print(f"ERROR reading CSV file: {e}")
            return
        
        print(f"\nProcessed {len(results_data)} rows from CSV")

        print("\n" + "="*60)
        print("WRITING RESULTS CSV")
        print("="*60)
        
        try:
            with open(output_filename, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                
                # Write header - original columns + status + field results
                result_header = header_row + ["Update Status"] + [f"{prop_id} Result" for prop_id in custom_prop_ids]
                writer.writerow(result_header)
                
                # Write data rows
                for result_row in results_data:
                    writer.writerow(result_row)
            
            print(f"Results written to: {output_filename}")
            
            # Summary statistics
            total_rows = len(results_data)
            successful_updates = sum(1 for r in results_data if len(r) > len(header_row) and r[len(header_row)] == "SUCCESS")
            failed_updates = sum(1 for r in results_data if len(r) > len(header_row) and r[len(header_row)].startswith("FAILED"))
            skipped_updates = sum(1 for r in results_data if len(r) > len(header_row) and r[len(header_row)].startswith("SKIPPED"))
            error_updates = sum(1 for r in results_data if len(r) > len(header_row) and r[len(header_row)].startswith("ERROR"))
            
            print(f"\nSUMMARY:")
            print(f"Total rows processed: {total_rows}")
            print(f"✓ Successful updates: {successful_updates}")
            print(f"✗ Failed updates: {failed_updates}")
            print(f"- Skipped updates: {skipped_updates}")
            if error_updates > 0:
                print(f"! Error updates: {error_updates}")

            
        except Exception as e:
            print(f"ERROR writing results CSV: {e}")

        print("\n" + "="*60)
        print("PROCESS COMPLETED")
        print("="*60)
        print(f"'{CUSTOM_PROP_GROUP_ID}' properties assignment process completed.")
        print(f"Detailed results saved to: {output_filename}")


if __name__ == "__main__":
    # Expected CSV format (WITH header):
    # asset name,column name,custom_prop_id1,custom_prop_id2,custom_prop_id3,...
    # 
    # Example: You have a column-level custom property group with the identifier "data_quality". 
    # This group has three custom properties with the identifiers: score, purpose, primary_key
    # Input CSV:
    # asset name,column name,score,purpose,primary_key
    # T_US_STATES,STATE,80,No purpose,Yes
    # T_US_STATES,ABBREV,75,Abbreviation,No
    # 
    # Configuration:
    # - Set CUSTOM_PROP_GROUP_ID at the top of this script to target different groups
    # - One custom property group per execution
    # 
    # The script will:
    # 1. Validate CUSTOM_PROP_GROUP_ID exists in CPD custom attribute groups
    # 2. Validate all custom property IDs in CSV header exist in that group's schema
    # 3. Parse values according to the actual schema definitions (integer, date, list, etc.)
    #
    main(input_filename='column_properties.csv')