import os
import csv
from dotenv import load_dotenv
from datetime import datetime
from cpd_client import CPDClient
from typing import Dict, List, Optional, Tuple

load_dotenv(override=True)

# Environment variables
project_id = os.environ.get('PROJECT_ID')

# Global cache for artifacts
_artifact_cache: Dict[str, List[Dict]] = {}


def _load_artifacts(client: CPDClient, artifact_type: str):
    """Load artifacts into cache if not already loaded"""
    if artifact_type in _artifact_cache:
        return
        
    offset = 0
    batch_size = 10000
    all_results = []
    
    while True:
        payload = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"metadata.artifact_type": artifact_type}},
                        {"term": {"metadata.state": "PUBLISHED"}}
                    ]
                }
            },
            "from": offset,
            "size": batch_size,
            "_source": [
                "metadata.name",
                "categories.primary_category_name",
                "entity.artifacts.global_id",
                "artifact_id"
            ],
        }
        
        response = client.search(payload)
        if response.status_code != 200:
            print(f"Error loading {artifact_type}: {response.status_code}")
            break
            
        data = response.json()
        rows = data.get("rows", [])
        total_hits = data.get("size", 0)
        
        if not rows:
            break
            
        all_results.extend(rows)
        
        if offset + len(rows) >= total_hits:
            break
            
        offset += batch_size

    _artifact_cache[artifact_type] = all_results
    print(f"Loaded {len(all_results)} {artifact_type} artifacts")


def lookup_by_name_and_category(artifact_type: str, name: str, primary_category: str) -> Optional[Tuple[str, str]]:
    """
    Lookup artifact by name and primary category, return (global_id, artifact_id)
    Returns None if not found
    """
    
    for artifact in _artifact_cache[artifact_type]:
        artifact_name = artifact.get("metadata", {}).get("name", "")
        artifact_category = artifact.get("categories", {}).get("primary_category_name", "")
        
        if artifact_name == name and artifact_category == primary_category:
            global_id = artifact.get("entity", {}).get("artifacts", {}).get("global_id", "")
            artifact_id = artifact.get("artifact_id", "")
            return (global_id, artifact_id)
    
    return None


def get_term_id(primary_category: str, term_name: str) -> Optional[str]:
    """
    Search for a business term by exact name and category match.
    Returns the global_id if found, None otherwise.
    """
    result = lookup_by_name_and_category("glossary_term", term_name, primary_category)
    return result[0] if result else None


def get_classification_id(primary_category: str, classification_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Search for a classification by exact name and category match.
    Returns both the artifact_id and global_id if found, (None, None) otherwise.
    """
    result = lookup_by_name_and_category("classification", classification_name, primary_category)
    return (result[1], result[0]) if result else (None, None)


def get_data_class_id(primary_category: str, data_class_name: str) -> Optional[str]:
    """
    Search for a data class by exact name and category match.
    Returns the global_id if found, None otherwise.
    """
    result = lookup_by_name_and_category("data_class", data_class_name, primary_category)
    return result[0] if result else None


def getAssetByName(client: CPDClient, name: str) -> str:
    """
    This function retrieves the ID of an asset in a project based on its name.
    """
    url = f"/v2/asset_types/data_asset/search?project_id={project_id}&allow_metadata_on_dpr_deny=true"
    
    payload = {
        "query": f"asset.name:{name}",
        "limit": 20
    }
    
    response = client.post(url, json=payload)
    
    if response.status_code != 200:
        raise ValueError(f"Error scanning project: {response.text}")
    else:
        response_data = response.json()
        if response_data['total_rows'] != 1:
            raise AssertionError(f'Asset {name} is either not found or duplicated')
        return response_data['results'][0]['metadata']['asset_id']


def checkColumnInfoExists(client: CPDClient, asset_id: str) -> bool:
    """Check if column_info exists in the asset entity structure."""
    url = f"/v2/assets/{asset_id}?project_id={project_id}&allow_metadata_on_dpr_deny=true"
    
    response = client.get(url)
    
    if response.status_code != 200:
        return False
    
    asset_data = response.json()
    
    # Check if column_info exists in entity
    entity = asset_data.get('entity', {})
    return 'column_info' in entity


def checkSpecificColumnExists(client: CPDClient, asset_id: str, column_name: str) -> bool:
    """Check if a specific column exists within column_info."""
    url = f"/v2/assets/{asset_id}?project_id={project_id}&allow_metadata_on_dpr_deny=true"
    
    response = client.get(url)
    
    if response.status_code != 200:
        return False
    
    asset_data = response.json()
    
    # Check if the specific column exists in column_info
    column_info = asset_data.get('entity', {}).get('column_info', {})
    return column_name in column_info


def updateColumnInfoBulk(client: CPDClient, asset_id: str, asset_name: str, column_name: str, column_data: Dict):
    """Update column_info but preserve existing metadata."""

    url = f"/v2/assets/bulk_patch?project_id={project_id}"
    
    # Check if column_info exists
    column_info_exists = checkColumnInfoExists(client, asset_id)
    
    operations = []
    
    if not column_info_exists:
        # Case 1: column_info doesn't exist - create it with the column data
        operations.append({
            "op": "add",
            "path": "/entity/column_info",
            "value": {
                column_name: column_data
            }
        })
        print(f"  → Creating column_info with {column_name}")
    else:
        # Case 2: column_info exists - check if specific column exists
        specific_column_exists = checkSpecificColumnExists(client, asset_id, column_name)
        
        if not specific_column_exists:
            # Case 2a: column_info exists but this column doesn't - create the column
            operations.append({
                "op": "add",
                "path": f"/entity/column_info/{column_name}",
                "value": column_data
            })
            print(f"  → Creating new column {column_name} in existing column_info")
        else:
            # Case 2b: both column_info and column exist - update specific attributes granularly
            print(f"  → Updating existing column {column_name}")
            
            # Add description operation if description exists
            if 'description' in column_data:
                operations.append({
                    "op": "add",
                    "path": f"/entity/column_info/{column_name}/column_description",
                    "value": column_data['description']
                })
                print(f"    • Adding description")
            
            # Add term assignment operation if terms exist
            if 'column_terms' in column_data:
                operations.append({
                    "op": "add",
                    "path": f"/entity/column_info/{column_name}/column_terms",
                    "value": column_data['column_terms']
                })
                print(f"    • Adding terms: {[t['term_display_name'] for t in column_data['column_terms']]}")
            
            # Add classification assignment operation if classifications exist
            if 'column_classifications' in column_data:
                operations.append({
                    "op": "add", 
                    "path": f"/entity/column_info/{column_name}/column_classifications",
                    "value": column_data['column_classifications']
                })
                print(f"    • Adding classifications: {[c['name'] for c in column_data['column_classifications']]}")
            
            # Add data class assignment operation if data class exists
            if 'data_class' in column_data:
                operations.append({
                    "op": "add",
                    "path": f"/entity/column_info/{column_name}/data_class", 
                    "value": column_data['data_class']
                })
                print(f"    • Adding data class: {column_data['data_class']['selected_data_class']['name']}")
            
            # Add tag assignment operation if tags exist
            if 'column_tags' in column_data:
                operations.append({
                    "op": "add",
                    "path": f"/entity/column_info/{column_name}/column_tags",
                    "value": column_data['column_tags']
                })
                print(f"    • Adding tags: {column_data['column_tags']}")
    
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
                    print(f"✓ Successfully updated {asset_name}.{column_name}")
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


def preload_all_artifacts(client: CPDClient):
    """Preload all artifact types into cache at the beginning"""
    print("="*60)
    print("PRELOADING ALL ARTIFACTS INTO CACHE")
    print("="*60)
    
    artifact_types = ["glossary_term", "classification", "data_class"]
    
    for artifact_type in artifact_types:
        _load_artifacts(client, artifact_type)


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
    
    with CPDClient() as client:
        # Preload all artifacts into cache
        preload_all_artifacts(client)
        
        print("\n" + "="*60)
        print("PROCESSING CSV FILE")
        print("="*60)
        
        # Read CSV and process each row individually
        results_data = []
        
        try:
            with open(input_filename) as csv_file:
                reader = csv.reader(csv_file, skipinitialspace=True, delimiter=',')
                
                # Read and validate header
                try:
                    header_row = next(reader)
                    print(f"CSV Columns: {header_row}")
                    
                    if len(header_row) < 14:
                        print(f"ERROR: Header has only {len(header_row)} columns, need at least 14")
                        print("Expected: Asset Name,Column Name,Column Description,Term Name,Term Category,Term2 Name,Term2 Category,Classification,Classification Category,Classification2,Classification2 Category,Data Class Name,Data Class Category,Tags")
                        return
                        
                except StopIteration:
                    print("ERROR: File is empty or has no header")
                    return
                
                # Process data rows
                for row_num, row in enumerate(reader, 2):  # Start from row 2 since row 1 is header
                    if len(row) < 14:
                        print(f"WARNING: Row {row_num} has insufficient columns ({len(row)}/14), skipping")
                        continue
                    
                    asset_name = row[0]
                    column_name = row[1]
                    column_description = row[2]
                    term_name = row[3]
                    term_category = row[4]
                    term2_name = row[5]
                    term2_category = row[6]
                    classification = row[7]
                    classification_category = row[8]
                    classification2 = row[9]
                    classification2_category = row[10]
                    data_class_name = row[11]
                    data_class_category = row[12]
                    tags_string = row[13]
                    
                    print(f"\nProcessing row {row_num}: {asset_name}.{column_name}")
                    
                    # Initialize result tracking
                    term_result = "SKIPPED"
                    term2_result = "SKIPPED"
                    classification_result = "SKIPPED"
                    classification2_result = "SKIPPED"
                    data_class_result = "SKIPPED"
                    
                    try:
                        # Get asset ID
                        asset_id = getAssetByName(client, asset_name)
                        
                        # Validate column exists
                        if not validateColumn(client, asset_id, column_name):
                            error_msg = f"Column '{column_name}' is not found in asset {asset_name}"
                            print(f"  ✗ {error_msg}")
                            results_data.append(row + ["FAILED: Column not found", "FAILED: Column not found", "FAILED: Column not found", "FAILED: Column not found", "FAILED: Column not found", f"FAILED: {error_msg}"])
                            continue
                        
                        # Build column data structure
                        column_data = {}
                        
                        # Process description assignment
                        if column_description.strip():
                            column_data['description'] = column_description.strip()
                            print(f"  ✓ Description: {column_description[:50]}{'...' if len(column_description) > 50 else ''}")
                        
                        # Process term assignments
                        terms = []
                        
                        # Process first term
                        if term_name and term_category:
                            term_global_id = get_term_id(term_category, term_name)
                            if term_global_id:
                                terms.append({
                                    'term_display_name': term_name,
                                    'term_id': term_global_id
                                })
                                term_result = "SUCCESS"
                                print(f"  ✓ Term 1: {term_name} (Category: {term_category})")
                            else:
                                term_result = "FAILED: Not found"
                                print(f"  ✗ Term 1 '{term_name}' with category '{term_category}' not found")
                        else:
                            term_result = "SKIPPED"
                        
                        # Process second term
                        if term2_name and term2_category:
                            term2_global_id = get_term_id(term2_category, term2_name)
                            if term2_global_id:
                                terms.append({
                                    'term_display_name': term2_name,
                                    'term_id': term2_global_id
                                })
                                term2_result = "SUCCESS"
                                print(f"  ✓ Term 2: {term2_name} (Category: {term2_category})")
                            else:
                                term2_result = "FAILED: Not found"
                                print(f"  ✗ Term 2 '{term2_name}' with category '{term2_category}' not found")
                        else:
                            term2_result = "SKIPPED"
                        
                        # Add terms to column data if any exist
                        if terms:
                            column_data['column_terms'] = terms
                        
                        classifications = []
                        
                        # Process first classification
                        if classification and classification_category:
                            artifact_id, global_id = get_classification_id(classification_category, classification)
                            if artifact_id and global_id:
                                classifications.append({
                                    'id': artifact_id,
                                    'global_id': global_id,
                                    'name': classification
                                })
                                classification_result = "SUCCESS"
                                print(f"  ✓ Classification 1: {classification} (Category: {classification_category})")
                            else:
                                classification_result = "FAILED: Not found"
                                print(f"  ✗ Classification 1 '{classification}' with category '{classification_category}' not found")
                        else:
                            classification_result = "SKIPPED"
                        
                        # Process second classification
                        if classification2 and classification2_category:
                            artifact_id, global_id = get_classification_id(classification2_category, classification2)
                            if artifact_id and global_id:
                                classifications.append({
                                    'id': artifact_id,
                                    'global_id': global_id,
                                    'name': classification2
                                })
                                classification2_result = "SUCCESS"
                                print(f"  ✓ Classification 2: {classification2} (Category: {classification2_category})")
                            else:
                                classification2_result = "FAILED: Not found"
                                print(f"  ✗ Classification 2 '{classification2}' with category '{classification2_category}' not found")
                        else:
                            classification2_result = "SKIPPED"
                        
                        if classifications:
                            column_data['column_classifications'] = classifications
                        
                        # Process data class assignment
                        if data_class_name and data_class_category:
                            data_class_id = get_data_class_id(data_class_category, data_class_name)
                            if data_class_id:
                                column_data['data_class'] = {
                                    'selected_data_class': {
                                        'id': data_class_id,
                                        'name': data_class_name,
                                        'setByUser': True
                                    }
                                }
                                data_class_result = "SUCCESS"
                                print(f"  ✓ Data Class: {data_class_name} (Category: {data_class_category})")
                            else:
                                data_class_result = "FAILED: Not found"
                                print(f"  ✗ Data class '{data_class_name}' with category '{data_class_category}' not found")
                        else:
                            data_class_result = "SKIPPED"
                        
                        # Process tag assignment
                        if tags_string.strip():
                            # Split by pipe and clean up whitespace
                            tag_list = [tag.strip() for tag in tags_string.split('|') if tag.strip()]
                            if tag_list:
                                column_data['column_tags'] = tag_list
                                print(f"  ✓ Tags: {tag_list}")
                        
                        # Update asset if we have valid assignments
                        if column_data:
                            update_status = updateColumnInfoBulk(client, asset_id, asset_name, column_name, column_data)
                            results_data.append(row + [term_result, term2_result, classification_result, classification2_result, data_class_result, update_status])
                        else:
                            results_data.append(row + [term_result, term2_result, classification_result, classification2_result, data_class_result, "SKIPPED: No valid assignments"])
                            print(f"  ! No valid assignments found for {column_name}")
                                                
                    except Exception as e:
                        error_msg = f"Processing error: {e}"
                        print(f"  ✗ {error_msg}")
                        results_data.append(row + ["FAILED: Processing error", "FAILED: Processing error", "FAILED: Processing error", "FAILED: Processing error", "FAILED: Processing error", f"FAILED: {error_msg}"])
        
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
                
                # Write header
                header = [
                    'Asset Name', 'Column Name', 'Column Description', 'Term Name', 'Term Category','Term2 Name', 'Term2 Category',
                    'Classification', 'Classification Category', 'Classification2', 'Classification2 Category',
                    'Data Class Name', 'Data Class Category', 'Tags',
                    'Term Result', 'Term2 Result', 'Classification Result', 'Classification2 Result', 'Data Class Result', 'Update Status'
                ]
                writer.writerow(header)
                
                # Write data rows
                for result_row in results_data:
                    writer.writerow(result_row)
            
            print(f"Results written to: {output_filename}")
            
            # Summary statistics
            total_rows = len(results_data)
            successful_updates = sum(1 for r in results_data if len(r) > 19 and r[19] == "SUCCESS")  # Updated index
            failed_updates = sum(1 for r in results_data if len(r) > 19 and r[19].startswith("FAILED"))
            skipped_updates = sum(1 for r in results_data if len(r) > 19 and r[19].startswith("SKIPPED"))
            error_updates = sum(1 for r in results_data if len(r) > 19 and r[19].startswith("ERROR"))
            
            print(f"\nSUMMARY:")
            print(f"Total rows processed: {total_rows}")
            print(f"✓ Successful updates: {successful_updates}")
            print(f"✗ Failed updates: {failed_updates}")
            print(f"- Skipped updates: {skipped_updates}")
            if error_updates > 0:
                print(f"! Error updates: {error_updates}")

            # Additional breakdown
            if total_rows > 0:
                term_successes = sum(1 for r in results_data if len(r) > 14 and r[14] == "SUCCESS")
                term_skipped = sum(1 for r in results_data if len(r) > 14 and r[14] == "SKIPPED")
                term_failed = sum(1 for r in results_data if len(r) > 14 and r[14].startswith(("FAILED:", "ERROR:")))
                
                term2_successes = sum(1 for r in results_data if len(r) > 15 and r[15] == "SUCCESS")
                term2_skipped = sum(1 for r in results_data if len(r) > 15 and r[15] == "SKIPPED")
                term2_failed = sum(1 for r in results_data if len(r) > 15 and r[15].startswith(("FAILED:", "ERROR:")))
                
                classification_successes = sum(1 for r in results_data if len(r) > 16 and r[16] == "SUCCESS")
                classification_skipped = sum(1 for r in results_data if len(r) > 16 and r[16] == "SKIPPED")
                classification_failed = sum(1 for r in results_data if len(r) > 16 and r[16].startswith(("FAILED:", "ERROR:")))
                
                classification2_successes = sum(1 for r in results_data if len(r) > 17 and r[17] == "SUCCESS")
                classification2_skipped = sum(1 for r in results_data if len(r) > 17 and r[17] == "SKIPPED")
                classification2_failed = sum(1 for r in results_data if len(r) > 17 and r[17].startswith(("FAILED:", "ERROR:")))
                
                data_class_successes = sum(1 for r in results_data if len(r) > 18 and r[18] == "SUCCESS")
                data_class_skipped = sum(1 for r in results_data if len(r) > 18 and r[18] == "SKIPPED")
                data_class_failed = sum(1 for r in results_data if len(r) > 18 and r[18].startswith(("FAILED:", "ERROR:")))
                
                print(f"\nMetadata Assignment Breakdown (✓ Success / - Skipped / ✗ Failed):")
                print(f"Terms 1:           ✓ {term_successes} / - {term_skipped} / ✗ {term_failed}")
                print(f"Terms 2:           ✓ {term2_successes} / - {term2_skipped} / ✗ {term2_failed}")
                print(f"Classifications 1: ✓ {classification_successes} / - {classification_skipped} / ✗ {classification_failed}")
                print(f"Classifications 2: ✓ {classification2_successes} / - {classification2_skipped} / ✗ {classification2_failed}")
                print(f"Data Classes:      ✓ {data_class_successes} / - {data_class_skipped} / ✗ {data_class_failed}")
            
        except Exception as e:
            print(f"ERROR writing results CSV: {e}")

        print("\n" + "="*60)
        print("PROCESS COMPLETED")
        print("="*60)
        print("Column metadata assignment process completed.")
        print(f"Detailed results saved to: {output_filename}")

if __name__ == "__main__":
    # File format (WITH header - will be skipped):
    # Asset Name,Column Name,Column Description,Term Name,Term Category,Term2 Name,Term2 Category,Classification,Classification Category,Classification2,Classification2 Category,Data Class Name,Data Class Category,Tags
    # T_US_STATES,ABBREV,Abbreviation of name,Country Code,Location,State Code,Location,Confidential,[uncategorized],PII,[uncategorized],Country Code,Location Data Classes,TAG1|TAG2

    main(input_filename='col_term_map.csv')