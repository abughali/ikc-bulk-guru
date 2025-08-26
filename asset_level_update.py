import os
import csv
from dotenv import load_dotenv
from datetime import datetime
from cpd_client import CPDClient
from typing import List, Dict, Optional, Tuple

load_dotenv(override=True)

# Constants / Configs
target = 'PROJECT'  # or 'CATALOG'

# Environment variables
project_id = os.environ.get('PROJECT_ID')
catalog_id = os.environ.get('CATALOG_ID')

# Global variable to store current user ID
current_user_id = None

# Global cache for artifacts
_artifact_cache: Dict[str, List[Dict]] = {}

# Global cache for uid -> username mapping
_user_cache: Dict[int, str] = {}


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


def _load_users(client: CPDClient):
    """Load users into cache if not already loaded"""
    if _user_cache:
        return
        
    offset = 0
    batch_size = 100
    
    while True:
        response = client.get("/usermgmt/v1/usermgmt/users", 
                            params={"offset": offset, "limit": batch_size, "include_groups": "false"})
        
        if response.status_code != 200:
            print(f"Error fetching users: {response.status_code} {response.text}")
            break
            
        users = response.json()
        if not users:
            break
        
        for user in users:
            uid = user.get('uid')
            username = user.get('username', '')
            if uid:
                _user_cache[uid] = username
            
        if len(users) < batch_size:
            break
        offset += batch_size
    
    print(f"Loaded {len(_user_cache)} users")


def get_uid(username: str) -> Optional[int]:
    """Get uid by username from cache"""
    for uid, cached_username in _user_cache.items():
        if cached_username == username:
            return uid
    return None  # Username not found


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


def get_classification_id(primary_category: str, classification_name: str) -> Optional[Tuple[str, str]]:
    """
    Search for a classification by exact name and category match.
    Returns (global_id, artifact_id) if found, None otherwise.
    """
    return lookup_by_name_and_category("classification", classification_name, primary_category)


def preload_all_artifacts(client: CPDClient):
    """Preload all artifact types and users into cache at the beginning"""
    print("="*60)
    print("PRELOADING ALL ARTIFACTS AND USERS INTO CACHE")
    print("="*60)
    
    artifact_types = ["glossary_term", "classification"]
    
    for artifact_type in artifact_types:
        _load_artifacts(client, artifact_type)
    
    # Load users for owner assignment
    _load_users(client)


def getCurrentUserInfo(client: CPDClient) -> str:
    """
    Get current user information and return the user ID.
    Returns the UID if successful, '1000330999' (admin) as fallback.
    """
    url = "/usermgmt/v1/user/currentUserInfo"
    
    response = client.get(url)
    
    if response.status_code == 200:
        try:
            user_data = response.json()
            user_id = user_data.get('uid', '1000330999')
            user_name = user_data.get('user_name', 'unknown')
            print(f"✓ Current user: {user_name} (ID: {user_id})")
            return user_id
        except Exception as e:
            print(f"✗ Error parsing user info: {e}")
            return '1000330999'
    else:
        print(f"✗ Failed to get user info: {response.status_code} - {response.text}")
        return '1000330999'


def getAssetByName(client: CPDClient, name: str) -> str:
    """
    This function retrieves the ID of an asset in a project or catalog based on its name.
    """
    # Build query parameters based on target
    if target == 'PROJECT':
        query_param = f"project_id={project_id}"
        context_name = "project"
    elif target == 'CATALOG':
        query_param = f"catalog_id={catalog_id}"
        context_name = "catalog"
    else:
        raise ValueError(f"Invalid target: {target}. Must be 'PROJECT' or 'CATALOG'")
    
    url = f"/v2/asset_types/data_asset/search?{query_param}&allow_metadata_on_dpr_deny=true"
    
    payload = {
        "query": f"asset.name:{name}",
        "limit": 2  # Only need to detect if there are duplicates
    }
    
    response = client.post(url, json=payload)
    
    if response.status_code != 200:
        raise ValueError(f"Error scanning {context_name}: {response.text}")
    else:
        response_data = response.json()
        if response_data['total_rows'] != 1:
            raise AssertionError(f'Asset {name} is either not found or duplicated in {context_name}')
        return response_data['results'][0]['metadata']['asset_id']


def assignAssetOwner(client: CPDClient, asset_id: str, asset_name: str, owner_username: str) -> str:
    """
    Assign owner to an asset using the collaborators endpoint.
    Returns status string indicating success or failure.
    """
    # Get UID for the owner username
    owner_uid = get_uid(owner_username)
    if not owner_uid:
        return f"Username '{owner_username}' not found"
    
    # Build query parameters based on target
    if target == 'PROJECT':
        query_param = f"project_id={project_id}"
    elif target == 'CATALOG':
        query_param = f"catalog_id={catalog_id}"
    else:
        return "Invalid target configuration"
    
    url = f"/v2/assets/{asset_id}/collaborators?{query_param}"
    
    # Build the patch payload
    payload = [
        {
            "op": "add",
            "path": "/metadata/rov/member_roles",
            "value": {
                str(owner_uid): {
                    "user_iam_id": str(owner_uid),
                    "roles": ["OWNER"]
                }
            }
        }
    ]
    
    response = client.patch(url, json=payload)
    
    if response.status_code == 200:
        print(f"✓ Successfully assigned owner '{owner_username}' to {asset_name}")
        return "SUCCESS"
    else:
        # Parse the error response to extract the reason
        error_msg = f"HTTP {response.status_code}: {response.text}"
        print(f"✗ Failed to assign owner to {asset_name}: {error_msg}")
        
        # Try to extract the reason from the JSON error response
        try:
            import json
            error_data = json.loads(response.text)
            if 'errors' in error_data and len(error_data['errors']) > 0:
                error_message = error_data['errors'][0].get('message', '')
                # Look for nested JSON with reason field
                if 'reason":"' in error_message:
                    # Extract the reason from the nested JSON
                    reason_start = error_message.find('reason":"') + 9
                    reason_end = error_message.find('"', reason_start)
                    if reason_end > reason_start:
                        reason = error_message[reason_start:reason_end]
                        return reason
                # Fallback to the main error message
                return error_message
            else:
                return f"HTTP {response.status_code}"
        except:
            # If JSON parsing fails, return HTTP status
            return f"HTTP {response.status_code}"


def updateAsset(client: CPDClient, asset_id: str, asset_name: str, display_name: str = None, description: str = None, 
               tags: List[str] = None, business_terms: List[Dict[str, str]] = None, 
               classifications: List[Dict[str, str]] = None) -> str:
    """
    Update asset display name, description, tags, business terms and classifications using bulk patch endpoint.
    business_terms should be a list of dicts with 'name' and 'category' keys.
    classifications should be a list of dicts with 'name' and 'category' keys.
    Returns status string indicating success or failure.
    """
    # Build query parameters based on target
    if target == 'PROJECT':
        query_param = f"project_id={project_id}"
    elif target == 'CATALOG':
        query_param = f"catalog_id={catalog_id}"
    else:
        raise ValueError(f"Invalid target: {target}. Must be 'PROJECT' or 'CATALOG'")
    
    url = f"/v2/assets/bulk_patch?{query_param}"
    
    # Build the operations array
    operations = []
    
    # Add display name (semantic name) operation if display name provided
    if display_name:
        # Generate current timestamp for assignment_date
        assignment_date = datetime.now().isoformat(timespec='milliseconds') + "Z"
        operations.append({
            "op": "add",
            "path": "/entity/data_asset/semantic_name",
            "value": {
                "expanded_name": display_name,
                "status": "accepted",
                "assignment_date": assignment_date,
                "user": current_user_id
            }
        })
    
    # Add description operation if description provided
    if description:
        operations.append({
            "op": "add",
            "path": "/metadata/description",
            "value": description
        })
    
    # Add tags operation if tags provided
    if tags:
        operations.append({
            "op": "add",
            "path": "/metadata/tags",
            "value": tags
        })
    
    # Add business terms operation if terms provided
    if business_terms:
        # Prepare terms for assignment
        terms_to_assign = []
        
        for term_info in business_terms:
            term_name = term_info.get('name', '').strip()
            term_category = term_info.get('category', '').strip()
            
            if not term_name or not term_category:
                continue
                
            term_global_id = get_term_id(term_category, term_name)
            if term_global_id:
                assignment_date = datetime.now().isoformat(timespec='milliseconds') + "Z"
                terms_to_assign.append({
                    "term_display_name": term_name,
                    "term_id": term_global_id,
                    "user": current_user_id,
                    "assignment_date": assignment_date
                })
            else:
                print(f"  ! Warning: Term '{term_name}' with category '{term_category}' not found, skipping")
        
        if terms_to_assign:
            operations.append({
                "op": "add",
                "path": "/entity/asset_terms",
                "value": {
                    "list": terms_to_assign,
                    "rejected_terms": [],
                    "suggested_terms": []
                }
            })
    
    # Add classifications operation if classifications provided
    if classifications:
        # Prepare classifications for assignment
        classifications_to_assign = []
        
        for classification_info in classifications:
            classification_name = classification_info.get('name', '').strip()
            classification_category = classification_info.get('category', '').strip()
            
            if not classification_name or not classification_category:
                continue
                
            classification_result = get_classification_id(classification_category, classification_name)
            if classification_result:
                global_id, artifact_id = classification_result
                assignment_date = datetime.now().isoformat(timespec='milliseconds') + "Z"
                classifications_to_assign.append({
                    "id": artifact_id,
                    "name": classification_name,
                    "global_id": global_id,
                    "user": current_user_id,
                    "assignment_date": assignment_date                    
                })
            else:
                print(f"  ! Warning: Classification '{classification_name}' with category '{classification_category}' not found, skipping")
        
        if classifications_to_assign:
            operations.append({
                "op": "add",
                "path": "/entity/data_profile",
                "value": {
                    "data_classification_manual": classifications_to_assign
                }
            })
    
    if not operations:
        return "SKIPPED: No display name, description, tags, terms or classifications to update"
    
    # Build the bulk patch payload
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
                    updates = []
                    if display_name:
                        updates.append("display name")
                    if description:
                        updates.append("description")
                    if tags:
                        updates.append(f"tags ({len(tags)} tags)")
                    if business_terms:
                        updates.append(f"business terms ({len(business_terms)} terms)")
                    if classifications:
                        updates.append(f"classifications ({len(classifications)} classifications)")
                    update_summary = " and ".join(updates)
                    print(f"✓ Successfully updated {update_summary} for {asset_name}")
                    return "SUCCESS"
                else:
                    # Extract error details
                    errors = resource.get('errors', [])
                    error_messages = []
                    for error in errors:
                        error_messages.append(f"{error.get('code', 'unknown')}: {error.get('message', 'unknown error')}")
                    
                    error_summary = "; ".join(error_messages) if error_messages else "Unknown error"
                    print(f"✗ Resource error for {asset_name}: Status {resource_status} - {error_summary}")
                    return f"ERROR: Status {resource_status} - {error_summary}"
            else:
                print(f"✗ No resources in response for {asset_name}")
                return "ERROR: No resources in response"
                
        except Exception as e:
            print(f"✗ Error parsing response for {asset_name}: {e}")
            return f"ERROR: Response parsing failed - {e}"
    else:
        error_msg = f"HTTP {response.status_code}: {response.text}"
        print(f"✗ Failed to update {asset_name}: {error_msg}")
        return f"ERROR: {error_msg}"


def main(input_filename):
    """Main execution function"""
    
    # Validate environment variables based on target
    if target == 'PROJECT':
        if not project_id:
            print("ERROR: PROJECT_ID environment variable is required when target is 'PROJECT'")
            return
        context_info = f"Project ID: {project_id}"
    elif target == 'CATALOG':
        if not catalog_id:
            print("ERROR: CATALOG_ID environment variable is required when target is 'CATALOG'")
            return
        context_info = f"Catalog ID: {catalog_id}"
    else:
        print(f"ERROR: Invalid target '{target}'. Must be 'PROJECT' or 'CATALOG'")
        return
    
    # Create output directory if it doesn't exist
    output_dir = "out"
    os.makedirs(output_dir, exist_ok=True)
        
    # Generate output filename based on input filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = os.path.join(output_dir, f"{input_filename.replace('.csv', '')}_{timestamp}.csv")
    
    print(f"Target: {target}")
    print(f"Context: {context_info}")
    print(f"Input file: {input_filename}")
    print(f"Output file: {output_filename}")
    
    with CPDClient() as client:
        # Preload all artifacts and users into cache
        preload_all_artifacts(client)
        
        print("\n" + "="*60)
        print("GETTING CURRENT USER INFORMATION")
        print("="*60)
        
        # Get current user information and store globally
        global current_user_id
        current_user_id = getCurrentUserInfo(client)
        
        print("\n" + "="*60)
        print("PROCESSING ASSET UPDATES")
        print("="*60)
        
        # Read CSV and process each row individually
        results_data = []
        
        try:
            with open(input_filename) as csvfile:
                reader = csv.reader(csvfile, skipinitialspace=True, delimiter=',')
                
                # Skip header row
                header = next(reader, None)
                if header is None:
                    print("ERROR: Empty CSV file")
                    return
                
                print(f"Header found: {header}")
                
                for row_num, row in enumerate(reader, 2):  # Start from 2 since we skipped header
                    if len(row) < 2:  # Ensure we have at least Asset column
                        print(f"WARNING: Row {row_num} has insufficient columns, skipping")
                        continue
                    
                    asset_name = row[0].strip()
                    display_name = row[1].strip() if len(row) > 1 else ""
                    description = row[2].strip() if len(row) > 2 else ""
                    tags_input = row[3].strip() if len(row) > 3 else ""
                    term1_name = row[4].strip() if len(row) > 4 else ""
                    term1_category = row[5].strip() if len(row) > 5 else ""
                    term2_name = row[6].strip() if len(row) > 6 else ""
                    term2_category = row[7].strip() if len(row) > 7 else ""
                    classification1_name = row[8].strip() if len(row) > 8 else ""
                    classification1_category = row[9].strip() if len(row) > 9 else ""
                    classification2_name = row[10].strip() if len(row) > 10 else ""
                    classification2_category = row[11].strip() if len(row) > 11 else ""
                    owner_username = row[12].strip() if len(row) > 12 else ""
                    
                    # Parse tags (pipe-separated)
                    tags = []
                    if tags_input:
                        tags = [tag.strip() for tag in tags_input.split('|') if tag.strip()]
                    
                    # Parse business terms from separate columns
                    business_terms = []
                    if term1_name and term1_category:
                        business_terms.append({
                            'name': term1_name,
                            'category': term1_category
                        })
                    if term2_name and term2_category:
                        business_terms.append({
                            'name': term2_name,
                            'category': term2_category
                        })
                    
                    # Parse classifications from separate columns
                    classifications = []
                    if classification1_name and classification1_category:
                        classifications.append({
                            'name': classification1_name,
                            'category': classification1_category
                        })
                    if classification2_name and classification2_category:
                        classifications.append({
                            'name': classification2_name,
                            'category': classification2_category
                        })
                    
                    print(f"\nProcessing row {row_num}: {asset_name}")
                    
                    # Initialize result tracking for this row
                    result_row = {
                        'row_number': row_num,
                        'asset_name': asset_name,
                        'display_name': display_name,
                        'description': description,
                        'tags_input': tags_input,
                        'tags_parsed': tags,
                        'term1_name': term1_name,
                        'term1_category': term1_category,
                        'term2_name': term2_name,
                        'term2_category': term2_category,
                        'classification1_name': classification1_name,
                        'classification1_category': classification1_category,
                        'classification2_name': classification2_name,
                        'classification2_category': classification2_category,
                        'owner_username': owner_username,
                        'update_status': '',
                        'owner_status': ''
                    }
                    
                    # Skip if no updates to perform
                    if not display_name and not description and not tags and not business_terms and not classifications and not owner_username:
                        result_row['update_status'] = "SKIPPED: No updates to perform"
                        result_row['owner_status'] = "SKIPPED: No owner to assign"
                        print(f"  ! Skipping {asset_name}: No updates to perform")
                        results_data.append(result_row)
                        continue
                    
                    try:
                        # Get asset ID
                        asset_id = getAssetByName(client, asset_name)
                        print(f"  → Found asset ID: {asset_id}")
                        
                        # Show what will be updated
                        updates_preview = []
                        if display_name:
                            updates_preview.append(f"display name: {display_name}")
                        if description:
                            updates_preview.append(f"description: {description[:50]}{'...' if len(description) > 50 else ''}")
                        if tags:
                            updates_preview.append(f"tags: {', '.join(tags)}")
                        if business_terms:
                            term_names = [f"{t['name']} ({t['category']})" for t in business_terms]
                            updates_preview.append(f"business terms: {', '.join(term_names)}")
                        if classifications:
                            classification_names = [f"{c['name']} ({c['category']})" for c in classifications]
                            updates_preview.append(f"classifications: {', '.join(classification_names)}")
                        if owner_username:
                            updates_preview.append(f"owner: {owner_username}")
                        print(f"  → Will update: {' | '.join(updates_preview)}")
                        
                        # Update asset metadata (if any metadata updates needed)
                        if display_name or description or tags or business_terms or classifications:
                            update_status = updateAsset(client, asset_id, asset_name, display_name, description, tags, business_terms, classifications)
                            result_row['update_status'] = update_status
                        else:
                            result_row['update_status'] = "SKIPPED: No metadata updates needed"
                        
                        # Assign owner (if owner specified)
                        if owner_username:
                            owner_status = assignAssetOwner(client, asset_id, asset_name, owner_username)
                            result_row['owner_status'] = owner_status
                        else:
                            result_row['owner_status'] = "SKIPPED: No owner specified"
                        
                    except AssertionError as msg:
                        error_msg = f"Asset error: {msg}"
                        print(f"  ✗ {error_msg}")
                        result_row['update_status'] = f"ERROR: {error_msg}"
                        result_row['owner_status'] = "SKIPPED: Asset not found"
                        
                    except Exception as e:
                        error_msg = f"Processing error: {e}"
                        print(f"  ✗ {error_msg}")
                        result_row['update_status'] = f"ERROR: {error_msg}"
                        result_row['owner_status'] = "SKIPPED: Processing failed"
                    
                    results_data.append(result_row)
        
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
        
        # Write results CSV
        try:
            with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                header = ['Asset', 'Display_Name', 'Description', 'Tags', 'Term1 Name', 'Term1 Category', 'Term2 Name', 'Term2 Category', 
                         'Classification1 Name', 'Classification1 Category', 'Classification2 Name', 'Classification2 Category', 'Owner', 
                         'Metadata Update Status', 'Owner Update Status']
                writer.writerow(header)
                
                # Write data rows
                for result_row in results_data:
                    output_row = [
                        result_row['asset_name'],
                        result_row['display_name'],
                        result_row['description'],
                        result_row['tags_input'],
                        result_row['term1_name'],
                        result_row['term1_category'],
                        result_row['term2_name'],
                        result_row['term2_category'],
                        result_row['classification1_name'],
                        result_row['classification1_category'],
                        result_row['classification2_name'],
                        result_row['classification2_category'],
                        result_row['owner_username'],
                        result_row['update_status'],
                        result_row['owner_status']
                    ]
                    writer.writerow(output_row)
            
            print(f"Results written to: {output_filename}")
            
            # Print summary statistics
            total_rows = len(results_data)
            successful_updates = sum(1 for r in results_data if r['update_status'] == "SUCCESS")
            skipped_updates = sum(1 for r in results_data if r['update_status'].startswith("SKIPPED"))
            failed_updates = total_rows - successful_updates - skipped_updates
            
            successful_owners = sum(1 for r in results_data if r['owner_status'] == "SUCCESS")
            skipped_owners = sum(1 for r in results_data if r['owner_status'].startswith("SKIPPED"))
            failed_owners = total_rows - successful_owners - skipped_owners
            
            print(f"\nSUMMARY:")
            print(f"Total rows processed: {total_rows}")
            print(f"Successful updates: {successful_updates}")
            print(f"Skipped updates: {skipped_updates}")
            print(f"Failed updates: {failed_updates}")
            print(f"Successful owner assignments: {successful_owners}")
            print(f"Skipped owner assignments: {skipped_owners}")
            print(f"Failed owner assignments: {failed_owners}")
            
            # Additional breakdown by update type
            display_name_updates = sum(1 for r in results_data 
                                     if r['update_status'] == "SUCCESS" and r['display_name'])
            description_updates = sum(1 for r in results_data 
                                    if r['update_status'] == "SUCCESS" and r['description'])
            tag_updates = sum(1 for r in results_data 
                             if r['update_status'] == "SUCCESS" and r['tags_parsed'])
            term_updates = sum(1 for r in results_data 
                              if r['update_status'] == "SUCCESS" and (r['term1_name'] or r['term2_name']))
            classification_updates = sum(1 for r in results_data 
                                       if r['update_status'] == "SUCCESS" and (r['classification1_name'] or r['classification2_name']))
            
            print(f"\nUPDATE BREAKDOWN:")
            print(f"Assets with display name updates: {display_name_updates}")
            print(f"Assets with description updates: {description_updates}")
            print(f"Assets with tag updates: {tag_updates}")
            print(f"Assets with business term updates: {term_updates}")
            print(f"Assets with classification updates: {classification_updates}")
            
        except Exception as e:
            print(f"ERROR writing results CSV: {e}")

        print("\n" + "="*60)
        print("PROCESS COMPLETED")
        print("="*60)
        print("Asset update process completed.")
        print(f"Detailed results saved to: {output_filename}")


if __name__ == "__main__":
    # Expected CSV format with header:
    # Asset,Display_Name,Description,Tags,Term1 Name,Term1 Category,Term2 Name,Term2 Category,Classification1 Name,Classification1 Category,Classification2 Name,Classification2 Category,Owner
    # 
    # Where:
    # - Asset: Name of the asset to update
    # - Display_Name: Business display name for the asset (can be empty)
    # - Description: New description for the asset (can be empty)
    # - Tags: Pipe-separated list of tags (e.g. "tag1|tag2|tag3", can be empty)
    # - Term1 Name: First business term name (can be empty)
    # - Term1 Category: First business term category (can be empty)
    # - Term2 Name: Second business term name (can be empty)
    # - Term2 Category: Second business term category (can be empty)
    # - Classification1 Name: First classification name (can be empty)
    # - Classification1 Category: First classification category (can be empty)
    # - Classification2 Name: Second classification name (can be empty)
    # - Classification2 Category: Second classification category (can be empty)
    # - Owner: Username of the asset owner to assign (can be empty)
    #
    # Examples:
    # Customer_Data,"CUSTOMER_MASTER","Customer information and demographics","PII|Finance|Customer","Customer","Location","Personal Data","Data Privacy","Personal Information","Sensitive Data","Confidential","Security","john.doe"
    # Product_Catalog,"PRODUCT_CATALOG","Product catalog with pricing","Product|Catalog","Product","Reference Data","Catalog","Reference Data","Public","General","","","jane.smith"
    # Sales_Data,"","Sales transaction records","","Transaction","Sales","Revenue","Finance","Internal Use","Business","","","admin"
    # User_Profiles,"USER_PROFILE_DATA","User profile information","PII|User","Customer","Location","","","Personal Information","Sensitive Data","Customer Data","Business","bob.wilson"
    
    main(input_filename='assets_to_update.csv')