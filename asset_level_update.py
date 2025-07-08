import os
import csv
from dotenv import load_dotenv
from datetime import datetime
from cpd_client import CPDClient
from typing import List

load_dotenv(override=True)

# Constants / Configs
target = 'PROJECT'  # or 'CATALOG'

# Environment variables
project_id = os.environ.get('PROJECT_ID')
catalog_id = os.environ.get('CATALOG_ID')

# Global variable to store current user ID
current_user_id = None


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


def updateAssetDescription(client: CPDClient, asset_id: str, asset_name: str, display_name: str = None, description: str = None, tags: List[str] = None) -> str:
    """
    Update asset display name, description and tags using bulk patch endpoint.
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
            "op": "replace",
            "path": "/metadata/description",
            "value": description
        })
    
    # Add tags operation if tags provided
    if tags:
        operations.append({
            "op": "replace",
            "path": "/metadata/tags",
            "value": tags
        })
    
    if not operations:
        return "SKIPPED: No display name, description or tags to update"
    
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
                    if len(row) < 2:  # Ensure we have at least Asset and Display_Name columns
                        print(f"WARNING: Row {row_num} has insufficient columns, skipping")
                        continue
                    
                    asset_name = row[0].strip()
                    display_name = row[1].strip() if len(row) > 1 else ""
                    description = row[2].strip() if len(row) > 2 else ""
                    tags_input = row[3].strip() if len(row) > 3 else ""
                    
                    # Parse tags (pipe-separated)
                    tags = []
                    if tags_input:
                        tags = [tag.strip() for tag in tags_input.split('|') if tag.strip()]
                    
                    print(f"\nProcessing row {row_num}: {asset_name}")
                    
                    # Initialize result tracking for this row
                    result_row = {
                        'row_number': row_num,
                        'asset_name': asset_name,
                        'display_name': display_name,
                        'description': description,
                        'tags_input': tags_input,
                        'tags_parsed': tags,
                        'update_status': ''
                    }
                    
                    # Skip if no display name, description or tags provided
                    if not display_name and not description and not tags:
                        result_row['update_status'] = "SKIPPED: No display name, description or tags provided"
                        print(f"  ! Skipping {asset_name}: No display name, description or tags provided")
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
                        print(f"  → Will update: {' | '.join(updates_preview)}")
                        
                        # Update asset display name, description and tags
                        update_status = updateAssetDescription(client, asset_id, asset_name, display_name, description, tags)
                        result_row['update_status'] = update_status
                        
                    except AssertionError as msg:
                        error_msg = f"Asset error: {msg}"
                        print(f"  ✗ {error_msg}")
                        result_row['update_status'] = f"ERROR: {error_msg}"
                        
                    except Exception as e:
                        error_msg = f"Processing error: {e}"
                        print(f"  ✗ {error_msg}")
                        result_row['update_status'] = f"ERROR: {error_msg}"
                    
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
                header = ['Asset', 'Display_Name', 'Description', 'Tags', 'Update Status']
                writer.writerow(header)
                
                # Write data rows
                for result_row in results_data:
                    output_row = [
                        result_row['asset_name'],
                        result_row['display_name'],
                        result_row['description'],
                        result_row['tags_input'],
                        result_row['update_status']
                    ]
                    writer.writerow(output_row)
            
            print(f"Results written to: {output_filename}")
            
            # Print summary statistics
            total_rows = len(results_data)
            successful_updates = sum(1 for r in results_data if r['update_status'] == "SUCCESS")
            skipped_updates = sum(1 for r in results_data if r['update_status'].startswith("SKIPPED"))
            failed_updates = total_rows - successful_updates - skipped_updates
            
            print(f"\nSUMMARY:")
            print(f"Total rows processed: {total_rows}")
            print(f"Successful updates: {successful_updates}")
            print(f"Skipped updates: {skipped_updates}")
            print(f"Failed updates: {failed_updates}")
            
            # Additional breakdown by update type
            display_name_updates = sum(1 for r in results_data 
                                     if r['update_status'] == "SUCCESS" and r['display_name'])
            description_updates = sum(1 for r in results_data 
                                    if r['update_status'] == "SUCCESS" and r['description'])
            tag_updates = sum(1 for r in results_data 
                             if r['update_status'] == "SUCCESS" and r['tags_parsed'])
            
            print(f"\nUPDATE BREAKDOWN:")
            print(f"Assets with display name updates: {display_name_updates}")
            print(f"Assets with description updates: {description_updates}")
            print(f"Assets with tag updates: {tag_updates}")
            
        except Exception as e:
            print(f"ERROR writing results CSV: {e}")

        print("\n" + "="*60)
        print("PROCESS COMPLETED")
        print("="*60)
        print("Asset display name, description and tag update process completed.")
        print(f"Detailed results saved to: {output_filename}")


if __name__ == "__main__":
    # Expected CSV format with header:
    # Asset,Display_Name,Description,Tags
    # 
    # Where:
    # - Asset: Name of the asset to update
    # - Display_Name: Business display name for the asset (can be empty)
    # - Description: New description for the asset (can be empty)
    # - Tags: Pipe-separated list of tags (e.g. "tag1|tag2|tag3", can be empty)
    #
    # Examples:
    # Customer_Data,"CUSTOMER_MASTER","Customer information and demographics","PII|Finance|Customer"
    # Product_Catalog,"PRODUCT_CATALOG","Product catalog with pricing","Product|Catalog"
    # Sales_Data,"","Sales transaction records",""
    # User_Profiles,"USER_PROFILE_DATA","User profile information","PII|User"
    
    # To use with CATALOG instead of PROJECT:
    # 1. Set target = 'CATALOG'
    # 2. Ensure CATALOG_ID environment variable is set
    # 3. Run the script
    
    main(input_filename='assets_to_update.csv')