import csv
import os
from datetime import datetime
from cpd_client import CPDClient
from typing import Dict, List

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
                # Convert string UIDs to integers for consistent lookup
                if isinstance(uid, str) and uid.isdigit():
                    _user_cache[int(uid)] = username
                elif isinstance(uid, int):
                    _user_cache[uid] = username
            
        if len(users) < batch_size:
            break
        offset += batch_size
    
    print(f"Loaded {len(_user_cache)} users")


def get_username(uid) -> str:
    """Get username by uid from cache, return original value if lookup fails"""
    try:
        return _user_cache.get(int(uid), uid)
    except:
        return uid


def lookup_term_by_id(term_id: str) -> tuple[str, str]:
    """
    Lookup term name and category by term_id (global_id from term metadata)
    Returns (term_name, term_category)
    """
    if not term_id or "glossary_term" not in _artifact_cache:
        return ("", "")
    
    for artifact in _artifact_cache["glossary_term"]:
        artifact_global_id = artifact.get("entity", {}).get("artifacts", {}).get("global_id", "")
        if artifact_global_id == term_id:
            name = artifact.get("metadata", {}).get("name", "")
            category = artifact.get("categories", {}).get("primary_category_name", "")
            return (name, category)
    
    return ("", "")


def lookup_classification_by_global_id(global_id: str) -> tuple[str, str]:
    """
    Lookup classification name and category by global_id from classification metadata
    Returns (classification_name, classification_category)
    """
    if not global_id or "classification" not in _artifact_cache:
        return ("", "")
    
    for artifact in _artifact_cache["classification"]:
        artifact_global_id = artifact.get("entity", {}).get("artifacts", {}).get("global_id", "")
        if artifact_global_id == global_id:
            name = artifact.get("metadata", {}).get("name", "")
            category = artifact.get("categories", {}).get("primary_category_name", "")
            return (name, category)
    
    return ("", "")


def preload_all_artifacts(client: CPDClient):
    """Preload all artifact types and users into cache at the beginning"""
    print("="*60)
    print("PRELOADING ALL ARTIFACTS AND USERS INTO CACHE")
    print("="*60)
    
    artifact_types = ["glossary_term", "classification"]
    
    for artifact_type in artifact_types:
        _load_artifacts(client, artifact_type)
    
    # Load users for owner lookup
    _load_users(client)


def get_all_projects():
    """Get ALL projects using pagination"""
    with CPDClient() as client:
        all_projects = []
        bookmark = None
        limit = 100  # Maximum allowed by API
        
        while True:
            # Build query parameters
            params = {
                "type": "cpd",
                "limit": limit,
                "include": "name"
            }
            
            if bookmark:
                params["bookmark"] = bookmark
            
            # Make API request
            response = client.get("/v2/projects", params=params)
            
            if response.status_code != 200:
                print(f"Error fetching projects: {response.status_code}")
                print(f"Response: {response.text}")
                break
                
            data = response.json()
            projects = data.get("resources", [])
            
            if not projects:
                break
                
            all_projects.extend(projects)
            print(f"Fetched {len(projects)} projects (total: {len(all_projects)})")
            
            # Check if there are more pages
            bookmark = data.get("bookmark")
            if not bookmark:
                break
        
        return all_projects


def scan_project_data_assets(client, project_id):
    """
    Retrieves a list of data assets in a specified project using search API.
    Returns the project_assets list containing information about all data assets in the project.
    """
    project_assets = []
    payload = {
        "query": "*:*",
        "limit": 200
    }
    stop_flag = 0

    while stop_flag == 0:
        response = client.post(f"/v2/asset_types/data_asset/search?project_id={project_id}&allow_metadata_on_dpr_deny=true", json=payload)

        if response.status_code != 200:
            print(f"Error scanning project {project_id}: {response.status_code} - {response.text}")
            break
        else:
            response_data = response.json()
            if 'next' in response_data:
                payload = response_data['next']
            else:
                stop_flag += 1

            project_assets.extend(response_data.get('results', []))
    
    return project_assets


def scan_data_asset(client, project_id, asset_id):
    """
    Retrieves metadata for a specified data asset within a project
    Parameters:
        client: CPD client instance
        project_id (str): ID of the project containing the asset
        asset_id (str): ID of the asset to retrieve metadata for
    Returns:
        dict: JSON data containing metadata for the specified asset
    """
    response = client.get(f"/v2/assets/{asset_id}?project_id={project_id}&allow_metadata_on_dpr_deny=true")

    if response.status_code != 200:
        print(f"Error scanning asset {asset_id}: {response.status_code} - {response.text}")
        return None
    else:
        return response.json()


def export_asset_metadata():
    """Export all project assets with their existing metadata populated in CSV format"""
    
    # Create exports directory if it doesn't exist
    os.makedirs("exports", exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"exports/projects_assets_{timestamp}.csv"
    
    # Get all projects first
    print("Fetching all projects...")
    projects = get_all_projects()
    print(f"Found {len(projects)} total projects")
    
    if not projects:
        print("No projects found")
        return
    
    total_assets = 0
    
    with open(filename, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        # Write header with Project_Name as second column
        writer.writerow(['Project_ID', 'Project_Name', 'Asset', 'Display_Name', 'Description', 'Tags', 
                        'Term1 Name', 'Term1 Category', 'Term2 Name', 'Term2 Category',
                        'Classification1 Name', 'Classification1 Category', 
                        'Classification2 Name', 'Classification2 Category', 'Owner'])
        
        with CPDClient() as client:
            # Preload all artifacts and users into cache for lookups
            preload_all_artifacts(client)
            
            print("\n" + "="*60)
            print("PROCESSING PROJECTS AND ASSETS")
            print("="*60)
            
            for i, project in enumerate(projects, 1):
                project_id = project['metadata']['guid']
                project_name = project['entity']['name']
                
                print(f"Processing project {i}/{len(projects)}: {project_name}")
                
                # Get all assets in this project
                try:
                    assets = scan_project_data_assets(client, project_id)
                    print(f"  Found {len(assets)} assets in project {project_name}")
                    total_assets += len(assets)
                    
                    for asset in assets:
                        asset_id = asset['metadata']['asset_id']
                        asset_name = asset['metadata']['name']
                        
                        print(f"    Processing asset: {asset_name}")
                        
                        # Get asset details including existing metadata
                        try:
                            asset_data = scan_data_asset(client, project_id, asset_id)
                            
                            if asset_data:
                                # Extract asset-level metadata
                                metadata = asset_data.get('metadata', {})
                                entity = asset_data.get('entity', {})
                                
                                # Get basic metadata
                                description = metadata.get('description', '')
                                
                                # Get display name from semantic name
                                display_name = ""
                                if 'data_asset' in entity:
                                    semantic_name = entity['data_asset'].get('semantic_name', {})
                                    if semantic_name:
                                        display_name = semantic_name.get('expanded_name', '')
                                
                                # Get tags
                                tags = metadata.get('tags', [])
                                tags_string = "|".join(tags) if tags else ""
                                
                                # Get business terms
                                term1_name = ""
                                term1_category = ""
                                term2_name = ""
                                term2_category = ""
                                
                                asset_terms = entity.get('asset_terms', {})
                                if asset_terms and 'list' in asset_terms:
                                    terms_list = asset_terms['list']
                                    if len(terms_list) > 0:
                                        term_id = terms_list[0].get('term_id', '')
                                        if term_id:
                                            term1_name, term1_category = lookup_term_by_id(term_id)
                                    if len(terms_list) > 1:
                                        term_id = terms_list[1].get('term_id', '')
                                        if term_id:
                                            term2_name, term2_category = lookup_term_by_id(term_id)
                                
                                # Get classifications
                                classification1_name = ""
                                classification1_category = ""
                                classification2_name = ""
                                classification2_category = ""
                                
                                data_profile = entity.get('data_profile', {})
                                if data_profile and 'data_classification_manual' in data_profile:
                                    classifications = data_profile['data_classification_manual']
                                    if len(classifications) > 0:
                                        global_id = classifications[0].get('global_id', '')
                                        if global_id:
                                            classification1_name, classification1_category = lookup_classification_by_global_id(global_id)
                                    if len(classifications) > 1:
                                        global_id = classifications[1].get('global_id', '')
                                        if global_id:
                                            classification2_name, classification2_category = lookup_classification_by_global_id(global_id)
                                
                                # Get owner
                                owner_id = metadata.get('owner_id', '')
                                owner_username = get_username(owner_id)
                                
                                # Create row with existing asset metadata
                                row = [
                                    project_id,                    # Project_ID
                                    project_name,                  # Project_Name
                                    asset_name,                    # Asset
                                    display_name,                  # Display_Name
                                    description,                   # Description
                                    tags_string,                   # Tags
                                    term1_name,                    # Term1 Name
                                    term1_category,                # Term1 Category
                                    term2_name,                    # Term2 Name
                                    term2_category,                # Term2 Category
                                    classification1_name,          # Classification1 Name
                                    classification1_category,      # Classification1 Category
                                    classification2_name,          # Classification2 Name
                                    classification2_category,      # Classification2 Category
                                    owner_username                 # Owner
                                ]
                                writer.writerow(row)
                                
                            else:
                                print(f"    Warning: No asset data found for {asset_name}")
                                # Write empty row for asset with no metadata (include Project_ID and Project_Name)
                                row = [project_id, project_name, asset_name, "", "", "", "", "", "", "", "", "", "", "", ""]
                                writer.writerow(row)
                                
                        except Exception as e:
                            print(f"    Error processing asset {asset_name}: {str(e)}")
                            # Write empty row for failed asset (include Project_ID and Project_Name)
                            row = [project_id, project_name, asset_name, "", "", "", "", "", "", "", "", "", "", "", ""]
                            writer.writerow(row)
                            continue
                    
                except Exception as e:
                    print(f"  Error scanning assets in project {project_name}: {str(e)}")
                    continue
    
    print(f"Exported to file: {filename}")
    print(f"Total projects processed: {len(projects)}")
    print(f"Total assets processed: {total_assets}")
    print(f"\nNext steps:")
    print(f"1. Open {filename} in Excel")
    print(f"2. Review and modify existing metadata as needed")
    print(f"3. Add missing metadata for assets without assignments")
    print(f"4. Save the file and use it as input for the asset import script")
    
    return filename


def main():
    """Main execution function"""
    print("Exporting asset metadata from all CPD projects...")
    export_asset_metadata()


if __name__ == "__main__":
    main()