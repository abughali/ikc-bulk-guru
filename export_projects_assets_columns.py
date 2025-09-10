import csv
import os
from datetime import datetime
from cpd_client import CPDClient
from typing import Dict, List

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


def lookup_data_class_by_id(data_class_id: str) -> tuple[str, str]:
    """
    Lookup data class name and category by id from data class metadata
    Returns (data_class_name, data_class_category)
    """
    if not data_class_id or "data_class" not in _artifact_cache:
        return ("", "")
    
    for artifact in _artifact_cache["data_class"]:
        artifact_global_id = artifact.get("entity", {}).get("artifacts", {}).get("global_id", "")
        if artifact_global_id == data_class_id:
            name = artifact.get("metadata", {}).get("name", "")
            category = artifact.get("categories", {}).get("primary_category_name", "")
            return (name, category)
    
    return ("", "")


def preload_all_artifacts(client: CPDClient):
    """Preload all artifact types into cache at the beginning"""
    print("="*60)
    print("PRELOADING ALL ARTIFACTS INTO CACHE")
    print("="*60)
    
    artifact_types = ["glossary_term", "classification", "data_class"]
    
    for artifact_type in artifact_types:
        _load_artifacts(client, artifact_type)


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
        return response.json().get('entity')


def export_metadata():
    """Export all project assets and columns with their existing metadata populated in CSV format"""
    
    # Create exports directory if it doesn't exist
    os.makedirs("exports", exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"exports/projects_assets_columns_{timestamp}.csv"
    
    # Get all projects first
    print("Fetching all projects...")
    projects = get_all_projects()
    print(f"Found {len(projects)} total projects")
    
    if not projects:
        print("No projects found")
        return
    
    total_assets = 0
    total_columns = 0
    
    with open(filename, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        
        writer.writerow(['Project ID', 'Project Name', 'Asset Name', 'Column Name', 'Column Description', 
                         'Term1 Name', 'Term1 Category', 'Term2 Name', 'Term2 Category',
                         'Classification1 Name', 'Classification1 Category', 
                         'Classification2 Name', 'Classification2 Category', 'Data Class Name', 'Data Class Category', 'Tags'])
        
        with CPDClient() as client:
            # Preload all artifacts into cache for category lookups
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
                        
                        # Get asset details including existing metadata
                        try:
                            asset_details = scan_data_asset(client, project_id, asset_id)
                            
                            if asset_details and 'data_asset' in asset_details:
                                columns = asset_details['data_asset'].get('columns', [])
                                column_info = asset_details.get('column_info', {})  # Get existing metadata
                                
                                print(f"    Processing asset: {asset_name} ({len(columns)} columns)")
                                
                                for column in columns:
                                    try:
                                        column_name = column['name']
                                        
                                        # Get existing metadata for this column
                                        existing_metadata = column_info.get(column_name, {})
                                        
                                        # Extract existing values
                                        description = existing_metadata.get('column_description', '')
                                        
                                        # Extract existing terms
                                        term1_name = ""
                                        term1_category = ""
                                        term2_name = ""
                                        term2_category = ""
                                        terms = existing_metadata.get('column_terms', [])
                                        if terms:
                                            # First term
                                            if len(terms) > 0:
                                                term_id = terms[0].get('term_id', '')
                                                if term_id:
                                                    term1_name, term1_category = lookup_term_by_id(term_id)
                                            # Second term
                                            if len(terms) > 1:
                                                term_id_2 = terms[1].get('term_id', '')
                                                if term_id_2:
                                                    term2_name, term2_category = lookup_term_by_id(term_id_2)
                                        
                                        # Extract existing classifications - lookup names and categories by global_id
                                        classification1_name = ""
                                        classification1_category = ""
                                        classification2_name = ""
                                        classification2_category = ""
                                        classifications = existing_metadata.get('column_classifications', [])
                                        if classifications:
                                            if len(classifications) > 0:
                                                classification1_global_id = classifications[0].get('global_id', '')
                                                if classification1_global_id:
                                                    classification1_name, classification1_category = lookup_classification_by_global_id(classification1_global_id)
                                            if len(classifications) > 1:
                                                classification2_global_id = classifications[1].get('global_id', '')
                                                if classification2_global_id:
                                                    classification2_name, classification2_category = lookup_classification_by_global_id(classification2_global_id)
                                        
                                        # Extract existing data class - lookup name and category by data class id
                                        data_class_name = ""
                                        data_class_category = ""
                                        data_class = existing_metadata.get('data_class', {})
                                        if data_class and 'selected_data_class' in data_class:
                                            data_class_id = data_class['selected_data_class'].get('id', '')
                                            if data_class_id:
                                                data_class_name, data_class_category = lookup_data_class_by_id(data_class_id)
                                        
                                        # Extract existing tags
                                        tags = existing_metadata.get('column_tags', [])
                                        tags_string = "|".join(tags) if tags else ""
                                        
                                        # Create populated row with existing metadata
                                        row = [
                                            project_id,                   # Project ID
                                            project_name,                 # Project Name
                                            asset_name,                   # Asset Name  
                                            column_name,                  # Column Name
                                            description,                  # Column Description
                                            term1_name,                   # Term1 Name
                                            term1_category,               # Term1 Category
                                            term2_name,                   # Term2 Name
                                            term2_category,               # Term2 Category
                                            classification1_name,         # Classification1 Name
                                            classification1_category,     # Classification1 Category
                                            classification2_name,         # Classification2 Name
                                            classification2_category,     # Classification2 Category
                                            data_class_name,              # Data Class Name
                                            data_class_category,          # Data Class Category
                                            tags_string                   # Tags
                                        ]
                                        writer.writerow(row)
                                        total_columns += 1
                                        
                                    except KeyError as e:
                                        print(f"      Warning: Missing key {e} for column in asset {asset_name}")
                                        row = [
                                            project_id,                      # Project ID
                                            project_name,                    # Project Name
                                            asset_name,                      # Asset Name
                                            column.get('name', 'Unknown'),   # Column Name
                                            "",                              # Column Description (empty)
                                            "",                              # Term1 Name (empty)
                                            "",                              # Term1 Category (empty)
                                            "",                              # Term2 Name (empty)
                                            "",                              # Term2 Category (empty)
                                            "",                              # Classification1 Name (empty)
                                            "",                              # Classification1 Category (empty)
                                            "",                              # Classification2 Name (empty)
                                            "",                              # Classification2 Category (empty)
                                            "",                              # Data Class Name (empty)
                                            "",                              # Data Class Category (empty)
                                            ""                               # Tags (empty)
                                        ]
                                        writer.writerow(row)
                                        total_columns += 1
                            else:
                                print(f"    Warning: No data_asset info found for {asset_name}")
                                
                        except Exception as e:
                            print(f"    Error processing asset {asset_name}: {str(e)}")
                            continue
                    
                except Exception as e:
                    print(f"  Error scanning assets in project {project_name}: {str(e)}")
                    continue
    
    print(f"Exported to file: {filename}")
    print(f"Total projects processed: {len(projects)}")
    print(f"Total assets processed: {total_assets}")
    print(f"Total columns exported: {total_columns}")
    print(f"\nNext steps:")
    print(f"1. Open {filename} in Excel")
    print(f"2. Review and modify existing metadata as needed")
    print(f"3. Add missing metadata for columns without assignments")
    print(f"4. Save the file and use it as input for import_projects_assets_columns.py script")
    
    return filename


def main():
    """Main execution function"""
    print("Creating populated metadata from CPD projects...")
    export_metadata()


if __name__ == "__main__":
    main()