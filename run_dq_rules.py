from typing import Dict, List
from cpd_client import CPDClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
import threading
from collections import defaultdict
import time
from datetime import datetime
import csv
import os
from dotenv import load_dotenv

load_dotenv(override=True)

# Environment variables
project_id = os.environ.get('PROJECT_ID')

def get_datastage_settings(client: CPDClient, project_id: str) -> Dict:
    """
    Get DataStage settings for a project to retrieve jobNameSuffix and other configurations
    
    Args:
        client: CPD client
        project_id: Project ID
        
    Returns:
        Dict: DataStage settings response
    """
    default_suffix = ".DataStage job"
    url = f"/data_intg/v3/assets/datastage_settings?project_id={project_id}"
    
    try:
        response = client.get(url)
        
        if response.status_code == 200:
            settings_data = response.json()
            
            # Extract key settings
            project_settings = settings_data.get('entity', {}).get('project', {})
            job_name_suffix = project_settings.get('jobNameSuffix', default_suffix)
            
            print(f"Retrieved DataStage settings")
            print(f"   Job Name Suffix: '{job_name_suffix}'")
            
            return {
                'success': True,
                'job_name_suffix': job_name_suffix
            }
        else:
            print(f"Error getting DataStage settings: HTTP {response.status_code}")
            return {
                'success': False,
                'error': f"HTTP {response.status_code}: {response.text}",
                'job_name_suffix': default_suffix
            }
            
    except Exception as e:
        print(f"Exception getting DataStage settings: {e}")
        return {
            'success': False,
            'error': str(e),
            'job_name_suffix': default_suffix
        }

def create_job(client: CPDClient, project_id: str, flow_id: str, flow_name: str, job_name_suffix: str) -> Dict:
    """
    Create a DataStage job for a data integration flow
    
    Args:
        client: CPD client
        project_id: Project ID
        flow_id: Data integration flow ID
        flow_name: Data integration flow name
        
    Returns:
        Dict: Job creation response
    """
    job_name = f"{flow_name}{job_name_suffix}"
    
    # Prepare payload
    payload = {
        "job": {
            "name": job_name,
            "asset_ref": flow_id,
            "configuration": {}
        }
    }
    
    # Create the job
    url = f"/v2/jobs?project_id={project_id}"
    
    print(f"Creating job: {job_name}")
    print(f"For flow ID: {flow_id}")
    
    try:
        response = client.post(url, json=payload)
        
        if response.status_code == 201:
            job_data = response.json()
            job_id = job_data['metadata']['asset_id']
            
            print(f"Job created successfully!")
            print(f"   Job ID: {job_id}")
            print(f"   Job Name: {job_name}")
            
            return {
                'success': True,
                'job_id': job_id,
                'job_name': job_name,
                'flow_id': flow_id,
                'response': job_data
            }
        else:
            print(f"Error creating job: HTTP {response.status_code}")
            print(f"   Response: {response.text}")
            
            return {
                'success': False,
                'error': f"HTTP {response.status_code}: {response.text}",
                'job_name': job_name,
                'flow_id': flow_id
            }
            
    except Exception as e:
        print(f"Exception creating job: {e}")
        return {
            'success': False,
            'error': str(e),
            'job_name': job_name,
            'flow_id': flow_id
        }

def run_job(client: CPDClient, project_id: str, job_id: str, run_name: str = "job run") -> Dict:
    """
    Run a DataStage job
    
    Args:
        client: CPD client
        project_id: Project ID
        job_id: Job ID to run
        run_name: Name for this job run (default: "job run")
        
    Returns:
        Dict: Job run response with run_id and status
    """
    # Prepare payload
    payload = {
        "job_run": {
            "name": run_name
        }
    }
    
    # Run the job
    url = f"/v2/jobs/{job_id}/runs?project_id={project_id}"
        
    try:
        response = client.post(url, json=payload)
        
        if response.status_code == 201:
            job_run_data = response.json()
            
            # Extract key information
            run_id = job_run_data['metadata']['asset_id']
            state = job_run_data['entity']['job_run']['state']
            job_name = job_run_data['entity']['job_run']['job_name']
            job_ref = job_run_data['entity']['job_run']['job_ref']
            
            #print(f"Job run started successfully!")
            #print(f"   Run ID: {run_id}")
            #print(f"   Job Name: {job_name}")
            #print(f"   State: {state}")
            
            return {
                'success': True,
                'run_id': run_id,
                'job_id': job_id,
                'job_ref': job_ref,
                'job_name': job_name,
                'run_name': run_name,
                'state': state,
                'response': job_run_data
            }
        else:
            print(f"Error running job: HTTP {response.status_code}")
            print(f"   Response: {response.text}")
            
            return {
                'success': False,
                'error': f"HTTP {response.status_code}: {response.text}",
                'job_id': job_id,
                'run_name': run_name
            }
            
    except Exception as e:
        print(f"Exception running job: {e}")
        return {
            'success': False,
            'error': str(e),
            'job_id': job_id,
            'run_name': run_name
        }

def get_job_run_info(client: CPDClient, project_id: str, job_id: str, run_id: str) -> Dict:
    """
    Get simple timing and status info for a job run
    
    Args:
        client: CPD client
        project_id: Project ID
        job_id: Job ID
        run_id: Job run ID
        
    Returns:
        Dict: Simple timing and status info
    """
    url = f"/v2/jobs/{job_id}/runs/{run_id}?project_id={project_id}"
    
    try:
        response = client.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            job_run = data['entity']['job_run']
            
            # Extract the key timing and status info
            result = {
                'success': True,
                'run_id': run_id,
                'job_id': job_id,
                'job_name': job_run['job_name'],
                'state': job_run['state'],
                'created': data['metadata']['created'],
                'queue_start': job_run.get('queue_start'),
                'queue_end': job_run.get('queue_end'),
                'execution_start': job_run.get('execution_start'),
                'execution_end': job_run.get('execution_end'),
                'duration': job_run.get('duration')
            }
            
            # Calculate queue duration
            if result['queue_start'] and result['queue_end']:
                result['queued_duration'] = round((result['queue_end'] - result['queue_start']) / 1000, 2)
            
            # Calculate execution duration
            if result['execution_start'] and result['execution_end']:
                result['execution_duration'] = round((result['execution_end'] - result['execution_start']) / 1000, 2)
            
            # Calculate total duration from creation to completion
            if result['created'] and result['execution_end']:
                result['total_duration'] = round((result['execution_end'] - result['created']) / 1000, 2)
            
            return result
        else:
            return {
                'success': False,
                'error': f"HTTP {response.status_code}: {response.text}",
                'run_id': run_id,
                'job_id': job_id
            }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'run_id': run_id,
            'job_id': job_id
        }

def listAssets(client: CPDClient, asset_type: str, query, project_id: str):
    """
    Scan assets of a given type with CPD search pagination.
    """
    url = f"/v2/asset_types/{asset_type}/search?project_id={project_id}&hide_deprecated_response_fields=true"
    payload = {"query": query, "limit": 20}
    assets = []

    while True:
        resp = client.post(url, json=payload)
        if resp.status_code != 200:
            body = resp.text
            raise ValueError(f"Error scanning catalog (HTTP {resp.status_code}): {body}")

        data = resp.json()

        results = data.get("results", [])
        if results:
            assets.extend(results)

        nxt = data.get("next")
        if not nxt:
            break
        payload = nxt

    return assets

def assetRelationships(client: CPDClient, id: str, project_id: str) -> str:
    """
    This function retrieves the relationships of an asset in a project or catalog.
    """
    
    url = f"/v2/assets/get_relationships?asset_id={id}&project_id={project_id}&related_asset_types=data_rule&relationship_names=uses&limit=1"
        
    response = client.post(url)
    
    if response.status_code != 200:
        raise ValueError(f"Error reading asset relations: {response.text}")
    else:
        response_data = response.json()
        if response_data['total_rows'] == 0:
            return None  # No relationship found
        return response_data

def getAssetById(client: CPDClient, id: str, project_id: str) -> str:
    """
    This function retrieves the ID of an asset in a catalog based on its name.
    """
    url = f"/v2/assets/{id}?project_id={project_id}&allow_metadata_on_dpr_deny=true&hide_deprecated_response_fields=true"
    
    response = client.get(url)
    
    if response.status_code != 200:
        raise ValueError(f"Error reading asset: {response.text}")
    else:
        response_data = response.json()
        return response_data

def format_timestamp(timestamp_ms):
    """Convert millisecond timestamp to readable format"""
    if timestamp_ms:
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
    return 'N/A'

def format_duration(duration_seconds):
    """Format duration in seconds to readable format"""
    if duration_seconds is None:
        return 'N/A'
    
    if duration_seconds < 60:
        return f"{duration_seconds}s"
    elif duration_seconds < 3600:
        minutes = int(duration_seconds // 60)
        seconds = int(duration_seconds % 60)
        return f"{minutes}m {seconds}s"
    else:
        hours = int(duration_seconds // 3600)
        minutes = int((duration_seconds % 3600) // 60)
        seconds = int(duration_seconds % 60)
        return f"{hours}h {minutes}m {seconds}s"

def create_dqr_flow_job_matrix(client: CPDClient, 
                               project_id: str, 
                               create_missing_jobs: bool = True
) -> Dict:
    """
    Create a matrix of DQR → Flow → Job with all asset IDs
    Optionally create missing jobs automatically
    
    Args:
        client: CPD client
        project_id: Project ID
        create_missing_jobs: If True, automatically create missing jobs
        
    Returns:
        Dict: Complete mapping with all asset IDs
    """
    print("="*80)
    print("DQR → FLOW → JOB MATRIX WITH ASSET IDS")
    print("="*80)
    
    # Step 1: Get all data quality rules
    print("\n1. Getting all data quality rules...")
    rules = listAssets(client, "data_rule", "*:*", project_id)
    print(f"   Found {len(rules)} rules")
    
    # Create rules lookup for quick access
    rules_lookup = {rule['metadata']['asset_id']: rule for rule in rules}
    
    # Step 2: Get all data integration flows
    print("\n2. Getting all data integration flows...")
    flows = listAssets(client, "data_intg_flow", "*:*", project_id)
    print(f"   Found {len(flows)} flows")
    
    # Step 3: Get all DataStage jobs
    print("\n3. Getting all DataStage jobs...")
    jobs = listAssets(client, "job", "job.asset_ref_type:data_intg_flow", project_id)
    print(f"   Found {len(jobs)} DataStage jobs")
    
    # Step 4: Create flow-to-job mapping
    print("\n4. Mapping flows to jobs...")
    flow_to_job = {}
    
    for job in jobs:
        job_id = job['metadata']['asset_id']
        job_name = job['metadata']['name']
        
        # Get job details to find the flow it references
        try:
            job_details = getAssetById(client, job_id, project_id)
            flow_ref = job_details['entity']['job']['asset_ref']
            
            flow_to_job[flow_ref] = {
                'job_id': job_id,
                'job_name': job_name
            }
            
        except Exception as e:
            print(f"Error getting job details for {job_name}: {e}")
    
    print(f"   Mapped {len(flow_to_job)} flows to jobs")
    
    # Step 5: Create complete DQR-Flow-Job mapping
    print("\n5. Creating complete DQR-flow-job mapping...")
    
    # Initialize flow tracking with DQR relationship
    flows_dict = {}
    flows_without_jobs = []
    flows_without_dqr = []
    
    for flow in flows:
        flow_id = flow['metadata']['asset_id']
        flow_name = flow['metadata']['name']
        
        flow_data = {
            'flow_name': flow_name,
            'flow_id': flow_id,
            'has_job': False,
            'job_name': None,
            'job_id': None,
            'has_dqr': False,
            'dqr_name': None,
            'dqr_id': None
        }
        
        # Check if this flow has a DQR relationship
        try:
            rel_response = assetRelationships(client, flow_id, project_id)
            
            if rel_response and 'resources' in rel_response and len(rel_response['resources']) > 0:
                # Get the rule this flow implements
                rule_id = rel_response['resources'][0]['asset_id']
                
                if rule_id in rules_lookup:
                    rule = rules_lookup[rule_id]
                    flow_data['has_dqr'] = True
                    flow_data['dqr_name'] = rule['metadata']['name']
                    flow_data['dqr_id'] = rule_id
            else:
                flows_without_dqr.append(flow_id)
                
        except Exception as e:
            # Flow has no rule - that's ok
            flows_without_dqr.append(flow_id)
        
        # Check if this flow has a job
        if flow_id in flow_to_job:
            job_info = flow_to_job[flow_id]
            flow_data['has_job'] = True
            flow_data['job_name'] = job_info['job_name']
            flow_data['job_id'] = job_info['job_id']
        else:
            # Flow needs a job
            flows_without_jobs.append({
                'flow_id': flow_id,
                'flow_name': flow_name,
                'flow_data': flow_data
            })
        
        flows_dict[flow_id] = flow_data
    
    print(f"   Flows with DQR: {len([f for f in flows_dict.values() if f['has_dqr']])}")
    print(f"   Flows without DQR: {len(flows_without_dqr)}")
    
    # Step 6: Create missing jobs
    job_creation_results = []
    if create_missing_jobs and flows_without_jobs:
        print(f"\n6. Creating {len(flows_without_jobs)} missing jobs...")
        settings_result = get_datastage_settings(client, project_id)
        job_name_suffix = settings_result['job_name_suffix']
        
        for i, flow_info in enumerate(flows_without_jobs, 1):
            print(f"\n   ({i}/{len(flows_without_jobs)}) Creating job for flow: {flow_info['flow_name'][:50]}...")
            
            result = create_job(
                client=client,
                project_id=project_id,
                flow_id=flow_info['flow_id'],
                flow_name=flow_info['flow_name'],
                job_name_suffix=job_name_suffix
            )
            
            job_creation_results.append(result)
            
            # Update our matrix if job was created successfully
            if result['success']:
                flow_id = flow_info['flow_id']
                flows_dict[flow_id]['has_job'] = True
                flows_dict[flow_id]['job_name'] = result['job_name']
                flows_dict[flow_id]['job_id'] = result['job_id']
                print(f"   Updated matrix for flow: {flows_dict[flow_id]['flow_name']}")
            else:
                print(f"   Failed to create job: {result['error']}")
        
        # Summary of job creation
        successful_jobs = len([r for r in job_creation_results if r['success']])
        failed_jobs = len(job_creation_results) - successful_jobs
        
        print(f"\n   Job Creation Summary:")
        print(f"      • Successfully created: {successful_jobs}")
        print(f"      • Failed to create: {failed_jobs}")
        
    elif flows_without_jobs:
        print(f"\n6. Found {len(flows_without_jobs)} flows without jobs (use create_missing_jobs=True to auto-create)")
    else:
        print(f"\n6. All flows already have jobs")
    
    # Step 7: Print complete matrix
    print(f"\n7. COMPLETE MATRIX:")
    print("-" * 120)
    print(f"{'DQR NAME':<30} {'FLOW NAME':<30} {'HAS JOB':<10} {'STATUS'}")
    print("-" * 120)
    
    flows_with_jobs = 0
    flows_with_dqr_and_jobs = 0
    
    for flow_data in flows_dict.values():
        dqr_name = (flow_data['dqr_name'][:29] if flow_data['dqr_name'] else "NO DQR")
        flow_name = flow_data['flow_name'][:29]
        has_job = "YES" if flow_data['has_job'] else "NO"
        
        # Determine status
        if flow_data['has_dqr'] and flow_data['has_job']:
            status = "COMPLETE CHAIN"
            flows_with_dqr_and_jobs += 1
        elif flow_data['has_job']:
            status = "NO DQR"
            flows_with_jobs += 1
        else:
            status = "MISSING JOB"
        
        print(f"{dqr_name:<30} {flow_name:<30} {has_job:<10} {status}")
    
    # Step 8: Summary
    print(f"\n8. SUMMARY:")
    print(f"   • Total Flows: {len(flows_dict)}")
    print(f"   • Complete Chains (DQR→Flow→Job): {flows_with_dqr_and_jobs}")
    print(f"   • Flows with Jobs: {flows_with_jobs}")
    print(f"   • Flows with DQR: {len([f for f in flows_dict.values() if f['has_dqr']])}")
    print(f"   • Missing Jobs: {len(flows_dict) - flows_with_jobs}")
    
    coverage = (flows_with_jobs / len(flows_dict)) * 100 if flows_dict else 0
    print(f"   • Job Coverage: {coverage:.1f}%")
    
    return {
        'flows': flows_dict,
        'total_flows': len(flows_dict),
        'flows_with_jobs': flows_with_jobs,
        'flows_with_dqr_and_jobs': flows_with_dqr_and_jobs,
        'coverage_percentage': coverage,
        'flows_without_jobs_before': len(flows_without_jobs),
        'job_creation_results': job_creation_results
    }

def run_jobs_concurrent(
                        client: CPDClient,
                        project_id: str,
                        flows_matrix: Dict,
                        max_workers: int = 20,
                        wait_initial_monitor: float = 10.0,
                        wait_poll_interval: float = 5.0,
                        wait_next_job_submission = 1.0,
                        job_timeout: float = 3600.0,  # 1 hour timeout per job
                        total_timeout: float = 86400.0  # 24 hours total timeout
) -> Dict:
    """
    Run jobs concurrently with enhanced status tracking showing grouped counts
    
    Args:
        client: CPD client
        project_id: Project ID
        flows_matrix: Matrix from create_dqr_flow_job_matrix
        max_workers: Maximum number of concurrent workers
        wait_initial_monitor: Initial wait before monitoring (seconds)
        wait_poll_interval: Polling interval for monitoring (seconds)
        wait_next_job_submission: Delay between job submissions (seconds)
        job_timeout: Maximum time to wait for a single job (seconds)
        total_timeout: Maximum time for entire execution (seconds)
        
    Returns:
        Dict: Execution results with detailed tracking
    """
    print("="*80)
    print("CONCURRENT JOB EXECUTION WITH STATUS GROUP TRACKING")
    print("="*80)
    print(f"Configuration:")
    print(f"  • Max Workers: {max_workers}")
    print(f"  • Initial Monitor Wait: {wait_initial_monitor}s")
    print(f"  • Monitor Poll Interval: {wait_poll_interval}s")
    print(f"  • Submission Wait: {wait_next_job_submission}s")
    print(f"  • Job Timeout: {job_timeout}s")
    print(f"  • Total Timeout: {total_timeout}s")
    
    # Build job to flow mapping
    job_to_flow_map = {}
    jobs_to_run = []
    
    for flow_data in flows_matrix['flows'].values():
        if flow_data['has_job']:
            job_id = flow_data['job_id']
            job_to_flow_map[job_id] = flow_data
            jobs_to_run.append(job_id)
    
    if not jobs_to_run:
        return {'total_jobs': 0, 'completed_jobs': 0, 'failed_jobs': 0, 'results': [], 'summary': []}
    
    print(f"\n   Found {len(jobs_to_run)} DataStage jobs to run")
    
    # Thread-safe status tracking
    status_lock = threading.Lock()
    job_statuses = defaultdict(int)
    job_statuses['NotSubmitted'] = len(jobs_to_run)
    job_details = {}  # Store job_id -> (job_run_id, current_state)
    last_status_print_time = time.time()
    status_print_interval = 5.0  # Print status every 5 seconds
    
    def update_status(job_id: str, new_status: str, job_run_id: str = None, initial_submission: bool = False):
        """Thread-safe status update with automatic printing"""
        nonlocal last_status_print_time
        
        with status_lock:
            # Handle initial submission
            if initial_submission:
                job_statuses['NotSubmitted'] -= 1
                job_statuses[new_status] = job_statuses.get(new_status, 0) + 1
                job_details[job_id] = (job_run_id, new_status)
            else:
                # Update existing job status
                if job_id in job_details:
                    old_run_id, old_status = job_details[job_id]
                    if old_status in job_statuses and job_statuses[old_status] > 0:
                        job_statuses[old_status] -= 1
                    job_statuses[new_status] = job_statuses.get(new_status, 0) + 1
                    job_details[job_id] = (job_run_id or old_run_id, new_status)
                else:
                    # New job without previous status
                    job_statuses[new_status] = job_statuses.get(new_status, 0) + 1
                    job_details[job_id] = (job_run_id, new_status)
            
            # Print status if enough time has passed or on significant events
            current_time = time.time()
            should_print = (current_time - last_status_print_time >= status_print_interval or 
                          new_status in ['Completed', 'Failed', 'CompletedWithErrors', 'CompletedWithWarnings', 'Canceled'])
            
            if should_print:
                print_status_summary()
                last_status_print_time = current_time
    
    def print_status_summary():
        """Print current status summary (must be called within status_lock)"""
        # Clear previous line and print new status
        status_parts = []
        
        # Order matters for readability
        status_order = ['NotSubmitted', 'Submitting', 'Starting', 'Queued', 'Running', 
                       'Completed', 'CompletedWithWarnings', 'CompletedWithErrors', 
                       'Failed', 'Canceled', 'Timeout', 'Error']
        
        for status in status_order:
            if status in job_statuses and job_statuses[status] > 0:
                status_parts.append(f"{status}:{job_statuses[status]}")
        
        # Add any other statuses not in our predefined order
        for status, count in job_statuses.items():
            if status not in status_order and count > 0:
                status_parts.append(f"{status}:{count}")
        
        timestamp = datetime.now().strftime("%d-%b-%Y %H:%M:%S")
        print(f"\r[{timestamp}] Status: {' | '.join(status_parts)}", end='\n')
    
    def process_job(job_id: str, index: int) -> Dict:
        """Process job run with timeout protection and status tracking"""
        start_time = time.time()
        
        try:
            flow_data = job_to_flow_map[job_id]
            
            # Update status to Submitting
            update_status(job_id, 'Submitting', initial_submission=True)
            
            # Create job run
            job_run_result = run_job(client, project_id, job_id)
            
            if not job_run_result['success']:
                update_status(job_id, 'SubmitFailed')
                return {
                    'job_id': job_id,
                    'success': False,
                    'error': job_run_result['error'],
                    'index': index,
                    'flow_data': flow_data
                }
            
            job_run_id = job_run_result['run_id']
            job_name = job_run_result['job_name']
            
            # Update to initial state
            initial_state = job_run_result.get('state', 'Starting')
            update_status(job_id, initial_state, job_run_id)
            
            # Initial wait
            time.sleep(wait_initial_monitor)
            
            # Monitor with timeout protection
            final_states = {'Completed', 'Failed', 'Canceled', 'CompletedWithErrors', 'CompletedWithWarnings'}
            state = 'Unknown'
            final_run_info = None
            
            consecutive_failures = 0

            while True:
                # Check job timeout
                if time.time() - start_time > job_timeout:
                    state = 'Timeout'
                    update_status(job_id, state, job_run_id)
                    break
                
                try:
                    job_run_info = get_job_run_info(client, project_id, job_id, job_run_id)
                    
                    if job_run_info['success']:
                        state = job_run_info['state']
                        consecutive_failures = 0  # Reset on success
                        update_status(job_id, state, job_run_id)
                        
                        if state in final_states:
                            final_run_info = job_run_info
                            break
                        else:
                            time.sleep(wait_poll_interval)
                    else:
                        # HTTP error occurred during monitoring
                        consecutive_failures += 1
                        error_msg = job_run_info.get('error', 'Unknown error')
                        print(f"Monitor API error for job {job_run_id} (attempt {consecutive_failures}/3): {error_msg}")
                        
                        if consecutive_failures >= 3:
                            print(f"Max monitor failures reached for job {job_run_id}. Attempting final status check...")
                            
                            # Try one final time before giving up
                            final_attempt = get_job_run_info(client, project_id, job_id, job_run_id)
                            if final_attempt['success']:
                                state = final_attempt['state']
                                final_run_info = final_attempt
                                update_status(job_id, state, job_run_id)
                                print(f"Final attempt succeeded: {state}")
                                break
                            else:
                                state = 'MonitorError'
                                update_status(job_id, state, job_run_id)
                                print(f"Final attempt also failed: {final_attempt.get('error', 'Unknown')}")
                                break
                        else:
                            # Wait longer and continue monitoring
                            print(f"Retrying monitor in {wait_poll_interval * 2}s...")
                            time.sleep(wait_poll_interval * 2)
                            continue
                        
                except Exception as e:
                    print(f"\nError monitoring job {job_run_id}: {e}")
                    state = 'MonitorError'
                    update_status(job_id, state, job_run_id)
                    break

            job_succeeded = state in {'Completed', 'CompletedWithWarnings'}

            return {
                'job_id': job_id,
                'job_run_id': job_run_id,
                'job_name': job_name,
                'final_state': state,
                'success': job_succeeded,
                'index': index,
                'flow_data': flow_data,
                'run_info': final_run_info,
                'duration': time.time() - start_time
            }
            
        except Exception as e:
            print(f"\nException for jobId: {job_id} - {e}")
            update_status(job_id, 'Exception')
            return {
                'job_id': job_id,
                'success': False,
                'error': str(e),
                'index': index,
                'flow_data': job_to_flow_map.get(job_id, {}),
                'duration': time.time() - start_time
            }
    
    # Print initial status
    with status_lock:
        print_status_summary()
    
    start_time = time.time()
    results = []
    
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            # Submit all jobs
            for index, job_id in enumerate(jobs_to_run, 1):
                if index > 1:  # No delay for first job
                    time.sleep(wait_next_job_submission)
                future = executor.submit(process_job, job_id, index)
                futures.append(future)
            
            # Wait for completion with timeout
            try:
                for future in as_completed(futures, timeout=total_timeout):
                    try:
                        result = future.result()
                        results.append(result)
                    except concurrent.futures.TimeoutError:
                        results.append({
                            'job_id': 'unknown',
                            'success': False,
                            'error': 'Future result timeout',
                            'flow_data': {}
                        })
                    except Exception as e:
                        results.append({
                            'job_id': 'unknown',
                            'success': False,
                            'error': f'Future exception: {str(e)}',
                            'flow_data': {}
                        })
                        
            except concurrent.futures.TimeoutError:
                print(f"\nTotal execution timeout after {total_timeout}s")
                # Cancel any remaining futures
                for future in futures:
                    if not future.done():
                        future.cancel()
                        results.append({
                            'job_id': 'cancelled',
                            'success': False,
                            'error': 'Cancelled due to total timeout',
                            'flow_data': {}
                        })
                        
    except Exception as e:
        print(f"Unexpected error in thread pool: {e}")
        results.append({
            'job_id': 'error',
            'success': False,
            'error': f'ThreadPool error: {str(e)}',
            'flow_data': {}
        })
    
    # Final status print
    with status_lock:
        print("\n" + "="*80)
        print("FINAL STATUS SUMMARY:")
        print_status_summary()
    
    total_duration = time.time() - start_time
    successful_jobs = [r for r in results if r.get('success', False)]
    
    print(f"\nEXECUTION COMPLETE:")
    print(f"   • Total Jobs: {len(jobs_to_run)}")
    print(f"   • Completed: {len(successful_jobs)}")
    print(f"   • Failed: {len(jobs_to_run) - len(successful_jobs)}")
    print(f"   • Duration: {total_duration:.1f}s")
    print(f"   • Success Rate: {(len(successful_jobs)/len(jobs_to_run)*100):.1f}%")
    
    # Create summary for final report with all IDs
    summary_data = []
    for result in results:
        flow_data = result.get('flow_data', {})
        run_info = result.get('run_info', {})
        
        summary_entry = {
            'dqr_id': flow_data.get('dqr_id', ''),
            'dqr_name': flow_data.get('dqr_name', ''),
            'flow_id': flow_data.get('flow_id', ''),
            'flow_name': flow_data.get('flow_name', ''),
            'job_id': flow_data.get('job_id', ''),
            'job_name': result.get('job_name', flow_data.get('job_name', '')),
            'run_id': result.get('job_run_id', ''),
            'state': result.get('final_state', 'Unknown'),
            'success': result.get('success', False),
            'created': format_timestamp(run_info.get('created')) if run_info else 'N/A',
            'queue_start': format_timestamp(run_info.get('queue_start')) if run_info else 'N/A',
            'queue_end': format_timestamp(run_info.get('queue_end')) if run_info else 'N/A',
            'execution_start': format_timestamp(run_info.get('execution_start')) if run_info else 'N/A',
            'execution_end': format_timestamp(run_info.get('execution_end')) if run_info else 'N/A',
            'total_duration': format_duration(run_info.get('total_duration')) if run_info else 'N/A',
            'queue_duration': format_duration(run_info.get('queued_duration')) if run_info else 'N/A',
            'execution_duration': format_duration(run_info.get('execution_duration')) if run_info else 'N/A',
            'error': result.get('error', '') if not result.get('success', False) else ''
        }
        summary_data.append(summary_entry)
    
    # Final status distribution
    print(f"\nFINAL STATUS DISTRIBUTION:")
    with status_lock:
        for status, count in sorted(job_statuses.items()):
            if count > 0:
                percentage = (count / len(jobs_to_run)) * 100
                print(f"   • {status}: {count} ({percentage:.1f}%)")
    
    return {
        'total_jobs': len(jobs_to_run),
        'completed_jobs': len(successful_jobs),
        'failed_jobs': len(jobs_to_run) - len(successful_jobs),
        'total_duration': total_duration,
        'results': results,
        'success_rate': (len(successful_jobs) / len(jobs_to_run)) * 100 if jobs_to_run else 0,
        'summary': summary_data,
        'final_status_counts': dict(job_statuses)
    }

def print_summary(summary_data: List[Dict]):
    """
    Print a comprehensive summary table with all DQR-flow-job pipeline details including IDs
    """
    
    if not summary_data:
        print("No execution data available")
        return
    
    # Statistics
    total_runs = len(summary_data)
    successful_runs = len([s for s in summary_data if s['success']])
    completed_runs = len([s for s in summary_data if s['state'] == 'Completed'])
    runs_with_dqr = len([s for s in summary_data if s['dqr_id']])
    
    print(f"   • Total Runs: {total_runs}")
    print(f"   • Successful Runs: {successful_runs} ({(successful_runs/total_runs*100):.1f}%)")
    print(f"   • Completed Successfully: {completed_runs} ({(completed_runs/total_runs*100):.1f}%)")
    print(f"   • Runs with DQR: {runs_with_dqr} ({(runs_with_dqr/total_runs*100):.1f}%)")
    print(f"   • Failed/Error: {total_runs - successful_runs}")

def run_complete_pipeline(client: CPDClient, 
                          project_id: str
                          ) -> Dict:
    """
    Complete end-to-end pipeline: DQR-Flow-Job matrix creation, job creation, execution, and comprehensive summary
    
    Args:
        client: CPD client
        project_id: Project ID
        
    Returns:
        Dict: Complete pipeline results
    """
    print("STARTING COMPLETE DQR-FLOW-JOB PIPELINE")
    print("="*80)
    
    # Step 1: Create/analyze the DQR-flow-job matrix
    print("\nSTEP 1: ANALYZING DQR-FLOW-JOB MATRIX")
    matrix_results = create_dqr_flow_job_matrix(client, project_id, True)
    
    # Step 2: Run all jobs with tracking
    print("\nSTEP 2: EXECUTING ALL JOBS")
    execution_results = run_jobs_concurrent(
        client, 
        project_id, 
        matrix_results
    )
    
    # Step 3: Print summary
    print("\nSTEP 3: SUMMARY")
    print_summary(execution_results['summary'])
    
    # Step 4: Final pipeline summary
    print("\nPIPELINE COMPLETION SUMMARY:")
    print(f"   • Flows Analyzed: {matrix_results['total_flows']}")
    print(f"   • Complete Chains (DQR→Flow→Job): {matrix_results['flows_with_dqr_and_jobs']}")
    print(f"   • Flows with Jobs: {matrix_results['flows_with_jobs']}")
    print(f"   • Jobs Executed: {execution_results['total_jobs']}")
    print(f"   • Successful Executions: {execution_results['completed_jobs']}")
    print(f"   • Overall Success Rate: {execution_results['success_rate']:.1f}%")
    print(f"   • Total Pipeline Duration: {execution_results['total_duration']:.1f}s")
    
    return {
        'matrix_results': matrix_results,
        'execution_results': execution_results,
        'pipeline_summary': execution_results['summary']
    }

# Main
if __name__ == "__main__":
    
    with CPDClient() as client:
        # Run the complete pipeline
        pipeline_results = run_complete_pipeline(
            client, 
            project_id
        )
        
        # Access detailed summary data
        summary_data = pipeline_results['pipeline_summary']
        
        # Export summary to CSV
        print(f"\nSummary data contains {len(summary_data)} entries with detailed execution metrics")
        
        # Create output directory
        output_dir = "out"
        os.makedirs(output_dir, exist_ok=True)

        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = 'dqr_jobs_run_summary'
        output_filename = os.path.join(output_dir, f"{base_filename}_{timestamp}.csv")

        with open(output_filename, 'w', newline='') as csv_file:
            if summary_data:
                fieldnames = summary_data[0].keys()
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(summary_data)