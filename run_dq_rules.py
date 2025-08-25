import os
import threading
import time
import random
import csv
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
from cpd_client import CPDClient


def get_datastage_settings(client: CPDClient, project_id: str) -> Dict:
    """
    Get DataStage settings for a project to retrieve jobNameSuffix and other configurations
    """
    default_suffix = ".DataStage job"
    url = f"/data_intg/v3/assets/datastage_settings?project_id={project_id}"
    
    try:
        response = client.get(url)
        
        if response.status_code == 200:
            settings_data = response.json()
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
    """Create a DataStage job for a data integration flow"""
    job_name = f"{flow_name}{job_name_suffix}"
    
    payload = {
        "job": {
            "name": job_name,
            "asset_ref": flow_id,
            "configuration": {}
        }
    }
    
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

def run_job(client: CPDClient, project_id: str, job_id: str, 
           run_name: str = "job run", max_retries: int = 3) -> Dict:
    """Run job with retry logic for transient failures"""
    
    for attempt in range(max_retries):
        try:
            payload = {"job_run": {"name": run_name}}
            url = f"/v2/jobs/{job_id}/runs?project_id={project_id}"
            
            response = client.post(url, json=payload, timeout=60)
            
            if response.status_code == 201:
                job_run_data = response.json()
                run_id = job_run_data['metadata']['asset_id']
                state = job_run_data['entity']['job_run']['state']
                job_name = job_run_data['entity']['job_run']['job_name']
                job_ref = job_run_data['entity']['job_run']['job_ref']
                
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
            elif response.status_code in [429, 502, 503, 504] and attempt < max_retries - 1:
                # Retry on rate limiting or server errors
                wait_time = (2 ** attempt) + random.uniform(0, 1)  # Exponential backoff with jitter
                print(f"Job {job_id} attempt {attempt + 1} failed with {response.status_code}, retrying in {wait_time:.1f}s")
                time.sleep(wait_time)
                continue
            else:
                return {
                    'success': False,
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'job_id': job_id,
                    'run_name': run_name
                }
            
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Job {job_id} attempt {attempt + 1} failed with exception: {e}, retrying in {wait_time:.1f}s")
                time.sleep(wait_time)
                continue
            else:
                return {
                    'success': False,
                    'error': str(e),
                    'job_id': job_id,
                    'run_name': run_name
                }
    
    return {
        'success': False,
        'error': f"Max retries ({max_retries}) exceeded",
        'job_id': job_id,
        'run_name': run_name
    }

def get_job_run_info(client: CPDClient, project_id: str, 
                    job_id: str, run_id: str, max_retries: int = 3) -> Dict:
    """Get job run info with retry logic"""
    
    for attempt in range(max_retries):
        try:
            url = f"/v2/jobs/{job_id}/runs/{run_id}?project_id={project_id}"
            response = client.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                job_run = data['entity']['job_run']
                
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
                
                # Calculate durations
                if result['queue_start'] and result['queue_end']:
                    result['queued_duration'] = round((result['queue_end'] - result['queue_start']) / 1000, 2)
                
                if result['execution_start'] and result['execution_end']:
                    result['execution_duration'] = round((result['execution_end'] - result['execution_start']) / 1000, 2)
                
                if result['created'] and result['execution_end']:
                    result['total_duration'] = round((result['execution_end'] - result['created']) / 1000, 2)
                
                return result
                
            elif response.status_code in [429, 502, 503, 504] and attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Monitor job {run_id} attempt {attempt + 1} failed with {response.status_code}, retrying in {wait_time:.1f}s")
                time.sleep(wait_time)
                continue
            else:
                return {
                    'success': False,
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'run_id': run_id,
                    'job_id': job_id
                }
                
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Monitor job {run_id} attempt {attempt + 1} exception: {e}, retrying in {wait_time:.1f}s")
                time.sleep(wait_time)
                continue
            else:
                return {
                    'success': False,
                    'error': str(e),
                    'run_id': run_id,
                    'job_id': job_id
                }
    
    return {
        'success': False,
        'error': f"Max retries ({max_retries}) exceeded",
        'run_id': run_id,
        'job_id': job_id
    }

def listAssets(client: CPDClient, asset_type: str, query: str, project_id: str) -> List[Dict]:
    """Scan assets of a given type with CPD search pagination."""
    url = f"/v2/asset_types/{asset_type}/search?project_id={project_id}&hide_deprecated_response_fields=true"
    payload = {"query": query, "limit": 200}
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

def assetRelationships(client: CPDClient, id: str, project_id: str) -> Optional[Dict]:
    """Retrieve the relationships of an asset in a project."""
    url = f"/v2/assets/get_relationships?asset_id={id}&project_id={project_id}&related_asset_types=data_rule&relationship_names=uses&limit=1"
        
    response = client.post(url)
    
    if response.status_code != 200:
        raise ValueError(f"Error reading asset relations: {response.text}")
    else:
        response_data = response.json()
        if response_data['total_rows'] == 0:
            return None  # No relationship found
        return response_data

def getAssetById(client: CPDClient, id: str, project_id: str) -> Dict:
    """Retrieve an asset by its ID."""
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

# ============================================================================
# JOB PREPARATION AND VALIDATION
# ============================================================================

def prepare_dqr_jobs(client: CPDClient, 
                     project_id: str, 
                     create_missing_jobs: bool = True
) -> Dict:
    """
    Prepare all DQR jobs by ensuring each flow has a corresponding job.
    Each DQR always has an associated flow, so we just need to ensure jobs exist.
    """
    print("="*80)
    print("DATA QUALITY RULES JOB PREPARATION")
    print("="*80)
    
    # Step 1: Get all data quality rules
    print("\n1. Getting all data quality rules...")
    rules = listAssets(client, "data_rule", "*:*", project_id)
    print(f"   Found {len(rules)} rules")
    
    # Create rules lookup for quick access
    rules_lookup = {rule['metadata']['asset_id']: rule for rule in rules}
    
    # Step 2: Get all data integration flows (which implement the rules)
    print("\n2. Getting all data integration flows...")
    flows = listAssets(client, "data_intg_flow", "*:*", project_id)
    print(f"   Found {len(flows)} flows")
    
    # Step 3: Get all existing DataStage jobs
    print("\n3. Getting all existing DataStage jobs...")
    jobs = listAssets(client, "job", "job.asset_ref_type:data_intg_flow", project_id)
    print(f"   Found {len(jobs)} DataStage jobs")
    
    # Step 4: Build flow-to-job mapping
    print("\n4. Checking which flows have jobs...")
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
    
    print(f"   {len(flow_to_job)} flows already have jobs")
    
    # Step 5: Build complete DQR-Flow-Job tracking
    print("\n5. Building DQR-Flow-Job relationships...")
    
    dqr_data = {}
    flows_without_jobs = []
    
    for flow in flows:
        flow_id = flow['metadata']['asset_id']
        flow_name = flow['metadata']['name']
        
        flow_info = {
            'flow_name': flow_name,
            'flow_id': flow_id,
            'has_job': False,
            'job_name': None,
            'job_id': None,
            'dqr_name': None,
            'dqr_id': None
        }
        
        # Check if this flow implements a DQR
        try:
            rel_response = assetRelationships(client, flow_id, project_id)
            
            if rel_response and 'resources' in rel_response and len(rel_response['resources']) > 0:
                # Get the rule this flow implements
                rule_id = rel_response['resources'][0]['asset_id']
                
                if rule_id in rules_lookup:
                    rule = rules_lookup[rule_id]
                    flow_info['dqr_name'] = rule['metadata']['name']
                    flow_info['dqr_id'] = rule_id
                
        except Exception as e:
            # Flow has no associated rule - skip it
            pass
        
        # Check if this flow has a job
        if flow_id in flow_to_job:
            job_info = flow_to_job[flow_id]
            flow_info['has_job'] = True
            flow_info['job_name'] = job_info['job_name']
            flow_info['job_id'] = job_info['job_id']
        else:
            # Flow needs a job
            flows_without_jobs.append({
                'flow_id': flow_id,
                'flow_name': flow_name,
                'flow_info': flow_info
            })
        
        # Only track flows that have DQRs
        if flow_info['dqr_id']:
            dqr_data[flow_id] = flow_info
    
    print(f"   Found {len(dqr_data)} flows with DQRs")
    
    # Filter flows_without_jobs to only include those with DQRs
    flows_without_jobs = [f for f in flows_without_jobs if f['flow_info']['dqr_id']]
    
    # Step 6: Create missing jobs if requested
    job_creation_results = []
    if create_missing_jobs and flows_without_jobs:
        print(f"\n6. Creating {len(flows_without_jobs)} missing jobs for DQR flows...")
        settings_result = get_datastage_settings(client, project_id)
        job_name_suffix = settings_result['job_name_suffix']
        
        for i, flow_data in enumerate(flows_without_jobs, 1):
            print(f"\n   ({i}/{len(flows_without_jobs)}) Creating job for flow: {flow_data['flow_name'][:50]}...")
            
            result = create_job(
                client=client,
                project_id=project_id,
                flow_id=flow_data['flow_id'],
                flow_name=flow_data['flow_name'],
                job_name_suffix=job_name_suffix
            )
            
            job_creation_results.append(result)
            
            # Update our tracking if job was created successfully
            if result['success']:
                flow_id = flow_data['flow_id']
                dqr_data[flow_id]['has_job'] = True
                dqr_data[flow_id]['job_name'] = result['job_name']
                dqr_data[flow_id]['job_id'] = result['job_id']
            else:
                print(f"   Failed to create job: {result['error']}")
        
        # Summary of job creation
        successful_jobs = len([r for r in job_creation_results if r['success']])
        failed_jobs = len(job_creation_results) - successful_jobs
        
        print(f"\n   Job Creation Summary:")
        print(f"      • Successfully created: {successful_jobs}")
        print(f"      • Failed to create: {failed_jobs}")
        
    elif flows_without_jobs:
        print(f"\n6. Found {len(flows_without_jobs)} DQR flows without jobs (set create_missing_jobs=True to auto-create)")
    else:
        print(f"\n6. All DQR flows already have jobs")
    
    # Step 7: Summary
    dqr_flows_with_jobs = len([f for f in dqr_data.values() if f['has_job']])
    
    print(f"\n7. PREPARATION SUMMARY:")
    print(f"   • Total DQRs: {len(rules)}")
    print(f"   • DQR Flows: {len(dqr_data)}")
    print(f"   • DQR Flows with Jobs: {dqr_flows_with_jobs}")
    print(f"   • Missing Jobs: {len(dqr_data) - dqr_flows_with_jobs}")
    
    coverage = (dqr_flows_with_jobs / len(dqr_data)) * 100 if dqr_data else 0
    print(f"   • Job Coverage: {coverage:.1f}%")
    
    return {
        'dqr_flows': dqr_data,
        'total_dqrs': len(rules),
        'total_dqr_flows': len(dqr_data),
        'dqr_flows_with_jobs': dqr_flows_with_jobs,
        'coverage_percentage': coverage,
        'flows_without_jobs_count': len(flows_without_jobs),
        'job_creation_results': job_creation_results
    }

# ============================================================================
# BATCH JOB EXECUTION
# ============================================================================

def execute_dqr_jobs_batch(
    client: CPDClient,
    project_id: str,
    dqr_preparation: Dict,
    max_workers: int = 20,
    wait_initial_monitor: float = 10.0,
    wait_poll_interval: float = 5.0,
    wait_next_job_submission: float = 1.0,
    job_timeout: float = 3600.0,
    total_timeout: float = 86400.0,
    enable_jitter: bool = True,
    retry_attempts: int = 3
) -> Dict:
    """
    Execute all DQR jobs in batch using concurrent workers
    """
    print("="*80)
    print("BATCH JOB EXECUTION")
    print("="*80)
    print(f"Configuration:")
    print(f"  • Max Workers: {max_workers}")
    print(f"  • Initial Monitor Wait: {wait_initial_monitor}s")
    print(f"  • Monitor Poll Interval: {wait_poll_interval}s")
    print(f"  • Submission Wait: {wait_next_job_submission}s")
    print(f"  • Job Timeout: {job_timeout}s")
    print(f"  • Total Timeout: {total_timeout}s")
    
    # Build list of jobs to run
    jobs_to_run = []
    job_to_dqr_map = {}
    
    for flow_data in dqr_preparation['dqr_flows'].values():
        if flow_data['has_job']:
            job_id = flow_data['job_id']
            jobs_to_run.append(job_id)
            job_to_dqr_map[job_id] = flow_data
    
    if not jobs_to_run:
        return {'total_jobs': 0, 'completed_jobs': 0, 'failed_jobs': 0, 'results': [], 'summary': []}
    
    print(f"\n   Found {len(jobs_to_run)} DQR jobs to execute")
    
    # Thread-safe status tracking
    status_lock = threading.Lock()
    job_statuses = defaultdict(int)
    job_statuses['NotSubmitted'] = len(jobs_to_run)
    job_details = {}
    last_status_print_time = time.time()
    status_print_interval = 5.0
    
    def update_status(job_id: str, new_status: str, job_run_id: str = None, initial_submission: bool = False):
        """Thread-safe status update with automatic printing"""
        nonlocal last_status_print_time
        
        with status_lock:
            if initial_submission:
                job_statuses['NotSubmitted'] -= 1
                job_statuses[new_status] = job_statuses.get(new_status, 0) + 1
                job_details[job_id] = (job_run_id, new_status)
            else:
                if job_id in job_details:
                    old_run_id, old_status = job_details[job_id]
                    if old_status in job_statuses and job_statuses[old_status] > 0:
                        job_statuses[old_status] -= 1
                    job_statuses[new_status] = job_statuses.get(new_status, 0) + 1
                    job_details[job_id] = (job_run_id or old_run_id, new_status)
                else:
                    job_statuses[new_status] = job_statuses.get(new_status, 0) + 1
                    job_details[job_id] = (job_run_id, new_status)
            
            current_time = time.time()
            should_print = (current_time - last_status_print_time >= status_print_interval or 
                          new_status in ['Completed', 'Failed', 'CompletedWithErrors', 'CompletedWithWarnings', 'Canceled'])
            
            if should_print:
                print_status_summary()
                last_status_print_time = current_time
    
    def print_status_summary():
        """Print current status summary (must be called within status_lock)"""
        status_parts = []
        status_order = ['NotSubmitted', 'Submitting', 'Starting', 'Queued', 'Running', 
                       'Completed', 'CompletedWithWarnings', 'CompletedWithErrors', 
                       'Failed', 'Canceled', 'Timeout', 'Error']
        
        for status in status_order:
            if status in job_statuses and job_statuses[status] > 0:
                status_parts.append(f"{status}:{job_statuses[status]}")
        
        for status, count in job_statuses.items():
            if status not in status_order and count > 0:
                status_parts.append(f"{status}:{count}")
        
        timestamp = datetime.now().strftime("%d-%b-%Y %H:%M:%S")
        print(f"\r[{timestamp}] Status: {' | '.join(status_parts)}", end='\n')
    
    def process_job(job_id: str, index: int) -> Dict:
        """Process job run with thread-safe client and comprehensive error handling"""
        start_time = time.time()
        thread_id = threading.get_ident()
        
        # Each thread gets its own client from the pool
        thread_client = client
        
        try:
            dqr_data = job_to_dqr_map[job_id]
            
            # Add jitter to prevent thundering herd
            if enable_jitter and index > 1:
                jitter = random.uniform(0, wait_next_job_submission)
                time.sleep(jitter)
            
            update_status(job_id, 'Submitting', initial_submission=True)
            
            # Submit job with retry logic
            job_run_result = run_job(thread_client, project_id, job_id, max_retries=retry_attempts)
            
            if not job_run_result['success']:
                update_status(job_id, 'SubmitFailed')
                return {
                    'job_id': job_id,
                    'success': False,
                    'error': job_run_result['error'],
                    'index': index,
                    'dqr_data': dqr_data,
                    'thread_id': thread_id
                }
            
            job_run_id = job_run_result['run_id']
            job_name = job_run_result['job_name']
            
            # Update to initial state
            initial_state = job_run_result.get('state', 'Starting')
            update_status(job_id, initial_state, job_run_id)
            
            # Initial wait with jitter
            initial_wait = wait_initial_monitor
            if enable_jitter:
                initial_wait += random.uniform(0, wait_initial_monitor * 0.2)
            time.sleep(initial_wait)
            
            # Monitor with timeout protection and retry logic
            final_states = {'Completed', 'Failed', 'Canceled', 'CompletedWithErrors', 'CompletedWithWarnings'}
            state = 'Unknown'
            final_run_info = None
            
            while True:
                # Check job timeout
                if time.time() - start_time > job_timeout:
                    state = 'Timeout'
                    update_status(job_id, state, job_run_id)
                    break
                
                job_run_info = get_job_run_info(thread_client, project_id, job_id, job_run_id, max_retries=retry_attempts)
                
                if job_run_info['success']:
                    state = job_run_info['state']
                    update_status(job_id, state, job_run_id)
                    
                    if state in final_states:
                        final_run_info = job_run_info
                        break
                    else:
                        # Add jitter to polling interval to distribute load
                        poll_wait = wait_poll_interval
                        if enable_jitter:
                            poll_wait += random.uniform(0, wait_poll_interval * 0.3)
                        time.sleep(poll_wait)
                else:
                    # All retries exhausted
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
                'dqr_data': dqr_data,
                'run_info': final_run_info,
                'duration': time.time() - start_time,
                'thread_id': thread_id
            }
            
        except Exception as e:
            update_status(job_id, 'Exception')
            return {
                'job_id': job_id,
                'success': False,
                'error': str(e),
                'index': index,
                'dqr_data': job_to_dqr_map.get(job_id, {}),
                'duration': time.time() - start_time,
                'thread_id': thread_id
            }
    
    # Print initial status
    with status_lock:
        print_status_summary()
    
    start_time = time.time()
    results = []
    
    try:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="CPDJob") as executor:
            # Submit all jobs
            futures = {
                executor.submit(process_job, job_id, index): job_id 
                for index, job_id in enumerate(jobs_to_run, 1)
            }
            
            # Wait for completion with timeout
            try:
                for future in as_completed(futures, timeout=total_timeout):
                    job_id = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        results.append({
                            'job_id': job_id,
                            'success': False,
                            'error': f'Future exception: {str(e)}',
                            'dqr_data': job_to_dqr_map.get(job_id, {})
                        })
                        
            except concurrent.futures.TimeoutError:
                print(f"\nTotal execution timeout after {total_timeout}s")
                # Cancel any remaining futures
                for future in futures:
                    if not future.done():
                        future.cancel()
                        job_id = futures[future]
                        results.append({
                            'job_id': job_id,
                            'success': False,
                            'error': 'Cancelled due to total timeout',
                            'dqr_data': job_to_dqr_map.get(job_id, {})
                        })
                        
    except Exception as e:
        print(f"Unexpected error in thread pool execution: {e}")
    
    # Final status and summary
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
    
    # Create summary data
    summary_data = []
    for result in results:
        dqr_data = result.get('dqr_data', {})
        run_info = result.get('run_info', {})
        
        summary_entry = {
            'dqr_id': dqr_data.get('dqr_id', ''),
            'dqr_name': dqr_data.get('dqr_name', ''),
            'flow_id': dqr_data.get('flow_id', ''),
            'flow_name': dqr_data.get('flow_name', ''),
            'job_id': dqr_data.get('job_id', ''),
            'job_name': result.get('job_name', dqr_data.get('job_name', '')),
            'run_id': result.get('job_run_id', ''),
            'state': result.get('final_state', 'Unknown'),
            'success': result.get('success', False),
            'thread_id': result.get('thread_id', ''),
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

def print_execution_summary(summary_data: List[Dict]):
    """Print execution summary statistics"""
    
    if not summary_data:
        print("No execution data available")
        return
    
    # Statistics
    total_runs = len(summary_data)
    successful_runs = len([s for s in summary_data if s['success']])
    completed_runs = len([s for s in summary_data if s['state'] == 'Completed'])
    
    print(f"\nEXECUTION SUMMARY STATISTICS:")
    print(f"   • Total Runs: {total_runs}")
    print(f"   • Successful Runs: {successful_runs} ({(successful_runs/total_runs*100):.1f}%)")
    print(f"   • Completed Successfully: {completed_runs} ({(completed_runs/total_runs*100):.1f}%)")
    print(f"   • Failed/Error: {total_runs - successful_runs}")

# ============================================================================
# MAIN BATCH EXECUTION
# ============================================================================

def run_dqr_batch_execution(project_id: str, 
                           config_file=None, 
                           pool_size: int = 50, 
                           max_workers: int = 20, 
                           max_token_age_hours: float = 23.0,
                           create_missing_jobs: bool = True
) -> Dict:
    """
    Complete DQR batch job execution: preparation, job creation if needed, and execution
    
    Args:
        project_id: Project ID
        config_file: Optional config file path
        pool_size: Size of the connection pool
        max_workers: Maximum concurrent workers
        max_token_age_hours: Refresh token after this many hours
        create_missing_jobs: Whether to create missing jobs
        
    Returns:
        Dict: Complete execution results
    """
    print("DATA QUALITY RULES BATCH JOB EXECUTOR")
    print("="*80)
    
    with CPDClient(config_file, pool_size, max_token_age_hours) as client:
        # Step 1: Prepare DQR jobs (check and create if needed)
        print("\nSTEP 1: PREPARING DQR JOBS")
        preparation_results = prepare_dqr_jobs(client, project_id, create_missing_jobs)
        
        # Step 2: Execute all DQR jobs
        print("\nSTEP 2: EXECUTING DQR JOBS IN BATCH")
        execution_results = execute_dqr_jobs_batch(
            client, 
            project_id, 
            preparation_results,
            max_workers=max_workers
        )
        
        # Step 3: Print summary
        print("\nSTEP 3: EXECUTION SUMMARY")
        print_execution_summary(execution_results['summary'])
        
        # Step 4: Final summary
        print("\nBATCH EXECUTION COMPLETION:")
        print(f"   • DQRs Processed: {preparation_results['total_dqrs']}")
        print(f"   • DQR Flows Found: {preparation_results['total_dqr_flows']}")
        print(f"   • Jobs Prepared: {preparation_results['dqr_flows_with_jobs']}")
        print(f"   • Jobs Executed: {execution_results['total_jobs']}")
        print(f"   • Successful Executions: {execution_results['completed_jobs']}")
        print(f"   • Overall Success Rate: {execution_results['success_rate']:.1f}%")
        print(f"   • Total Duration: {execution_results['total_duration']:.1f}s")
        
        return {
            'preparation_results': preparation_results,
            'execution_results': execution_results,
            'execution_summary': execution_results['summary']
        }

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    # Load environment variables
    load_dotenv(override=True)
    project_id = os.environ.get('PROJECT_ID')
    
    if not project_id:
        print("ERROR: PROJECT_ID environment variable is required")
        exit(1)
    
    # Run the batch execution
    batch_results = run_dqr_batch_execution(
        project_id=project_id,
        pool_size=50,
        max_workers=20,
        max_token_age_hours=12.0,  # Refresh tokens every 12 hours for safety
        create_missing_jobs=True   # Auto-create missing jobs
    )
    
    # Access detailed summary data
    summary_data = batch_results['execution_summary']
    
    # Export summary to CSV
    print(f"\nSummary data contains {len(summary_data)} entries with detailed execution metrics")
    
    # Create output directory
    output_dir = "out"
    os.makedirs(output_dir, exist_ok=True)

    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_filename = 'dqr_batch_execution_summary'
    output_filename = os.path.join(output_dir, f"{base_filename}_{timestamp}.csv")

    if summary_data:
        with open(output_filename, 'w', newline='') as csv_file:
            fieldnames = summary_data[0].keys()
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(summary_data)
        
        print(f"Results exported to: {output_filename}")
    
    print("\nBatch execution completed successfully!")