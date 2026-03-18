import os
import sys
import json
import urllib.request
import urllib.parse
import subprocess

def get_access_token():
    result = subprocess.run(["gcloud", "auth", "print-access-token"], capture_output=True, text=True)
    if result.returncode != 0:
        print("Failed to get gcloud token. Are you logged in?")
        sys.exit(1)
    return result.stdout.strip()

def get_project():
    if "PROJECT_ID" in os.environ:
        return os.environ["PROJECT_ID"]
        
    result = subprocess.run(["gcloud", "config", "get-value", "project"], capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        print("Failed to determine GCP project. Set it using 'gcloud config set project <PROJECT_ID>'")
        sys.exit(1)
    return result.stdout.strip()

def get_process_id_for_batch(project, batch_id, token):
    # Search common regions
    locations = ["us-east1", "us-central1", "us"]
    
    for location in locations:
        base_url = f"https://datalineage.googleapis.com/v1/projects/{project}/locations/{location}/processes"
        headers = {"Authorization": f"Bearer {token}"}
        
        next_page = ""
        while True:
            url = base_url
            if next_page:
                url += f"?pageToken={urllib.parse.quote(next_page)}"
                
            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req) as response:
                    data = json.loads(response.read().decode())
                    for process in data.get('processes', []):
                        origin_name = process.get('origin', {}).get('name', '')
                        custom_job = process.get('attributes', {}).get('custom_job_id', '')
                        # Match standard Dataproc batch UUIDs or Pipeline 4's custom attribute
                        if batch_id in origin_name or batch_id in custom_job:
                            return process['name'], location
                            
                    next_page = data.get('nextPageToken')
                    if not next_page:
                        break # exit pagination loop
            except urllib.error.HTTPError as e:
                break # move to next location
            
    return None, None

def get_runs_and_events(process_name, token):
    headers = {"Authorization": f"Bearer {token}"}
    runs_url = f"https://datalineage.googleapis.com/v1/{process_name}/runs"
    req = urllib.request.Request(runs_url, headers=headers)
    
    runs = []
    try:
        with urllib.request.urlopen(req) as response:
            runs_data = json.loads(response.read().decode())
            runs = runs_data.get('runs', [])
    except urllib.error.HTTPError as e:
        print(f"Error fetching runs: {e.code} {e.reason}")
        return []
        
    all_events = []
    for run in runs:
        run_name = run['name']
        events_url = f"https://datalineage.googleapis.com/v1/{run_name}/lineageEvents"
        req = urllib.request.Request(events_url, headers=headers)
        
        try:
            with urllib.request.urlopen(req) as response:
                events_data = json.loads(response.read().decode())
                for event in events_data.get('lineageEvents', []):
                    all_events.append({
                        "event": event,
                        "run_name": run_name,
                        "process_name": process_name
                    })
        except urllib.error.HTTPError as e:
            print(f"Error fetching events for {run_name}: {e.code} {e.reason}")
            
    return all_events

def fqn_to_gcs_path(fqn):
    # Convert gcs:your-project-id-demo_lineage_poc_direct_gcs.source to gs://your-project-id-demo_lineage_poc_direct_gcs/source
    if fqn.startswith("gcs:"):
        parts = fqn[4:].split(".", 1)
        if len(parts) == 2:
            return f"gs://{parts[0]}/{parts[1]}"
        return f"gs://{parts[0]}"
    return fqn

def print_lineage(events_with_context, target_batch_id):
    if not events_with_context:
        print("\n❌ Validation failed: No lineage events were captured for this batch.")
        return
        
    print(f"\n✅ Validation successful: Found {len(events_with_context)} lineage events.\n")
    print("=" * 80)
    print(f"LINEAGE GRAPH ENCOUNTERS FOR BATCH: {target_batch_id}")
    print("=" * 80)
    
    links_seen = set()
    
    for ctx in events_with_context:
        event = ctx["event"]
        run_name = ctx["run_name"]
        process_name = ctx["process_name"]
        event_name = event.get("name", "Unknown_Event")
        
        for link in event.get("links", []):
            source_fqn = link.get("source", {}).get("fullyQualifiedName", "Unknown_Source")
            target_fqn = link.get("target", {}).get("fullyQualifiedName", "Unknown_Target")
            
            if source_fqn != target_fqn: # ignore self-links
                conn_key = f"{source_fqn}->{target_fqn}"
                if conn_key not in links_seen:
                    links_seen.add(conn_key)
                    print(f" Dataproc Batch : {target_batch_id}")
                    print(f" Process ID     : {process_name}")
                    print(f" Run ID         : {run_name}")
                    print(f" Event ID       : {event_name}")
                    print("-" * 40)
                    print(f" Source (FQN)   : {source_fqn}")
                    print(f" Source (GCS)   : {fqn_to_gcs_path(source_fqn)}")
                    print("       ↓  ")
                    print(f" Target (FQN)   : {target_fqn}")
                    print(f" Target (GCS)   : {fqn_to_gcs_path(target_fqn)}")
                    print("=" * 80)
                    
    if not links_seen:
         print("No unique source-to-target movements detected (only self-reads).")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python inspect_batch_lineage.py <DATAPROC_BATCH_ID>")
        print("Example: python inspect_batch_lineage.py lineage-direct-gcs-verify")
        sys.exit(1)
        
    batch_id = sys.argv[1]
    
    project = get_project()
    token = get_access_token()
    
    print(f"Looking up Dataplex Lineage Process for batch '{batch_id}' in project '{project}'...")
    
    process_name, location = get_process_id_for_batch(project, batch_id, token)
    if not process_name:
        print(f"Validation failed: Could not find a lineage process matching batch ID '{batch_id}' across standard regions.")
        sys.exit(1)
        
    print(f"Found Process in region {location}:\n -> {process_name}")
    print("\nFetching lineage events...")
    
    events_with_context = get_runs_and_events(process_name, token)
    print_lineage(events_with_context, batch_id)
