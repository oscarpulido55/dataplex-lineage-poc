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

def search_links(project, location, token, target_fqn):
    base_url = f"https://datalineage.googleapis.com/v1/projects/{project}/locations/{location}:searchLinks"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # We want to find processes that read from or write to this table.
    payload = {
        "target": {
            "fullyQualifiedName": target_fqn
        }
    }
    
    req = urllib.request.Request(base_url, headers=headers, data=json.dumps(payload).encode('utf-8'), method='POST')
    links = []
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            links = data.get('links', [])
            
            # handle pagination if necessary
            next_page = data.get('nextPageToken')
            while next_page:
                payload['pageToken'] = next_page
                req = urllib.request.Request(base_url, headers=headers, data=json.dumps(payload).encode('utf-8'), method='POST')
                with urllib.request.urlopen(req) as response:
                    data = json.loads(response.read().decode())
                    links.extend(data.get('links', []))
                    next_page = data.get('nextPageToken')
                    
    except urllib.error.HTTPError as e:
        print(f"Error searching links in {location}: {e.code} {e.reason}")
        
    return links

def get_run_details(run_name, token):
    url = f"https://datalineage.googleapis.com/v1/{run_name}"
    headers = {"Authorization": f"Bearer {token}"}
    req = urllib.request.Request(url, headers=headers)
    try:
         with urllib.request.urlopen(req) as response:
             return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
         return None

def get_process_details(process_name, token):
    url = f"https://datalineage.googleapis.com/v1/{process_name}"
    headers = {"Authorization": f"Bearer {token}"}
    req = urllib.request.Request(url, headers=headers)
    try:
         with urllib.request.urlopen(req) as response:
             return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
         return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python inspect_lineage_by_table.py <TARGET_FQN_OR_TABLE_ID>")
        print("Example: python inspect_lineage_by_table.py bigquery:my-project.demo_lineage_poc_pipeline6_native.pipeline6_source")
        sys.exit(1)
        
    identifier = sys.argv[1]
    
    # Auto-format to bigquery fqn if they just passed project.dataset.table
    if not identifier.startswith("bigquery:") and not identifier.startswith("custom:") and not identifier.startswith("gcs:"):
        identifier = f"bigquery:{identifier}"
        
    project = get_project()
    token = get_access_token()
    
    print(f"Searching for lineage links targeting: {identifier}")
    print("=" * 80)
    
    locations = ["us-east1", "us-central1", "us"]
    all_links = []
    for loc in locations:
        links = search_links(project, loc, token, identifier)
        if links:
            all_links.extend(links)
            
    if not all_links:
        print("No upstream lineage links found for this target.")
        sys.exit(0)
        
    # Group by Process
    runs_by_process = {}
    for link in all_links:
        # A Link object natively contains "source", "target", "startTime", "endTime"
        pass 
        
    # Wait, the searchLinks API returns links, but we want the Events and Runs.
    print("Found links! However, searchLinks only shows edges.")
    print("Let's query searchLineageEvents to get the actual Runs and Processes.")
    print("=" * 80)

    # Re-searching via searchLineageEvents is better to get the run.
    for loc in locations:
        events_url = f"https://datalineage.googleapis.com/v1/projects/{project}/locations/{loc}:searchLineageEvents"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {
            "source": {"fullyQualifiedName": identifier}
        }
        req = urllib.request.Request(events_url, headers=headers, data=json.dumps(payload).encode('utf-8'), method='POST')
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode())
                events = data.get('lineageEvents', [])
                for e in events:
                    # 'name' is the event node, e.g., projects/P/locations/L/processes/P/runs/R/lineageEvents/E
                    event_name = e.get("name")
                    # Extract Run and Process
                    parts = event_name.split("/")
                    run_name = "/".join(parts[:8])
                    process_name = "/".join(parts[:6])
                    
                    process_details = get_process_details(process_name, token)
                    p_displayName = process_details.get("displayName", "N/A") if process_details else "N/A"
                    p_name = process_details.get("origin", {}).get("name", "N/A") if process_details else "N/A"
                    print(f"Process Name (ID): {process_name}")
                    print(f"Process DisplayName: {p_displayName}")
                    print(f"Process Origin Name (Job Name): {p_name}")
                    print(f"  -> Run ID: {run_name}")
                    print("-" * 40)
        except urllib.error.HTTPError as e:
            pass
