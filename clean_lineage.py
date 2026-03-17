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

def delete_resource(url, token):
    req = urllib.request.Request(url, method="DELETE", headers={"Authorization": f"Bearer {token}"})
    try:
        urllib.request.urlopen(req)
        return True
    except urllib.error.HTTPError as e:
        print(f"Failed to delete {url}: {e.code} - {e.read().decode()}")
        return False

def clean_all_lineage():
    project = get_project()
    token = get_access_token()
    locations = ["us-east1", "us-central1", "us"]

    for location in locations:
        print(f"\nScanning location: {location} in project {project}...")
        base_url = f"https://datalineage.googleapis.com/v1/projects/{project}/locations/{location}/processes"
        
        processes_to_delete = []
        next_page = ""
        
        # 1. Collect all processes
        while True:
            url = base_url
            if next_page:
                url += f"?pageToken={urllib.parse.quote(next_page)}"
                
            req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
            try:
                with urllib.request.urlopen(req) as response:
                    data = json.loads(response.read().decode())
                    for process in data.get('processes', []):
                        processes_to_delete.append(process['name'])
                        
                    next_page = data.get('nextPageToken')
                    if not next_page:
                        break
            except urllib.error.HTTPError as e:
                break

        print(f"Found {len(processes_to_delete)} processes.")
        
        # 2. Delete runs and processes
        for process_name in processes_to_delete:
            print(f"Cleaning process: {process_name}")
            
            # Fetch runs
            runs_url = f"https://datalineage.googleapis.com/v1/{process_name}/runs"
            req = urllib.request.Request(runs_url, headers={"Authorization": f"Bearer {token}"})
            
            try:
                with urllib.request.urlopen(req) as r:
                    runs_data = json.loads(r.read().decode())
                    for run in runs_data.get('runs', []):
                        run_name = run['name']
                        # Fetch events to delete them
                        events_url = f"https://datalineage.googleapis.com/v1/{run_name}/lineageEvents"
                        ereq = urllib.request.Request(events_url, headers={"Authorization": f"Bearer {token}"})
                        try:
                            with urllib.request.urlopen(ereq) as er:
                                events_data = json.loads(er.read().decode())
                                for event in events_data.get('lineageEvents', []):
                                    event_name = event['name']
                                    delete_resource(f"https://datalineage.googleapis.com/v1/{event_name}", token)
                        except Exception:
                            pass
                            
                        # Delete Run
                        delete_resource(f"https://datalineage.googleapis.com/v1/{run_name}", token)
            except Exception:
                pass

            # Delete Process
            if delete_resource(f"https://datalineage.googleapis.com/v1/{process_name}", token):
                print(f"  Successfully deleted process.")

if __name__ == "__main__":
    clean_all_lineage()
    print("\nCleanup complete!")
