#!/bin/bash
PROJECT="YOUR_PROJECT_ID"
LOCATION="us-east1"
ENTRY_GROUP="wpp-lineage-poc-lineage-api-group"
ENTRY_TYPE="projects/YOUR_PROJECT_NUMBER/locations/us-east1/entryTypes/wpp-lineage-poc-custom-type"

TOKEN=$(gcloud auth print-access-token)

# 1. Create a Test Source Entry
echo "Creating Test Source Entry..."
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data "{
    \"entryType\": \"$ENTRY_TYPE\",
    \"fullyQualifiedName\": \"custom:my_system.test_source\",
    \"entrySource\": {
      \"system\": \"cloud_storage\",
      \"displayName\": \"Test Source FQN UI\"
    }
  }" \
  "https://dataplex.googleapis.com/v1/projects/$PROJECT/locations/$LOCATION/entryGroups/$ENTRY_GROUP/entries?entryId=test-source-ui"

# 2. Create a Test Dest Entry
echo -e "\n\nCreating Test Dest Entry..."
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data "{
    \"entryType\": \"$ENTRY_TYPE\",
    \"fullyQualifiedName\": \"custom:my_system.test_dest\",
    \"entrySource\": {
      \"system\": \"cloud_storage\",
      \"displayName\": \"Test Dest FQN UI\"
    }
  }" \
  "https://dataplex.googleapis.com/v1/projects/$PROJECT/locations/$LOCATION/entryGroups/$ENTRY_GROUP/entries?entryId=test-dest-ui"

# 3. Create a Process
echo -e "\n\nCreating Lineage Process..."
PROCESS_POST=$(curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data "{
    \"displayName\": \"Test UI Lineage Process\",
    \"origin\": {
      \"sourceType\": \"CUSTOM\",
      \"name\": \"custom\"
    }
  }" \
  "https://datalineage.googleapis.com/v1/projects/$PROJECT/locations/$LOCATION/processes")
echo $PROCESS_POST
PROCESS_NAME=$(echo $PROCESS_POST | jq -r .name)

# 4. Create a Run
echo -e "\n\nCreating Lineage Run..."
RUN_POST=$(curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data "{
    \"displayName\": \"Test UI Lineage Run\",
    \"state\": \"COMPLETED\",
    \"startTime\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",
    \"endTime\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"
  }" \
  "https://datalineage.googleapis.com/v1/$PROCESS_NAME/runs")
echo $RUN_POST
RUN_NAME=$(echo $RUN_POST | jq -r .name)

# 5. Create a Lineage Event mapping the EXACT custom FQNs
echo -e "\n\nCreating Lineage Event..."
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data "{
    \"startTime\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",
    \"endTime\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",
    \"links\": [
      {
        \"source\": { \"fullyQualifiedName\": \"custom:my_system.test_source\" },
        \"target\": { \"fullyQualifiedName\": \"custom:my_system.test_dest\" }
      }
    ]
  }" \
  "https://datalineage.googleapis.com/v1/$RUN_NAME/lineageEvents"
