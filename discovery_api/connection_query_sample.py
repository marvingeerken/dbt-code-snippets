# Sample script to connect to the dbt Cloud Discovery API and query a set of models

import requests

def fetch_api_data(api_url, access_token, query, variables=None):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    data = {
        "query": query,
        "variables": variables
    }

    response = requests.post(api_url, json=data, headers=headers)

    if response.status_code == 200:
        result = response.json()
        return result
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None


api_url = "https://metadata.cloud.getdbt.com/graphql"
access_token = "dbts_foo" # Replace dbt Cloud service token
jobId = 240681 # Replace dbt Cloud jobId

query = """
query Models($jobId: Int!) {
  models(jobId: $jobId) {
    name
    tests {
      columnName
      dependsOn
    }
  }
}
"""

variables = {
    "jobId": jobId
}

data = fetch_api_data(api_url, access_token, query, variables)

if data:

    # Filter models based on "tests" that have "dependsOn" value of "test_relationships"
    deps_list = []
    for test in data["data"]["models"]:
        tests = test.get("tests", [])
        for test in tests:
            depends_on = test.get("dependsOn", [])
            if "macro.dbt.test_relationships" in depends_on:
                deps_list.append((depends_on[0],depends_on[1],test.get("columnName", "")))
                break

    deps_list = list(set(deps_list))
    print(deps_list)
