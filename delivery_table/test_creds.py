import google.auth
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

def test_google_authentication():
    try:
        # Attempt to get the default credentials
        credentials, project = google.auth.default()

        # Check if the credentials are valid (if they expire, refresh them)
        if credentials.valid:
            print("Authentication successful!")
        elif credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
            print("Authentication successful, credentials refreshed.")
        else:
            print("Authentication failed: credentigals are invalid or expired.")
            return

        # Now, let's test by listing the projects under your Google Cloud account
        service = build('cloudresourcemanager', 'v1', credentials=credentials)
        request = service.projects().list()
        response = request.execute()

        if 'projects' in response:
            print("Projects available to your account:")
            for project in response['projects']:
                print(f"- {project['name']} (ID: {project['projectId']})")
        else:
            print("No projects found or unable to list projects.")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    test_google_authentication()
