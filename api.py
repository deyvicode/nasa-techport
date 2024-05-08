import requests

def getProjectsSinceDate(sinceDate: str) -> list:
    res = requests.get('https://techport.nasa.gov/api/projects?updatedSince='+sinceDate).json()
    return res['projects']

def getProjectById(id: str) -> list:
    res = requests.get('https://techport.nasa.gov/api/projects/'+id).json()
    return res['project']

def getOrganizationById(id: str) -> list:
    res = requests.get('https://techport.nasa.gov/api/organizations/'+id).json()
    return res['organization']