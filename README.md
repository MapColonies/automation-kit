[![Python 3.6](https://img.shields.io/badge/python-3.6-green.svg)](https://www.python.org/downloads/release/python-360/)
<img alt="GitHub release (latest by date including pre-releases)" src="https://img.shields.io/github/v/release/MapColonies/automation-kit">
![GitHub](https://img.shields.io/github/license/MapColonies/automation-kit)
<img alt="GitHub last commit" src="https://img.shields.io/github/last-commit/MapColonies/automation-kit">

# automation-kit

### This pytonic package include useful sub-packages and implementation for variant manipulation of automation infrastructure.

### Include:
1. base_request - wrapper for restful API's requests [post, get, etc..]
2. S3storage - wrapper of boto3 library for handling with S3 storage type
3. Common - common utils for string manipulations, urls, storage, os, and data-types convertor
4. Jira - integration sub-package for connecting pytest results with jira's dashboard.
5. PostGres support wrapping module that provide useful class and function working with pg
6. MC-ingestion mutual API's wrappers:
    * job manager api wrapper:
        * Multiple API access
        * Automation utilities -> Job Follower[Example]
    * overseer api wrapper
    * agent api wrapper
    * azure pvc handler -> automation internal service to mock NFS
7. MC-sync mutual API's wrappers:
    * layer spec api wrapper:
        * Multiple API access as describe on layer spec api docs
