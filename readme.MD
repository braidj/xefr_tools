# Overview
This is a command line tool for creating, viewing data and reporting on schema and portal definitions.

Requires a xefr_tools.ini to be created to enable the connections.

# xefr_tools.ini set up

## [ACTIVE]
This is where the connection section to be used is recorded.  
It requires the name of a subsequent section that details all the connection details.  

Example:

    [ACTIVE]
    target = Zhero LOCAL
    notes = set the target section here for connection

## Connection details
**4 Mandatory items**
1. api_key = 1234abcd
2. database = xefr-signify-dev
3. instance = DEV
4. server = https://xefr-signify-dev.azurewebsites.net
5. uri = mongodb+srv://signify-dev:abcdefgh

**Optional**
notes = passed  

Example: (with redaction)

    [Signify DEV]
    api_key = 12345abcd
    database = xefr-signify-dev
    instance = DEV
    server = https://xefr-signify-dev.azurewebsites.net
    uri = mongodb+srv://signify-dev:aareallylongstring
    notes = passed

### How to get the values
1. api_key: XEFR/Settings/API Keys, create you own personal one
2. database: login to mongo Atlas / Compass, and ensure you are using the right name
3. instance: just a text description, but it used to generate the download folder locally
4. server: for no local got to Azure Services to get this
5. uri: go to Azure services for this

# Code modules

## common_funcs
TBC

## json_tools
TBC

## mongo_connector
TBC

## xefr_cli
TBC

## xefr_endpoints
TBC









