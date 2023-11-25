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
**5 Mandatory items**
1. api_key
2. database
3. instance
4. server
5. uri

**1 Optional**
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
4. server: for non local got to Azure Services to get this
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

# Usage
Its a CLI so once it is running type:

? to list all available commands
x to close the session
r to re-run the last command

You only enter the command id, not the description

When running a command requiring a schema name, you can refer to the schema by using a s shorthand.
s1 refers to the first schema in the list
E.G.  
To display the data contained within the first schema you would type  
3 (the command id)  
s1 (the first schema)









