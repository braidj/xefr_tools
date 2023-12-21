"""
Roles and permissions to apply for a given application
Hierachy is application
    write access - > role - > schemas

Only two levels of access write and read
This access is granted by a role, with a role being made up of one or more users
This then applied at a schema level, and portals / portlets.
Not sure what happens if granted write access to portal but not schema
"""


portal_permissions = {

    "signify":{
        "Bullhorn":"finance",
        "Configuration" :"finance",
        "Data":"finance",
        "Dates":"finance",
        "External Data":"finance",
        "FOREX":"finance",
        "GBP FOREX":"finance",
        "Inter Company":"finance",
        "Leaderboards":"finance,frontoffice",
        "Leaders":"finance",
        "NFI":"finance",
        "New Deal Pivots":"finance",
        "New Deal Validations":"finance",
        "New Deals":"finance",
        "New Deals Config":"finance",
        "Retainers":"finance",
        "System":"finance",
        "TSP UK":"finance",
        "TSP US":"finance",
        "UK NFI":"finance",
        "US NFI":"finance",
        "USD FOREX" :"finance"
    }

}
