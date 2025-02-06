import pyrfc
from pyrfc import Connection
import json

# Load SAP connection parameters from settings.json
with open('settings.json', 'r') as file:
    sap_conn_params = json.load(file)

# Establish connection to SAP
conn = Connection(**sap_conn_params)

# Function to run RFC_READ_TABLE with SQL query style
def fetch_data_from_sap(table_name, fields, where_clauses=None):
    options = [{'TEXT': clause} for clause in where_clauses] if where_clauses else []
    parameters = {
        'QUERY_TABLE': table_name,
        'DELIMITER': '|',
        'FIELDS': [{'FIELDNAME': field} for field in fields],
        'OPTIONS': options
    }
    
    result = conn.call('RFC_READ_TABLE', **parameters)
    data = result['DATA']
    columns = [field['FIELDNAME'] for field in result['FIELDS']]
    
    # Parse the result
    parsed_data = []
    for row in data:
        values = row['WA'].split('|')
        parsed_data.append(dict(zip(columns, values)))
    
    return parsed_data

# Example usage
table_name = 'T333'
fields = ['LGNUM', 'BWLVS', 'NLTYP']
#where_clauses = ["FIELD1 = 'some_value'", "FIELD2 = 'another_value'"]
where_clauses = None

data = fetch_data_from_sap(table_name, fields, where_clauses)
for row in data:
    print(row)

# Close the connection
conn.close()