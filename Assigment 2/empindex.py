from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd

# Load dataset
df = pd.read_csv('C:\Users\VICTUS\Downloads\archive\Employee Sample Data 1.csv')

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Check connection
if es.ping():
    print("Connected to Elasticsearch")
else:
    print("Could not connect to Elasticsearch")

# Create index 
index_name = 'employees'
mapping = {
    "mappings": {
        "properties": {
            "Employee_ID": {"type": "integer"},
            "Full_Name": {"type": "text"},
            "Job_Title": {"type": "text"},
            "Department": {"type": "text"},
            "Business_Unit": {"type": "text"},
            "Gender": {"type": "keyword"},
            "Ethnicity": {"type": "keyword"},
            "Age": {"type": "integer"},
            "Hire_Date": {"type": "date", "format": "yyyy-MM-dd"},
            "Annual_Salary": {"type": "double"},
            "Bonus_%": {"type": "float"},
            "Country": {"type": "text"},
            "City": {"type": "text"},
            "Exit_Date": {"type": "date", "format": "yyyy-MM-dd"},
        }
    }
}
#Create the index in Elasticsearch
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=index_settings)
    print(f"Index '{index_name}' created")
    
#Upload data to Elasticsearch
def index_data(row):
    doc = {
        'Employee_ID': int(row['Employee ID']),
        'Full_Name': row['Full Name'],
        'Job_Title': row['Job Title'],
        'Department': row['Department'],
        'Business_Unit': row['Business Unit'],
        'Gender': row['Gender'],
        'Ethnicity': row['Ethnicity'],
        'Age': int(row['Age']),
        'Hire_Date': row['Hire Date'],
        'Annual_Salary': float(row['Annual Salary']),
        'Bonus_%': float(row['Bonus %']),
        'Country': row['Country'],
        'City': row['City'],
        'Exit_Date': row['Exit Date'] if pd.notna(row['Exit Date']) else None,
    }
# Index each row in Elasticsearch
    es.index(index=index_name, body=doc)

# Iterate over each row and upload the data
df.apply(index_data, axis=1)
print('Data indexed successfully')

# 1. indexData: Index employee data, excluding a specific column
def indexData(p_collection_name, p_exclude_column):
    def index_data_excluding(row):
        doc = {key: value for key, value in row.items() if key != p_exclude_column}
        es.index(index=p_collection_name, body=doc)

    df.apply(index_data_excluding, axis=1)
    print(f"Data indexed in '{p_collection_name}', excluding column '{p_exclude_column}'")

# 2. searchByColumn: Search for records matching a specific column value
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    results = es.search(index=p_collection_name, body=query)
    for hit in results['hits']['hits']:
        print(hit['_source'])

# 3. getEmpCount: Retrieve the count of employees in the collection
def getEmpCount(p_collection_name):
    count = es.count(index=p_collection_name)['count']
    print(f"Total employees in '{p_collection_name}': {count}")
    return count

# 4. delEmpById: Delete an employee by ID
def delEmpById(p_collection_name, p_employee_id):
    query = {
        "query": {
            "match": {
                "Employee_ID": p_employee_id
            }
        }
    }
    # Search for the employee and delete if found
    results = es.search(index=p_collection_name, body=query)
    if results['hits']['hits']:
        doc_id = results['hits']['hits'][0]['_id']
        es.delete(index=p_collection_name, id=doc_id)
        print(f"Employee with ID '{p_employee_id}' deleted successfully.")
    else:
        print(f"Employee with ID '{p_employee_id}' not found.")

# 5. getDepFacet: Get count of employees grouped by department
def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    results = es.search(index=p_collection_name, body=query)
    for bucket in results['aggregations']['departments']['buckets']:
        print(f"Department: {bucket['key']}, Count: {bucket['doc_count']}")

# Function Executions
v_nameCollection = 'Hash_YourName'
v_phoneCollection = 'Hash_1234'  # Replace 1234 with the last four digits of your phone number

# 3,4. Create collections 
createCollection(v_nameCollection)
createCollection(v_phoneCollection)

# 5. Get employee count for v_nameCollection
getEmpCount(v_nameCollection)

# 6. Index data in v_nameCollection excluding 'Department'
indexData(v_nameCollection, 'Department')

# 7. Index data in v_phoneCollection excluding 'Gender'
indexData(v_phoneCollection, 'Gender')

# 8. Delete employee by ID 'E02003' in v_nameCollection
delEmpById(v_nameCollection, 'E02003')

# 9. Get employee count after deletion for v_nameCollection
getEmpCount(v_nameCollection)

# 10. Search by 'Department' with value 'IT' in v_nameCollection
searchByColumn(v_nameCollection, 'Department', 'IT')

# 11. Search by 'Gender' with value 'Male' in v_nameCollection
searchByColumn(v_nameCollection, 'Gender', 'Male')

# 12. Search by 'Department' with value 'IT' in v_phoneCollection
searchByColumn(v_phoneCollection, 'Department', 'IT')

# 13. Get department facets for v_nameCollection
getDepFacet(v_nameCollection)

# 14. Get department facets for v_phoneCollection
getDepFacet(v_phoneCollection)
