import os
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from neo4j import GraphDatabase
from airflow.models import Variable

username = Variable.get("NEO4J_USER")
password = Variable.get("NEO4J_PASSWORD")

# Define the DAG
dag = DAG(
    'xml_to_neo4j',
    start_date=datetime(2023, 3, 26),  # Replace with your desired start date
    schedule_interval=None,  # Set to None to manually trigger the DAG
    catchup=False  # Set to False to skip any intervals where the DAG would have been inactive
)

# Define the Python function to download the XML file from Github
def download_xml_file():
    url = 'https://github.com/weavebio/data-engineering-coding-challenge/raw/main/data/Q9Y261.xml'
    response = requests.get(url)
    with open('/tmp/Q9Y261.xml', 'wb') as file:
        file.write(response.content)

# Define the Bash command to create the Neo4j nodes and relationships
create_neo4j_nodes_cmd = f"""
    cat /tmp/Q9Y261.xml |
    grep -E "<protein id=\\"[0-9a-zA-Z_-]+\\">" |
    sed -E 's/.*id=\\"([0-9a-zA-Z_-]+)\\".*/\\1/' |
    while read id; do
        cypher-shell --format plain --database neo4j --username {NEO4J_USER} --password {NEO4J_PASSWORD} \
        -- "CREATE (:Protein {{id: '{id}'}});"
    done
"""

create_neo4j_rels_cmd = f"""
    cat /tmp/Q9Y261.xml |
    grep -E "<interaction id=\"[0-9a-zA-Z_-]+\">" |
    sed -E 's/.*source=\"([0-9a-zA-Z_-]+)\".*target=\"([0-9a-zA-Z_-]+)\".*/\\1 \\2/' |
    while read source target; do
        printf "MATCH (p1:Protein {{id: \\"%s\\"}}), (p2:Protein {{id: \\"%s\\"}}) CREATE (p1)-[:INTERACTS_WITH]->(p2);\\n" "$source" "$target"
    done |
    cypher-shell --format plain --database neo4j --username {username} --password {password}
"""

# Define the Python function to load the XML file into Neo4j
def load_xml_file_to_neo4j():
    driver = GraphDatabase.driver("neo4j://localhost:7687", auth=(username, password))
    with driver.session() as session:
        session.run(create_neo4j_nodes_cmd.format(username, password))
        session.run(create_neo4j_rels_cmd.format(username, password))

# Define the DAG tasks
download_file_task = PythonOperator(
    task_id='download_file',
    python_callable=download_xml_file,
    dag=dag
)

create_nodes_task = BashOperator(
    task_id='create_nodes',
    bash_command=create_neo4j_nodes_cmd,
    dag=dag
)

create_rels_task = BashOperator(
    task_id='create_relationships',
    bash_command=create_neo4j_rels_cmd,
    dag=dag
)

load_file_task = PythonOperator(
    task_id='load_file_to_neo4j',
    python_callable=load_xml_file_to_neo4j,
    dag=dag
)

# Define the task dependencies
download_file_task >> create_nodes_task
download_file_task >> create_rels_task
create_nodes_task >> load_file_task
create_rels_task >> load_file_task
