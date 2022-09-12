import neo4j
from neo4j import GraphDatabase
from NFIS_Graph.agency import Agency

import json
from jsonpath import jsonpath

# This part for local testing only. Comment this part for development test. 
# driver = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("faiz", "Kewmann@123"))
# session = driver.session()

class n4j_process_rob(Agency):
    
    # 1. Define data
    def __init__(self, data, driver, annotation, rfi, jops, ip):
        self.df = data
        self.driver = driver
        self.annotation = annotation
        self.rfi = rfi
        self.jops = jops
        self.ip = ip

    # 2. Define graph model to create person node
    def create_person_node(px, name, icNumber, annotation, rfi, jops, ip):
        px.run("MERGE (p$icNumber:Person {name: $name, icNumber: $icNumber, annotation:$self.annotation, rfi: $self.rfi, jops: $self.jops, ip: $self.ip})", 
        name=name, icNumber=icNumber, annotation=annotation, rfi=rfi, jops=jops, ip=ip)

    # 3. Define graph model to create entity node and create relationship
    def create_entity_rel(rx, icNumber, businessName, businessRegNo): 
        rx.run("MATCH (p$icNumber:Person) WHERE p$icNumber.icNumber = $icNumber "
            "MERGE (p$icNumber)-[:OWNER]->(p$companyNo:entity {businessName: $businessName, businessRegNo: $businessRegNo})", 
            icNumber=icNumber, businessName=businessName, businessRegNo=businessRegNo)
    
    # 4. Extract json file to get name, idno, businessName, businessRegNo
        icNumber = jsonpath(self.df, "$..icNumber")
        name = jsonpath(self.df, "$.name")
        businessName  = jsonpath(self.df, "$..businessName")
        businessRegNo = jsonpath(self.df, "$..businessRegNo")

    # 5. Generate person and owner nodes.
    with driver.session() as session:

        session.write_transaction(create_person_node, name, icNumber, rfi=self.rfi, jops=self.jops, ip=self.ip)   
        session.write_transaction(create_entity_rel, icNumber, businessName, businessRegNo)

    driver.close()