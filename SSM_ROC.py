import neo4j
from neo4j import GraphDatabase

import json
from jsonpath import jsonpath

# This part for local test only. Comment this part for development test. 
driver = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("faiz", "Kewmann@123"))
session = driver.session()

class n4j_process(Agency):
    
    # 1. Get data
    def __init__(self, data, driver, rfi, jops, ip):
        self.df = data
        self.driver = driver
        self.rfi = rfi
        self.jops = jops
        self.ip = ip

    # 2. Define graph model to create person node
    def create_person_node(px, name, idNo, rfi, jops, ip):
        px.run("MERGE (p$idNo:Person {name: $name, idNo: $idNo, rfi: $self.rfi, jops: $self.jops, ip: $self.ip})", 
        name=name, idNo=idNo, rfi=rfi, jops=jops, ip=ip)

    # 3. Define graph model to create entity node and create relationship
    def create_entity_rel(rx, idNo, companyName, companyNo, designation): 
        rx.run("MATCH (p$idNo:Person) WHERE p$idNo.idNo = $idNo "
            "MERGE (p$idNo)-[:Position {designation: $designation}]->(p$companyNo:entity {companyName: $companyName, companyNo: $companyNo})", 
            idNo=idNo, companyName=companyName, companyNo=companyNo, designation=designation)
    
    # 4. Extract json file to get name, idno, companyname, companyno, designation
    name = jsonpath(self.df, "$.name") 
    idNo = jsonpath(self.df, "$.idNo")  
    companyName = jsonpath(self.df, "$..rocCompanyOfficerInfoList[*].companyName")
    companyNo   = jsonpath(self.df, "$..rocCompanyOfficerInfoList[*].companyNo")
    designation = jsonpath(self.df, "$..rocCompanyOfficerInfoList[*].designation")

    # 5. Generate person node
    with driver.session() as session:
        session.write_transaction(create_person_node, name, idNo, rfi=self.rfi, jops=self.jops, ip=self.ip)   
    driver.close()

    # 6. Generate entity node and generate relationship
    for n in range(len(companyName)):
        
        with driver.session() as session:
            session.write_transaction(create_entity_rel, idNo, companyName[n], companyNo[n], designation[n])
            
    driver.close()