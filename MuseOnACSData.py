#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
sys.path.append('On_Multiple_Semantics_for_Declarative_Database_Repairs/')
import On_Multiple_Semantics_for_Declarative_Database_Repairs

from database_generator.dba import DatabaseEngine
from Semantics.end_sem import EndSemantics
from Semantics.stage_sem import StageSemantics
from Semantics.step_sem import StepSemantics
from Semantics.independent_sem import IndependentSemantics

# specify the schema
mas_schema = {"synthetic_data_version_1": ('RAC1P', 'SEX', 'REGION', 'PINCP', 'ST', 'COW', 'CIT', 'NATIVITY', 'MSP', 'SCHL', 'DIS','MIL')
              }


def read_rules(rule_file):
    """read programs from txt file"""
    all_programs = []
    with open(rule_file) as f:
        rules = []
        for line in f:
            if line.strip():
                tbl, r = line.split("|")
                rules.append((tbl, r[:-2]))
            else:
                all_programs.append([r for r in rules])
                rules = []
        all_programs.append(rules)
    return all_programs

# load delta programs
programs = read_rules("On_Multiple_Semantics_for_Declarative_Database_Repairs/../delta_program.txt")

# start the database
db = DatabaseEngine("cr")

tbl_names = ["synthetic_data_version_1"]
subfolder = "/datasets/"

def database_reset(db):
    """reset the database"""
    res = db.execute_query("select current_database();")
    db_name = res[0][0]
    db.delete_tables(tbl_names)
    # db.close_connection()
    db = DatabaseEngine(db_name)
    db.load_database_tables(tbl_names)

# choose a delta program from the programs file
rules = programs[0]
print("delta program:", rules)

# # reset the database between runs
# database_reset(db)

# end_sem = EndSemantics(db, rules, tbl_names)
# end_semantics_result = end_sem.find_mss()
# print("result for end semantics:", end_semantics_result)

# # reset the database between runs
# database_reset(db)

# stage_sem = StageSemantics(db, rules, tbl_names)
# stage_semantics_result = stage_sem.find_mss()
# print("result for stage semantics:", stage_semantics_result)

# reset the database between runs
database_reset(db)

step_sem = StepSemantics(db, rules, tbl_names)
step_semantics_result = step_sem.find_mss(mas_schema)
print("result for step semantics:", step_semantics_result)

# reset the database between runs
# database_reset(db)

# ind_sem = IndependentSemantics(db, rules, tbl_names)
# ind_semantics_result = ind_sem.find_mss(mas_schema)
# print("result for independent semantics:", ind_semantics_result)


# In[ ]:


len(step_semantics_result)


# In[ ]:




