import sys
sys.path.append('On_Multiple_Semantics_for_Declarative_Database_Repairs')
import On_Multiple_Semantics_for_Declarative_Database_Repairs

from database_generator.dba import DatabaseEngine
from Semantics.end_sem import EndSemantics
from Semantics.stage_sem import StageSemantics
from Semantics.step_sem import StepSemantics
from Semantics.independent_sem import IndependentSemantics

import pandas as pd
import psycopg2

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

def database_reset(db):
    """reset the database"""
    res = db.execute_query("select current_database();")
    db_name = res[0][0]
    db.delete_tables(tbl_names)
    # db.close_connection()
    db = DatabaseEngine(db_name)
    db.load_database_tables(tbl_names)


# choose a delta program from the programs file
step_semantics_results = []

for rules in programs:
    # start the database
    db = DatabaseEngine("cr")

    tbl_names = ["synthetic_data_version_1"] #The file being read is '/datasets/muse/synthetic_data_version_1.csv'. If you want to change this, then you have to change this line as well as 'On_Multiple_Semantics_for_Declarative_Database_Repairs/database_generator/dba.py'.

    # specify the schema
    mas_schema = {tbl_names[0]: ('RAC1P', 'SEX', 'REGION', 'PINCP', 'ST', 'COW', 'CIT', 'NATIVITY', 'MSP', 'SCHL', 'DIS','MIL')}

    print("delta program:", rules)
    
    # reset the database between runs
    database_reset(db)
    
    step_sem = StepSemantics(db, rules, tbl_names)
    step_semantics_result = step_sem.find_mss(mas_schema)
    print("result for step semantics:", step_semantics_result)
    step_semantics_results.extend(step_semantics_result)



# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    database="cr",
    user="postgres",
    password="postgres"
)

# Execute an SQL query and create a dataframe 
muse_data_step = pd.read_sql("SELECT * FROM synthetic_data_version_1", conn).iloc[1:]
# muse_data_step.drop(muse_data_step.index[0])
muse_data_step.columns = muse_data_step.columns.str.upper()

conn.close()



def make_col_int(df, *args):
    for col in args:
        df[col] = df[col].astype(float)
        df[col] = df[col].astype(int)
def make_col_string(df, *args):
    for col in args:
        df[col] = df[col].astype(str)
        
make_col_int(muse_data_step, 'RAC1P', 'SEX', 'REGION', 'ST', 'CIT', 'NATIVITY', 'DIS')
make_col_string(muse_data_step, 'PINCP', 'COW', 'MSP', 'SCHL', 'MIL')
print(muse_data_step.dtypes)

print(muse_data_step)


tuples = [tup[1] for tup in step_semantics_results]
for t in tuples:
    row_index = muse_data_step.index[(muse_data_step == t).all(axis=1)]
    print(row_index)
    muse_data_step = muse_data_step.drop(row_index)

print(muse_data_step)

muse_data_step.to_csv('datasets/muse/updated_muse_data_step_testing.csv',index=False)