from jinja2 import FileSystemLoader, Environment
from csv import DictReader

sample_metadata = u'''
AE_HDS,HDS_T_DPC_PRODUCTS,AVAILABILITY,TEXT,YES,,
AE_HDS,HDS_T_DPC_PRODUCTS,CLIENTID,NUMBER,YES,,
'''

file_loader = FileSystemLoader('templates')
env = Environment(loader=file_loader)

template = env.get_template('sat_metadata_csv.jinja')

table_name = 'DPC_PRODUCTS'

with open('./HDS_T_' + table_name + '_columns.csv') as inputfile:
    rows = DictReader(inputfile, fieldnames=['schema', 'table', 'column', 'data_type', 'nullable', 'comment'])
    output = template.render(rows=rows, table_name=table_name)

with open('./metadata/sat_metadata_' + table_name + '.csv', 'w') as outputfile:
    outputfile.write(output)
