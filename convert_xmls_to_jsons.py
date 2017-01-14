# -*- coding: utf-8 -*-
import json
import os
import xmltodict as xmltodict

__author__ = 'goran'


# os.mkdir('jsons')
# for f in os.listdir('wwwStore'):
#     with open('wwwStore/{}'.format(f)) as fin:
#         out_name = f.split('.')[0] + '.json'
#         with open('jsons/{}'.format(out_name), 'w') as fout:
#             fout.write(json.dumps(xmltodict.parse(fin.read())))
#


def key_from_name(name):
    if name == 'lineitem':
        return 'L_LINENUMBER'
    if name == 'orders':
        return 'O_ORDERKEY'
    if name == 'customer':
        return 'C_CUSTKEY'
    if name == 'supplier':
        return 'S_SUPPKEY'
    return '{}_{}key'.format(name[0], name).upper()


def name_from_key(key):
    return key[2:-3].lower()


ordered_docs = ['region', 'nation', 'customer', 'part', 'supplier', 'partsupp', 'orders', 'lineitem']
foreign_keys = {
    'nation': ['N_REGIONKEY']
}


def encode_part_sup(partkey, supkey):
    return '{}_{}'.format(partkey, supkey)


parsed_data = {}
part_supp_keys = {}

for collection_name in ordered_docs:
    file_name = collection_name + '.xml'
    key = key_from_name(collection_name)

    with open('wwwStore/{}'.format(file_name)) as fin:
        data = xmltodict.parse(fin.read())
        data_for_keys = {}

        # make key -> data dict for this collection
        for d in data['table']['T']:
            if collection_name == 'partsupp':
                combined_key = encode_part_sup(d['PS_PARTKEY'], d['PS_SUPPKEY'])
                data_for_keys[combined_key] = d.copy()
            else:
                # print(collection_name)
                data_for_keys[d[key]] = d.copy()

        # replace foreign keys with actual data
        for line in data['table']['T']:
            if collection_name == 'lineitem':
                foreign_key = encode_part_sup(line['L_PARTKEY'], line['L_SUPPKEY'])

                referenced_collection = 'partsupp'
                data_for_keys[line[key]][referenced_collection] = parsed_data[referenced_collection][foreign_key]

                # referenced_collection = name_from_key('L_ORDERKEY')
                data_for_keys[line[key]]['order'] = parsed_data['orders'][line['L_ORDERKEY']]
            else:
                if collection_name == 'partsupp':
                    data_for_keys['partsup'] = {}
                    data_for_keys['partsup']['part'] = parsed_data['part'][line['PS_PARTKEY']]
                    data_for_keys['partsup']['supplier'] = parsed_data['supplier'][line['PS_SUPPKEY']]
                elif collection_name:
                    for k, v in line.items():
                        if 'KEY' in k and k != key:
                            referenced_collection = name_from_key(k)
                            if referenced_collection == 'cust':
                                referenced_collection = 'customer'
                            data_for_keys[line[key]][referenced_collection] = parsed_data[referenced_collection][v]

        parsed_data[collection_name] = data_for_keys

print(parsed_data.keys())
with open('jsons/all.json', 'w') as fout:
    fout.write(json.dumps(parsed_data, indent=4))

with open('jsons/items.json', 'w') as fout:
    fout.write(json.dumps(parsed_data['lineitem'], indent=4))