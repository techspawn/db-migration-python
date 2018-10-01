import psycopg2
import json
import psycopg2.extras
import yaml
import argparse
import os
import traceback

from sshtunnel import SSHTunnelForwarder
from pprint import pprint

base_dir = os.getcwd()
print(base_dir)

try:
    with open(base_dir + "/conf/config.yml", 'r') as yml_file:
        cfg = yaml.load(yml_file)

    tunnel = SSHTunnelForwarder((cfg['destination']['ssh']['host'], 22), ssh_username=cfg['destination']['ssh']['user'],
                                ssh_private_key=cfg['destination']['ssh']['pkey'],
                                remote_bind_address=('localhost', 5432),
                                local_bind_address=('localhost', 6543))
    tunnel.start()
    destination = psycopg2.connect(database=cfg['destination']['db']['database'], user=cfg['destination']['db']['user'],
                                   password=cfg['destination']['db']['password'], host=tunnel.local_bind_host,
                                   port=tunnel.local_bind_port)

    source = psycopg2.connect(host=cfg['source']['host'], database=cfg['source']['database'],
                              user=cfg['source']['username'], password=cfg['source']['password'],
                              port=cfg['source']['port'])
except Exception as ex:
    traceback.print_exc(ex)
    exit(1)


def get_type(schema_name, table_name):
    cur = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
    sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE " \
          "table_schema = '%s' AND table_name = '%s'" \
          % (schema_name, table_name)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    type_map = dict()
    for row in rows:
        obj = dict()
        obj['data_type'] = row['data_type']
        obj['is_nullable'] = row['is_nullable']
        type_map[row['column_name']] = obj
    pprint(type_map)
    return type_map


def get_mapping(filename):
    with open(filename, 'r') as mapping:
        data = json.load(mapping)
        mapping.close()
        pprint(data)
        return data


def migrate(destination_schema, destination_table, schema_name, table_name):
    file = "%s/%s.json" % (base_dir, table_name)
    mapping = get_mapping(file)
    col_type = get_type(destination_schema, destination_table)
    lst = list()
    for key in mapping:
        lst.append(key)
    select_str = ",".join(lst)
    sql_select = 'select ' + select_str + ' from %s.%s' % (schema_name, table_name)
    cur = source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql_select)
    row = cur.fetchone()

    while row is not None:
        value_map = dict()
        for key in mapping:
            value_map[mapping[key]] = row[key]
        create_insert(value_map, col_type, destination_schema, destination_table)
        row = cur.fetchone()
    cur.close()


def create_insert(value_map, col_type, schema_name, table_name):
    key_lst = list()
    value_lst = list()
    query = "INSERT INTO %s.%s(%s) VALUES (%s)"
    values = ()
    for key in col_type:
        key_lst.append(key)
        if key in value_map.keys():
            values = values + (evaluate_val(col_type, key, value_map[key]),)
            value_lst.append('%s')
        else:
            values = values + (evaluate_val(col_type, key, None),)
            value_lst.append('%s')
    key_str = ",".join(key_lst)
    value_str = ",".join(value_lst)
    query = query % (schema_name, table_name, key_str, value_str)
    print(query)
    cur = destination.cursor()
    cur.execute(query, values)
    destination.commit()
    cur.close()


def evaluate_val(col_type, key, value):
    if col_type[key]['data_type'] == 'integer':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return 0
            else:
                return None
    elif col_type[key]['data_type'] == 'character varying':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return ""
            else:
                return None
    elif col_type[key]['data_type'] == 'boolean':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return False
            else:
                return None
    elif col_type[key]['data_type'] == 'text':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return ""
            else:
                return None
    elif col_type[key]['data_type'] == 'double precision':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return 0.0
            else:
                return None
    elif col_type[key]['data_type'] == 'date':
        if value is not None:
            return str(value)
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return ""
            else:
                return None
    elif col_type[key]['data_type'] == 'numeric':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return 0
            else:
                return None
    elif col_type[key]['data_type'] == 'timestamp without time zone':
        if value is not None:
            return str(value)
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return ""
            else:
                return None
    elif col_type[key]['data_type'] == 'bytea':
        if value is not None:
            return str(value)
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return ""
            else:
                return None
    else:
        print("Cannot find mapping for type : " + col_type[key]['data_type'])
        return None


def disable_triggers(schema_name, table_name):
    cur = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
    sql = "ALTER TABLE %s.%s DISABLE TRIGGER ALL;" % (schema_name, table_name)
    cur.execute(sql)
    cur.close()


def enable_triggers(schema_name, table_name):
    cur = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
    sql = "ALTER TABLE %s.%s ENABLE TRIGGER ALL;" % (schema_name, table_name)
    cur.execute(sql)
    cur.close()


def main():
    parser = argparse.ArgumentParser(description="Script for migrating data")
    parser.add_argument('-s', help="Source schema name", required=True)
    parser.add_argument('-t', help="Source table name", required=True)
    parser.add_argument('-ds', help="Destination schema name", required=True)
    parser.add_argument('-dt', help="Destination schema name", required=True)
    arguments = parser.parse_args()
    disable_triggers(arguments.ds, arguments.dt)
    migrate(arguments.ds, arguments.dt, arguments.s, arguments.t)
    enable_triggers(arguments.ds, arguments.dt)


if __name__ == "__main__":
    try:
        main()
        exit(0)
    except Exception as ex:
        traceback.print_exc(ex)
        exit(1)
    finally:
        destination.close()
        source.close()
        if tunnel.is_alive:
            tunnel.stop()
