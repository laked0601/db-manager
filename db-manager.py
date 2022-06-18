from json import loads as json_loads, dumps as json_dumps
from sqlite3 import connect, IntegrityError

def flatten_json_tree(json_tree):
    paths = [['', json_tree]]
    rtrndict = {}
    while len(paths) != 0:
        rtrnpaths = []
        for path, json_obj in paths:
            for key, value in json_obj.items():
                if isinstance(value, dict):
                    rtrnpaths.append([path + '_' + key, value])
                else:
                    rtrndict[path + '_' + key] = value
        paths = rtrnpaths
    return rtrndict

def analyse_json(fpath):
    def get_file_list(fpath):
        with open(fpath,'r') as rf:
            flist = rf.read().split('\n')
        while len(flist) != 0:
            if flist[-1] == '':
                flist.pop(-1)
            else:
                break
        for i in range(len(flist)):
            flist[i] = json_loads(flist[i])
        return flist
    def flatten_json_tree(json_tree):
        paths = [['', json_tree]]
        rtrndict = {}
        while len(paths) != 0:
            rtrnpaths = []
            for path, json_obj in paths:
                for key, value in json_obj.items():
                    if isinstance(value, dict):
                        rtrnpaths.append([path + '_' + key, value])
                    else:
                        rtrndict[path + '_' + key] = value
            paths = rtrnpaths
        return rtrndict
    def make_headers(list_of_json_objects):
        headers_dict = {}
        flattened_list = []
        for line in list_of_json_objects:
            flattened_line = flatten_json_tree(line)
            for key, value in flattened_line.items():
                if key not in headers_dict:
                    headers_dict[key] = [
                        [lambda x: isinstance(x, bool) or isinstance(x, int), 0],
                        [lambda x: isinstance(x, float), 1],
                        [lambda x: isinstance(x, list) and isinstance(value[0],dict), 2],
                        [lambda x: isinstance(x, list) and isinstance(value[0],str), 3],
                        [lambda x: isinstance(x, list) and (isinstance(value[0],int) or isinstance(value[0],bool)), 4],
                        [lambda x: isinstance(x, list) and isinstance(value[0],float), 5]
                    ]
                if len(headers_dict[key]) != 0:
                    remaining = []
                    for value_test in headers_dict[key]:
                        if (value_test[0])(value):
                            remaining.append(value_test)
                    headers_dict[key] = remaining
            flattened_list.append(flattened_line)
        for key, value in headers_dict.items():
            if value == []:
                headers_dict[key] = "TEXT"
            elif value[0][1] == 0:
                headers_dict[key] = "INTEGER"
            elif value[0][1] == 1:
                headers_dict[key] = "REAL"
            elif value[0][1] == 2:
                headers_dict[key] = "TABLE-LINK-DICT"
            elif value[0][1] == 3:
                headers_dict[key] = "TABLE-LINK-STR"
            elif value[0][1] == 4:
                headers_dict[key] = "TABLE-LINK-INT"
            elif value[0][1] == 5:
                headers_dict[key] = "TABLE-LINK-FLOAT"
        return headers_dict, flattened_list
    def make_jsonobj_list(flattened_list, key):
        rtrnlist = []
        for row in flattened_list:
            if key in row:
                for value in row[key]:
                    rtrnlist.append(value)
        return rtrnlist
    file_list = get_file_list(fpath)
    completed_tables = {}
    headers_dict, flattened_list = make_headers(file_list)
    tables_in_progress = [[completed_tables, headers_dict, flattened_list]]
    del file_list
    while len(tables_in_progress) != 0:
        rtrntables = []
        for completed_reference, headers_dict, flattened_list in tables_in_progress:
            rem_keys = []
            for key, value in headers_dict.items():
                if value == "TABLE-LINK-DICT":
                    completed_reference[key] = {}
                    new_headers, new_list = make_headers(make_jsonobj_list(flattened_list, key))
                    rtrntables.append([completed_reference[key], new_headers, new_list])
                elif value == "TABLE-LINK-STR":
                    completed_reference[key] = {key: "TEXT"}
                elif value == "TABLE-LINK-INT":
                    completed_reference[key] = {key: "INTEGER"}
                elif value == "TABLE-LINK-FLOAT":
                    completed_reference[key] = {key: "REAL"}
                else:
                    completed_reference[key] = value
        tables_in_progress = rtrntables
    return completed_tables
def convert_json_to_sqlite_table(json_obj):
    statement = "CREATE TABLE json_structure (\n\t%s\n);\n" % (',\n\t'.join(["json_structure_pk INTEGER PRIMARY KEY AUTOINCREMENT", "table_name TEXT", "json_structure TEXT"]),)
    statement += "CREATE TABLE table_0 (\n\t%s\n);\n" % (',\n\t'.join(["table_0_pk INTEGER PRIMARY KEY AUTOINCREMENT"] + [key + " " + value for key, value in json_obj.items() if not isinstance(value,dict)]))
    for key, value in json_obj.items():
        if isinstance(value,dict):
            statement += "CREATE TABLE %s (\n\t%s\n);\n" % (key, ',\n\t'.join([f"{key}_pk INTEGER PRIMARY KEY AUTOINCREMENT,\n\t£FOREIGN_KEY£ INTEGER"] + [k + " " + v for k, v in value.items() if not isinstance(v,dict)] + ["FOREIGN KEY(£FOREIGN_KEY£) REFERENCES table_0(table_0_pk)"]))
    return statement
def upload_json_to_sql(fpath, master_table_name, sqlite_connection):
    def get_sql_headers():
        CRSR.execute("SELECT json_structure FROM json_structure WHERE table_name = ?", (master_table_name,))
        res = CRSR.fetchone()
        if res is None:
            raise Exception("Could not find headers in sqlite instance of json_structure")
        else:
            return json_loads(res[0])
    def insert_json(json_object):
        try:
            args = []
            for key in insert_statements[master_table_name]["keys"]:
                if key not in json_object:
                    args.append(None)
                else:
                    args.append(json_object[key])
            CRSR.execute(insert_statements[master_table_name]["statement"], tuple(args))
            insert_row = CRSR.lastrowid
            for tname in [x for x in insert_statements if x != master_table_name]:
                if tname in json_object:
                    if isinstance(json_object[tname][0], dict):
                        for dictobj in json_object[tname]:
                            dictobj = flatten_json_tree(dictobj)
                            args = [insert_row]
                            for key in insert_statements[tname]["keys"]:
                                if key not in dictobj:
                                    args.append(None)
                                else:
                                    args.append(dictobj[key])
                            CRSR.execute(insert_statements[tname]["statement"], tuple(args))
                    else:
                        for obj in json_object[tname]:
                            args = [insert_row, obj]
                            CRSR.execute(insert_statements[tname]["statement"], tuple(args))
        except IntegrityError:
            return

    def make_insert_statements():
        insert_statements = {}
        insert_statements_path = [[master_table_name, headers]]
        while len(insert_statements_path) != 0:
            rtrnpaths = []
            for path in insert_statements_path:
                keys = []
                for key, value in path[1].items():
                    if isinstance(value,dict):
                        rtrnpaths.append([key, value])
                    else:
                        keys.append(key)
                insert_statements[path[0]] = {"statement":None, "keys": keys}
            insert_statements_path = rtrnpaths
        for key, value in insert_statements.items():
            if key == master_table_name:
                value["statement"] = "INSERT INTO %s (%s) VALUES (%s);" % (key, ','.join(value["keys"]), ','.join(['?' for _ in range(len(value["keys"]))]))
            else:
                value["statement"] = "INSERT INTO %s (%s) VALUES (%s);" % (key, ','.join(["�FOREIGN_KEY�"] + value["keys"]), ','.join(['?' for _ in range(len(value["keys"]) + 1)]))
        return insert_statements
    CRSR = sqlite_connection.cursor()
    headers = get_sql_headers()
    insert_statements = make_insert_statements()
    with open(fpath,'r') as f:
        for x, line in enumerate(f):
            if line == '':
                break
            insert_json(flatten_json_tree(json_loads(line)))
            if x % 3000 == 0:
                sqlite_connection.commit()
        sqlite_connection.commit()
