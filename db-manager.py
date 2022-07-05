from json import loads as json_loads, dumps as json_dumps
from sqlite3 import connect, IntegrityError
import csv
from re import compile as re_compile


def flatten_json_tree(json_tree):
    paths = [[[], json_tree]]
    rtrndict = {}
    while len(paths) != 0:
        rtrnpaths = []
        for path, json_obj in paths:
            for key, value in json_obj.items():
                if isinstance(value, dict):
                    rtrnpaths.append([path + [key], value])
                else:
                    rtrndict['_'.join(path + [key])] = value
        paths = rtrnpaths
    return rtrndict

def analyse_csv(fpath, master_table_name, delimiter=",", quotechar='"'):
    def find_next_row(reader):
        row = reader.__next__()
        while row == []:
            row = reader.__next__()
        return row
    types_dict = {
        0: "INTEGER",
        1: "REAL",
        2: "TABLE-LINK-DICT",
        3: "TABLE-LINK-STR",
        4: "TABLE-LINK-INT",
        5: "TABLE-LINK-FLOAT"
    }
    with open(fpath,'r',encoding="UTF-8") as rf:
        float_re = re_compile("^-?[1-9][\d,]*\.?\d*$|^-?0\.?\d*$")
        int_re = re_compile("^-?[1-9][\d,]*$|^-?0$")
        reader = csv.reader(rf,delimiter=',',quotechar='"')
        headers = [master_table_name + '_' + header for header in find_next_row(reader)]
        first_row = find_next_row(reader)
        headers_tests = [[
                    [lambda x: float_re.search(x) is not None, 1],
                    [lambda x: int_re.search(x), 0]
                ] for _ in headers]
        for i, val in enumerate(first_row):
            if len(headers_tests[i]) != 0:  # otherwise ignore as value is TEXT
                for test_obj in headers_tests[i]:
                    if not (test_obj[0])(val):
                        headers_tests[i].remove(test_obj)
        for row in reader:
            if row == []:
                break
            for i, val in enumerate(row):
                if len(headers_tests[i]) != 0: # otherwise ignore as value is TEXT
                    for test_obj in headers_tests[i]:
                        if not (test_obj[0])(val):
                            headers_tests[i].remove(test_obj)
        rtrndict = {}
        for head_name, test_result in zip(headers, headers_tests):
            head_name = head_name.replace(' ','_')
            if len(test_result) == 0:
                rtrndict[head_name] = "TEXT"
            else:
                rtrndict[head_name] = types_dict[test_result[0][1]]
        return rtrndict
def analyse_json(fpath, master_table_name):
    def analyse_line(json_line, headers_dict):
        flattened_line = flatten_json_tree(json_line)
        for key, value in flattened_line.items():
            if key not in headers_dict:
                headers_dict[key] = [
                    [lambda x: isinstance(x, bool) or isinstance(x, int), 0],
                    [lambda x: isinstance(x, float), 1],
                    [lambda x: isinstance(x, list) and isinstance(x[0], dict), 2],
                    [lambda x: isinstance(x, list) and isinstance(x[0], str), 3],
                    [lambda x: isinstance(x, list) and (isinstance(x[0], int) or isinstance(value[0], bool)), 4],
                    [lambda x: isinstance(x, list) and isinstance(x[0], float), 5]
                ]
            if value is None or (isinstance(value, list) and len(value) == 0):
                pass
            elif len(headers_dict[key]) != 0:
                remaining = []
                for value_test in headers_dict[key]:
                    if (value_test[0])(value):
                        remaining.append(value_test)
                headers_dict[key] = remaining
        return headers_dict
    def format_headers_dict(headers_dict):
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
        return headers_dict
    completed_table_structure = {}
    with open(fpath, 'r') as rf:
        load_structure = [[[], {}, completed_table_structure]] # [path to json element, headers dict]
        while len(load_structure) != 0:
            rtrn_structure = []
            for json_path, headers_dict, completed_structure in load_structure:
                path_len = len(json_path)
                print(json_path)
                for line in rf:
                    if line == '':
                        break
                    try:
                        json_element = json_loads(line)
                    except:
                        with open("error.txt", 'a') as wf:
                            wf.write(line + '\n')
                        continue
                    element_queue = [json_element]
                    x = 0
                    while x < path_len:
                        rtrn_queue = []
                        for json_ele in element_queue:
                            flattened_ele = flatten_json_tree(json_ele)
                            if json_path[x] in flattened_ele:
                                if isinstance(flattened_ele[json_path[x]], list):
                                    for dict_obj in flattened_ele[json_path[x]]:
                                        rtrn_queue.append(dict_obj)
                                else:
                                    rtrn_queue.append(flattened_ele[json_path[x]])
                        element_queue = rtrn_queue
                        x += 1
                    for json_ele in element_queue:
                        headers_dict = analyse_line(json_ele, headers_dict)
                rf.seek(0)
                headers_dict = format_headers_dict(headers_dict)
                for key, value in headers_dict.items():
                    if value == "TABLE-LINK-DICT":
                        completed_structure[key] = {}
                        rtrn_structure.append([json_path + [key], {}, completed_structure[key]])
                    elif value == "TABLE-LINK-STR":
                        completed_structure[key] = {key: "TEXT"}
                    elif value == "TABLE-LINK-INT":
                        completed_structure[key] = {key: "INTEGER"}
                    elif value == "TABLE-LINK-FLOAT":
                        completed_structure[key] = {key: "REAL"}
                    else:
                        completed_structure[key] = value
                print("Finished a table...")
            load_structure = rtrn_structure
    return completed_table_structure
def convert_json_to_sqlite_table(json_obj,master_table_name,include_log_and_json_structure=True):
    statement = ''
    if include_log_and_json_structure:
        statement += "CREATE TABLE json_structure (\n\t%s\n);\n" % (',\n\t'.join(
            ["json_structure_pk INTEGER PRIMARY KEY AUTOINCREMENT", "table_name TEXT", "json_structure TEXT"]),)
        statement += "CREATE TABLE insert_log (\n\t%s\n);\n" % (',\n\t'.join(
            ["insert_log_pk INTEGER PRIMARY KEY AUTOINCREMENT", "table_name TEXT", "row_number INTEGER NOT NULL",
             "creation_time TEXT"]),)
    statement += "CREATE TABLE %s (\n\t%s\n);\n" % (master_table_name, ',\n\t'.join([f"{master_table_name}_pk INTEGER PRIMARY KEY AUTOINCREMENT"] + [key + " " + value for key, value in json_obj.items() if not isinstance(value,dict)]))
    statement += f"CREATE TRIGGER {master_table_name}_insert AFTER INSERT ON {master_table_name}\n\tBEGIN\n\t\tINSERT INTO insert_log (table_name, row_number, creation_time)\n\t\tVALUES ('{master_table_name}', NEW.{master_table_name}_pk, DateTime('now'));\n\tEND;\n"
    statement += f"INSERT INTO json_structure (table_name, json_structure) VALUES ('{master_table_name}', '{json_dumps(json_obj)}');\n"
    sub_tables = [[master_table_name, master_table_name + '_' + key.replace('-','_'), value] for key, value in json_obj.items() if isinstance(value, dict)]
    while len(sub_tables) != 0:
        rtrntables = []
        for previous_table_name, table_name_path, json_obj in sub_tables:
            table_columns = [table_name_path + "_pk INTEGER PRIMARY KEY AUTOINCREMENT", "£FOREIGN_KEY£ INTEGER"]
            for key, value in json_obj.items():
                if isinstance(value,dict):
                    rtrntables.append([table_name_path, table_name_path + '_' + key.replace('-','_'), value])
                else:
                    table_columns.append("%s %s" % (key.replace('-','_'), value))
            table_columns.append(f"FOREIGN KEY(£FOREIGN_KEY£) REFERENCES {previous_table_name}({previous_table_name}_pk)")
            statement += "CREATE TABLE %s (\n\t%s\n);\n" % (table_name_path, ',\n\t'.join(table_columns),)
        sub_tables = rtrntables
    return statement
def convert_json_to_sqlite_insert(json_structure,master_table_name):
    queries = [[json_structure, [master_table_name]]]
    end_structure = {}
    while len(queries) != 0:
        rtrnqueries = []
        for json_obj, tname in queries:
            non_dict_obj = []
            for key, value in json_obj.items():
                if isinstance(value, dict):
                    rtrnqueries.append([value, tname + [key]])
                else:
                    non_dict_obj.append(key)
            tname_str = '_'.join(tname)
            if tname_str == master_table_name:
                end_structure[tname_str] = {
                    "statement": "INSERT INTO %s (%s) VALUES (%s);" % (
                    tname_str, ','.join([x.replace('-','_') for x in non_dict_obj]), ','.join(['?' for _ in non_dict_obj])),
                    "columns": non_dict_obj
                }
            else:
                end_structure[tname_str]  = {
                    "statement": "INSERT INTO %s (%s) VALUES (%s);" % (tname_str, ','.join(["£FOREIGN_KEY£"] + [x.replace('-','_') for x in non_dict_obj]), ','.join(['?'] + ['?' for _ in non_dict_obj])),
                    "columns": non_dict_obj
                }
        queries = rtrnqueries
    return end_structure


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
def upload_file_to_sql(fpath, master_table_name, sqlite_connection, headers, extn=".csv", delimiter = ',', quotechar = '"'):
    def get_sql_headers():
        CRSR.execute("SELECT json_structure FROM json_structure WHERE table_name = ?", (master_table_name,))
        res = CRSR.fetchone()
        if res is None:
            raise Exception("Could not find headers in sqlite instance of json_structure")
        else:
            return json_loads(res[0])
    def insert_json(json_object):
        try:
            tables_to_insert = [[json_object, master_table_name, '']]
            while len(tables_to_insert) != 0:
                rtrnstatements = []
                for json_obj, tname, prev_table_name in tables_to_insert:
                    flattened_object = flatten_json_tree(json_obj)
                    # print(insert_statements[tname]["columns"])
                    columns_template = [None for _ in insert_statements[tname]["columns"]]
                    for x, key in enumerate(insert_statements[tname]["columns"]):
                        if key in flattened_object:
                            if isinstance(flattened_object[key], list):
                                columns_template[x] = str(columns_template[x])
                            else:
                                columns_template[x] = flattened_object[key]
                    if tname != master_table_name:
                        # print(insert_statements[tname]["statement"])
                        # print(len(columns_template), insert_statements[tname]["statement"].count('?'))
                        try:
                            CRSR.execute(insert_statements[tname]["statement"], tuple([insert_statements[prev_table_name]["primary"]] + columns_template))
                        except:
                            print(tname)
                            for x, y in zip(columns_template, insert_statements[tname]["columns"]):
                                print(y, type(x), x)
                            raise Exception
                    else:
                        CRSR.execute(insert_statements[tname]["statement"], tuple(columns_template))
                    insert_statements[tname]["primary"] += 1
                    for key, value in flattened_object.items():
                        if isinstance(value, list) and len(value) != 0:
                            if isinstance(value[0], dict):
                                for ele in value:
                                    rtrnstatements.append([ele, tname + '_' + key, tname])
                            elif tname + '_' + key in insert_statements:
                                querstr = insert_statements[tname + '_' + key]["statement"]
                                for ele in value:
                                    CRSR.execute(querstr, (insert_statements[tname]["primary"], ele, ))
                tables_to_insert = rtrnstatements
        except IntegrityError:
            return
    def json_file_type(fileobj):
        for x, line in enumerate(fileobj):
            if line == '':
                break
            try:
                ln = json_loads(line)
            except:
                continue
            insert_json(flatten_json_tree(ln))
            if x % 3000 == 0:
                # break
                sqlite_connection.commit()
        sqlite_connection.commit()
    def csv_file_type(fileobj):
        def find_next_row(reader):
            row = reader.__next__()
            while row == []:
                row = reader.__next__()
            return row
        reader = csv.reader(rf, delimiter=delimiter, quotechar=quotechar)
        indexes = {key: pos for pos, key in enumerate(insert_statements[master_table_name]["keys"])}
        headers = find_next_row(reader)
        order = [indexes[head.replace(' ','_')] for head in headers]
        first_row = find_next_row(reader)
        # print(insert_statements[master_table_name]["statement"])
        # print([first_row[x] for x in order])
        CRSR.execute(insert_statements[master_table_name]["statement"], [first_row[x] for x in order])
        for y, row in enumerate(reader):
            if row == []:
                break
            CRSR.execute(insert_statements[master_table_name]["statement"], [row[x] for x in order])
            if y % 3000 == 0:
                sqlite_connection.commit()
        sqlite_connection.commit()
    CRSR = sqlite_connection.cursor()
    # headers = get_sql_headers()
    insert_statements = convert_json_to_sqlite_insert(headers, master_table_name)
    for tname in insert_statements:
        CRSR.execute(f"SELECT MAX({tname}_pk) FROM {tname}")
        res = CRSR.fetchone()
        # print(res)
        if res[0] is None:
            insert_statements[tname]["primary"] = 0
        else:
            insert_statements[tname]["primary"] = res[0]
    with open(fpath,'r') as rf:
        if extn == ".json":
            json_file_type(rf)
        elif extn == ".csv":
            csv_file_type(rf)
