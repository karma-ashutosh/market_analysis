import json


def overwrite_or_merge(target_dict: dict, source_dict: dict, path=None):
    "merges b into a"
    if path is None:
        path = []

    for key in source_dict:
        if key in target_dict:
            if isinstance(target_dict[key], dict) and isinstance(source_dict[key], dict):
                overwrite_or_merge(target_dict[key], source_dict[key], path + [str(key)])
            else:
                target_dict[key] = source_dict[key]  # overwriting value in a by value in b
        else:
            target_dict[key] = source_dict[key]
    return target_dict


def save_csv_and_json_output(values, path):
    json_arr_to_csv(values, path + ".csv")
    with open(path + ".json", 'w') as handle:
        json.dump(values, handle, indent=1)


def get_all_values_for_key(j_arr: list, key_name: str) -> list:
    return list(map(lambda entry: entry[key_name], j_arr))


def json_file_to_csv(json_path, csv_path):
    with open(json_path) as handle:
        json_array = json.load(handle)
    json_arr_to_csv(json_array, csv_path)


def json_arr_to_csv(json_array, csv_path, seperator=","):
    # making exhaustive list of column names
    column_names = set()
    for j_element in json_array:
        j_element = flatten(j_element)
        for key in j_element:
            column_names.add(key)
    column_names = list(sorted(column_names))

    f = open(csv_path, 'w')
    f.write(seperator.join(column_names) + "\n")
    for j_element in json_array:
        j_element = flatten(j_element)
        vals = [str(j_element.get(key, "null")) for key in column_names]
        line = seperator.join(vals) + "\n"
        f.write(line)
    f.flush()
    f.close()


def flatten(j_elem: dict) -> dict:
    result = {}
    for key in j_elem.keys():
        if type(j_elem[key]) == dict:
            child_flattened = flatten(j_elem[key])
            for child_key in child_flattened.keys():
                result["{}_{}".format(key, child_key)] = child_flattened[child_key]
        else:
            result[key] = j_elem[key]
    return result


def csv_to_json(rows, keys):
    j_arr = []
    for entry in rows:
        j_elem = {}
        for index in range(len(entry)):  # iterate over each column of the row
            if entry[index]:  # verifying if the entry is not None or empty strings
                j_elem[keys[index]] = entry[index]
        j_arr.append(j_elem)
    return j_arr


def read_and_clean_lines(file_path) -> iter:
    return map(lambda line: line.strip().replace('"', '').split(","), open(file_path).readlines())


def csv_file_with_headers_to_json_arr(file_path) -> list:
    print("loading data from file: {}".format(file_path))
    lines = list(read_and_clean_lines(file_path))
    header = lines[0]
    rows = lines[1:]

    def row_to_dict(row):
        d = {}
        for index in range(len(header)):
            d[header[index]] = row[index]
        return d

    result = list(map(row_to_dict, rows))
    print("data loading completed")
    return result
