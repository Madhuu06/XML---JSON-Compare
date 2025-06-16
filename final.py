import xml.etree.ElementTree as ET
import csv
import os
import mysql.connector
import json

XML_FOLDER = r"C:\Users\madhu\OneDrive\Desktop\Karthik\litmus\Crap"
EXCLUDED_ATTRIBUTES_FILE = "excluded_attributes.csv"
INPUT_CSV_CASE1 = "input.csv"
ORDER_PAIR_CSV = "orders_to_compare.csv"
DB_CONFIG_XML = dict(host="localhost", user="root", password="madhu2005", database="xml")
DB_CONFIG_JSON = dict(host="localhost", user="root", password="madhu2005", database="json")
INPUT_CSV_JSON = "input_json.csv"
ORDER_PAIR_JSON = "orders_to_compare_json.csv" 
DEBUG = False

def strip_ns(tag):
    """Remove namespace from XML tag"""
    return tag.split('}', 1)[-1] if '}' in tag else tag

TAG_MAPPING = {}
ATTR_MAPPING = {}
IGNORE_TAGS = {
    "ApplicationArea", "Process", "ActionCriteria", "ActionExpression",
}

def canonical_tag(local):
    """Return canonical form of tag, fallback to original"""
    return TAG_MAPPING.get(local, local)

def canonical_attr(local):
    """Return canonical form of attribute, fallback to original"""
    result = ATTR_MAPPING.get(local, local)
    if DEBUG:
        print(f"Canonical attribute for '{local}': '{result}'")
    return result

def flatten_elements(root: ET.Element) -> dict:
    """
    Flatten an XML element tree into dictionary of paths mapping to attribute/text data
    """
    elements = {}

    def recurse(elem: ET.Element, path="", sib_counter=None):
        if sib_counter is None:
            sib_counter = {}

        local = strip_ns(elem.tag)
        canon = canonical_tag(local)
        if canon in IGNORE_TAGS:
            if DEBUG:
                print(f"Ignoring tag: {canon}")
            return

        attribs = {canonical_attr(strip_ns(k)): v for k, v in elem.attrib.items()}
        name_attr = attribs.get("name")

        if canon in {"ProtocolData", "UserDataField"} and name_attr:
            new_path = f"{path}/{canon}[@name='{name_attr}']"
        else:
            idx = sib_counter.get(canon, 0) + 1
            sib_counter[canon] = idx
            new_path = f"{path}/{canon}[{idx}]" if path else f"/{canon}[{idx}]"

        elements[new_path] = {
            "attrib": attribs,
            "text": (elem.text or "").strip(),
            "tag": canon  # Store the canonical tag name for reference
        }

        child_counts = {}
        for child in elem:
            recurse(child, new_path, child_counts)

    recurse(root)
    if DEBUG:
        print("Flattened elements (sample):")
        for i, (k,v) in enumerate(elements.items()):
            print(f"{k}: {v}")
            if i>=5:
                break
    return elements

def flatten_json(obj, path=""):
    """Flatten nested JSON into dictionary of paths to values"""
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_path = f"{path}.{k}" if path else k
            items.update(flatten_json(v, new_path))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            new_path = f"{path}[{i}]"
            items.update(flatten_json(v, new_path))
    else:
        items[path] = str(obj)
    return items

def compare_json_files(json1, json2, excluded_attrs=None):
    """Compare two JSON objects returning list of differences, respecting excluded attributes"""
    if excluded_attrs is None:
        excluded_attrs = set()

    flat1 = flatten_json(json1)
    flat2 = flatten_json(json2)
    diffs = []

    keys = set(flat1.keys()).union(set(flat2.keys()))
    for key in keys:
        # Check if this key should be excluded
        should_exclude = False
        
        # Check for exact match first
        if key in excluded_attrs:
            should_exclude = True
        else:
            # Check for pattern matches by normalizing array notation
            normalized_key = key
            # Convert Items[1].ProductID to Items.ProductID for comparison
            import re
            normalized_key = re.sub(r'\[\d+\]', '', key)
            
            if normalized_key in excluded_attrs:
                should_exclude = True
        
        if should_exclude:
            if DEBUG:
                print(f"Excluding JSON key '{key}' from comparison")
            continue
        val1 = flat1.get(key, "-")
        val2 = flat2.get(key, "-")
        if val1 != val2:
            if key in flat1 and key in flat2:
                diff_type = "Value mismatch"
            else:
                diff_type = "Missing key"
            diffs.append((key, diff_type, val1, val2))
    return diffs

def load_excluded_attributes(csv_path=EXCLUDED_ATTRIBUTES_FILE):
    """Load set of excluded attributes from CSV file"""
    excluded = set()
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                # Handle both 'attribute' and 'Attribute' column names
                attr_key = 'attribute' if 'attribute' in row else 'Attribute'
                if attr_key in row and row[attr_key].strip():
                    attr = canonical_attr(row[attr_key].strip())
                    excluded.add(attr)
                    if DEBUG:
                        print(f"Excluded attribute loaded: '{attr}'")
    except FileNotFoundError:
        print(f"  Attribute-exclusion file not found: {csv_path}")
    except Exception as e:
        print(f"  Error loading excluded attributes: {e}")
    
    print(f"Loaded {len(excluded)} excluded attributes: {excluded}")
    return excluded

def should_exclude_element(elem_data, excluded_attrs):
    """Check if an element should be excluded based on its tag name or attributes"""
    # Check if the tag itself is in excluded attributes
    tag_name = elem_data.get("tag", "")
    if tag_name in excluded_attrs:
        return True
    
    # Check if element has a 'name' attribute that's in excluded list
    name_attr = elem_data.get("attrib", {}).get("name", "")
    if name_attr in excluded_attrs:
        return True
    
    return False

def compare_dicts(wcs_dict: dict, micro_dict: dict, excluded_attrs: set):
    """Compare two flattened XML dicts, ignoring excluded attributes"""
    diffs = []

    # Filter out excluded elements from both dictionaries
    filtered_wcs = {k: v for k, v in wcs_dict.items() if not should_exclude_element(v, excluded_attrs)}
    filtered_micro = {k: v for k, v in micro_dict.items() if not should_exclude_element(v, excluded_attrs)}

    if DEBUG:
        print(f"Original WCS elements: {len(wcs_dict)}, Filtered: {len(filtered_wcs)}")
        print(f"Original Micro elements: {len(micro_dict)}, Filtered: {len(filtered_micro)}")

    for path, wcs_elem in filtered_wcs.items():
        if path not in filtered_micro:
            name_hint = wcs_elem["attrib"].get("name", path.split("/")[-1])
            # Double-check exclusion at difference level
            if name_hint not in excluded_attrs and wcs_elem.get("tag", "") not in excluded_attrs:
                diffs.append((name_hint, "Tag missing", wcs_elem["text"], "-"))
            continue

        mic_elem = filtered_micro[path]
        
        # Compare attributes
        for attr, wcs_val in wcs_elem["attrib"].items():
            if attr in excluded_attrs:
                if DEBUG:
                    print(f"Excluding attribute '{attr}' from comparison")
                continue
            mic_val = mic_elem["attrib"].get(attr)
            if mic_val is None:
                diffs.append((attr, "Attribute missing", wcs_val, "-"))
            elif mic_val != wcs_val:
                diffs.append((attr, "Attribute mismatch", wcs_val, mic_val))
        
        # Compare text content
        if wcs_elem["text"] != mic_elem["text"]:
            diffs.append(("(text)", "Text mismatch", wcs_elem["text"], mic_elem["text"]))

    # Check for extra elements in micro
    for path, mic_elem in filtered_micro.items():
        if path not in filtered_wcs:
            name_hint = mic_elem["attrib"].get("name", path.split("/")[-1])
            # Double-check exclusion at difference level
            if name_hint not in excluded_attrs and mic_elem.get("tag", "") not in excluded_attrs:
                diffs.append((name_hint, "Extra tag", "-", mic_elem["text"]))

    return diffs

def process_case1(input_csv, output_csv):
    """
    Case 1: Compare XML files from filesystem pairs listed in CSV
    CSV columns must include 'wcs_file' and 'micro_file' without extensions
    """
    excluded = load_excluded_attributes()
    all_diffs = []

    with open(input_csv, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        required_fields = {"wcs_file", "micro_file"}
        if not required_fields.issubset(set(rdr.fieldnames)):
            raise ValueError(f"CSV must have columns: {required_fields}")
        for row in rdr:
            wcs_path = os.path.join(XML_FOLDER, row["wcs_file"] + ".xml")
            mic_path = os.path.join(XML_FOLDER, row["micro_file"] + ".xml")

            print(f"\n🔍 Comparing {row['wcs_file']} ↔ {row['micro_file']}")

            try:
                wcs_root = ET.parse(wcs_path).getroot()
                mic_root = ET.parse(mic_path).getroot()
            except ET.ParseError as e:
                print(" XML parse error:", e)
                continue
            except FileNotFoundError as e:
                print(" File not found error:", e)
                continue

            wcs_dict = flatten_elements(wcs_root)
            mic_dict = flatten_elements(mic_root)
            all_diffs.extend(compare_dicts(wcs_dict, mic_dict, excluded))

    write_csv(all_diffs, output_csv)

def fetch_xml_by_id(conn, order_id):
    """Fetch XML content string by order_id from DB"""
    cur = conn.cursor()
    cur.execute("SELECT xml_content FROM orders WHERE order_id=%s", (order_id,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None

def fetch_json(conn, order_id):
    """Fetch JSON content string or dict by order_id from DB"""
    cur = conn.cursor()
    cur.execute("SELECT json_content FROM orders WHERE order_id=%s", (order_id,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None

def process_case2(output_csv):
    """
    Case 2: Compare XML content stored in DB for pairs specified in orders_to_compare.csv
    CSV columns: wcs_order_id, micro_order_id
    """
    excluded = load_excluded_attributes()
    conn_xml = mysql.connector.connect(**DB_CONFIG_XML)

    with open(ORDER_PAIR_CSV, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        required_fields = {"wcs_order_id", "micro_order_id"}
        if not required_fields.issubset(set(rdr.fieldnames)):
            raise ValueError(f"CSV must have columns: {required_fields}")
        pairs = [(r["wcs_order_id"], r["micro_order_id"]) for r in rdr]

    all_diffs = []
    for wcs_id, mic_id in pairs:
        print(f"\n Comparing DB order {wcs_id} ↔ {mic_id}")
        wcs_xml = fetch_xml_by_id(conn_xml, wcs_id)
        mic_xml = fetch_xml_by_id(conn_xml, mic_id)
        if not wcs_xml or not mic_xml:
            print(f" Missing XML for pair {wcs_id}, {mic_id}; skipping")
            continue
        try:
            wcs_root = ET.fromstring(wcs_xml)
            mic_root = ET.fromstring(mic_xml)
        except ET.ParseError as e:
            print(" Parse error:", e)
            continue

        wcs_dict = flatten_elements(wcs_root)
        mic_dict = flatten_elements(mic_root)
        all_diffs.extend(compare_dicts(wcs_dict, mic_dict, excluded))

    conn_xml.close()
    write_csv(all_diffs, output_csv)

def process_case3(input_csv, output_csv):
    """
    Case 3: Compare JSON files from filesystem pairs listed in CSV
    CSV columns must include 'wcs_json' and 'micro_json' with file names including extensions
    """
    all_diffs = []
    excluded = load_excluded_attributes()

    with open(input_csv, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        required_fields = {"wcs_json", "micro_json"}
        if not required_fields.issubset(set(rdr.fieldnames)):
            raise ValueError(f"CSV must have columns: {required_fields}")
        for row in rdr:
            wcs_path = os.path.join(XML_FOLDER, row["wcs_json"])
            mic_path = os.path.join(XML_FOLDER, row["micro_json"])
            print(f"\n Comparing {row['wcs_json']} ↔ {row['micro_json']}")

            try:
                with open(wcs_path, "r", encoding="utf-8") as f1, open(mic_path, "r", encoding="utf-8") as f2:
                    json1 = json.load(f1)
                    json2 = json.load(f2)
            except Exception as e:
                print(" JSON parse error:", e)
                continue
            all_diffs.extend(compare_json_files(json1, json2, excluded))
    write_csv(all_diffs, output_csv)

def process_case4(out="all_differences_case4.csv", pair_csv=ORDER_PAIR_JSON):
    """
    Case 4: Compare JSON content from DB for pairs specified in orders_to_compare_json.csv
    CSV columns: wcs_order_id, micro_order_id
    """
    excluded = load_excluded_attributes()
    conn_json = mysql.connector.connect(**DB_CONFIG_JSON)
    diffs = []
    with open(pair_csv, encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        required_fields = {"wcs_order_id", "micro_order_id"}
        if not required_fields.issubset(set(rdr.fieldnames)):
            raise ValueError(f"CSV must have columns: {required_fields}")
        pairs = [(r["wcs_order_id"], r["micro_order_id"]) for r in rdr]

    for wcs_id, mic_id in pairs:
        print(f"\n DB-JSON {wcs_id} ↔ {mic_id}")
        j1 = fetch_json(conn_json, wcs_id)
        j2 = fetch_json(conn_json, mic_id)
        if not j1 or not j2:
            print(" missing JSON – skipped")
            continue
        try:
            # MySQL may return dict or str depending on driver
            if isinstance(j1, str):
                j1 = json.loads(j1)
            if isinstance(j2, str):
                j2 = json.loads(j2)
        except json.JSONDecodeError as e:
            print(" decode error:", e)
            continue
        diffs += compare_json_files(j1, j2, excluded)
    conn_json.close()
    write_csv(diffs, out)

def write_csv(rows, out_path):
    """Write differences to CSV file"""
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["attribute", "difference type", "wcs value", "microservice value"])
        w.writerows(rows)
    print(f"\n {len(rows)} difference rows written ➜ {out_path}")

def main():
    print("Select source:\n 1 – File System\n 2 – Database")
    source_choice = input("Enter 1 or 2: ").strip()

    if source_choice not in ("1", "2"):
        print("Invalid source selection. Exiting.")
        return

    print("\nSelect data format:\n 1 – XML\n 2 – JSON")
    format_choice = input("Enter 1 or 2: ").strip()

    if format_choice not in ("1", "2"):
        print("Invalid format selection. Exiting.")
        return

    if source_choice == "1" and format_choice == "1":
        process_case1(INPUT_CSV_CASE1, "all_differences_case1.csv")  
    elif source_choice == "2" and format_choice == "1":
        process_case2("all_differences_case2.csv")                   
    elif source_choice == "1" and format_choice == "2":
        process_case3(INPUT_CSV_JSON, "all_differences_case3.csv")   
    elif source_choice == "2" and format_choice == "2":
        process_case4("all_differences_case4.csv")                  

if __name__ == "__main__":
    main()
