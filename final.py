import xml.etree.ElementTree as ET
import csv, os, json, mysql.connector

XML_FOLDER = r"C:\Users\madhu\OneDrive\Desktop\Crap\litmus\Crap"
EXCLUDED_ATTRIBUTES = "excluded_attributes.csv"
INPUT_CSV_CASE1 = "input.csv"
ORDER_PAIR_CSV = "orders_to_compare.csv"
INPUT_CSV_JSON = "input_json.csv"
ORDER_PAIR_JSON = "orders_to_compare_json.csv"
INPUT_CSV_HAR = "input_har.csv"
DB_CONFIG_xml = dict(host="localhost", user="root", password="KONOHA777", database="xml6")
DB_CONFIG_json = dict(host="localhost", user="root", password="KONOHA777", database="json_db")

def strip_ns(tag): return tag.split('}', 1)[-1] if '}' in tag else tag
TAG_MAPPING, ATTR_MAPPING = {}, {}
IGNORE_TAGS = {"ApplicationArea", "Process", "ActionCriteria", "ActionExpression"}

def canonical_tag(t): return TAG_MAPPING.get(t, t)
def canonical_attr(a): return ATTR_MAPPING.get(a, a)

def flatten_elements(root: ET.Element):
    elements = {}
    def rec(e, path="", sib=None):
        sib = sib or {}
        local = canonical_tag(strip_ns(e.tag))
        if local in IGNORE_TAGS: return
        attribs = {canonical_attr(strip_ns(k)): v for k, v in e.attrib.items()}
        name = attribs.get("name")
        if local in {"ProtocolData", "UserDataField"} and name:
            new_path = f"{path}/{local}[@name='{name}']"
        else:
            idx = sib.get(local, 0) + 1
            sib[local] = idx
            new_path = f"{path}/{local}[{idx}]" if path else f"/{local}[{idx}]"
        elements[new_path] = {"attrib": attribs, "text": (e.text or "").strip()}
        child_counts = {}
        for c in e: rec(c, new_path, child_counts)
    rec(root)
    return elements

def flatten_json(obj, path=""):
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(flatten_json(v, f"{path}.{k}" if path else k))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out.update(flatten_json(v, f"{path}[{i}]"))
    else:
        out[path] = str(obj)
    return out

def load_excluded(csv_path=EXCLUDED_ATTRIBUTES):
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            return {canonical_attr(r['attribute'].strip().lower()) for r in csv.DictReader(f) if r['attribute'].strip()}
    except FileNotFoundError:
        print("! Attribute exclusion file not found – continuing without.")
        return set()

def is_path_excluded(path, excl):
    segments = path.strip("/").split("/")
    for seg in segments:
        clean_tag = strip_ns(seg.split("[")[0].split("@")[0]).lower()
        if clean_tag in excl:
            return True
    return False

def compare_xml_dicts(wcs, mic, excl):
    diffs = set()
    for p, g in wcs.items():
        if is_path_excluded(p, excl): continue
        if p not in mic:
            tag_name = p.split("/")[-1].split("[")[0]
            diffs.add((tag_name, "Tag missing", g["text"], "-"))
            continue
        m = mic[p]
        for attr, wv in g["attrib"].items():
            attr_canon = canonical_attr(attr.lower())
            if attr_canon in excl: continue
            mv = m["attrib"].get(attr)
            if mv is None:
                diffs.add((attr, "Attribute missing", wv, "-"))
            elif mv != wv:
                diffs.add((attr, "Attribute mismatch", wv, mv))
        if "(text)" not in excl and g["text"] != m["text"]:
            tag_name = p.split("/")[-1].split("[")[0]
            name_attr = g["attrib"].get("name")
            if tag_name in {"UserDataField", "ProtocolData"} and name_attr:
                diffs.add((name_attr, "Text mismatch", g["text"], m["text"]))
            elif g["attrib"]:
                attr_names = ",".join(sorted(g["attrib"].keys()))
                diffs.add((attr_names, "Text mismatch", g["text"], m["text"]))
            else:
                diffs.add((tag_name, "Text mismatch", g["text"], m["text"]))
    for p, g in mic.items():
        if p in wcs or is_path_excluded(p, excl): continue
        tag_name = p.split("/")[-1].split("[")[0]
        if any((p2.split("/")[-1].split("[")[0] == tag_name) for p2 in wcs):
            continue
        diffs.add((tag_name, "Extra tag", "-", g["text"]))
    return diffs

def compare_json(a, b, excl):
    fa, fb = flatten_json(a), flatten_json(b)
    diffs = set()
    excl = {e.lower() for e in excl}

    for k in sorted(set(fa) | set(fb)):
        if k.lower() in excl:
            continue

        v1, v2 = fa.get(k, "-"), fb.get(k, "-")

        # Special handling for JSON inside postData.text
        if k.endswith("postData.text"):
            try:
                jd1, jd2 = json.loads(v1), json.loads(v2)
                nested_diffs = compare_json(jd1, jd2, excl)
                for subk, dtype, sv1, sv2 in nested_diffs:
                    diffs.add((f"{k}.{subk}", dtype, sv1, sv2))
                continue  # Skip normal diff handling for postData.text
            except Exception:
                pass  # If it's not valid JSON, fall back to default diff

        if v1 != v2:
            dtype = "Value mismatch" if k in fa and k in fb else "Missing key"
            diffs.add((k, dtype, v1, v2))

    return diffs


def write_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["attribute", "difference type", "wcs value", "microservice value"])
        for row in sorted(rows): w.writerow(row)
    print(f"\n✔ {len(rows)} difference rows written → {path}")

def fetch_xml(conn, oid):
    cur = conn.cursor()
    cur.execute("SELECT xml_content FROM orders WHERE order_id=%s", (oid,))
    r = cur.fetchone()
    return r and r[0]

def fetch_json(conn, oid):
    cur = conn.cursor()
    cur.execute("SELECT json_content FROM orders WHERE order_id=%s", (oid,))
    r = cur.fetchone()
    return r and r[0]

# ✅ Generalized HAR extraction – returns raw entries
def extract_json_from_har(har_path):
    try:
        har = json.load(open(har_path, encoding="utf-8"))
        return har.get("log", {}).get("entries", [])
    except Exception as e:
        print(f"! Error reading {har_path}: {e}")
        return []

# ✅ Generalized HAR comparison – compares full HAR entries
def process_case5(inp=INPUT_CSV_HAR, out="all_differences_case5.csv"):
    excl = load_excluded(); diffs = set()
    for r in csv.DictReader(open(inp, encoding="utf-8")):
        wcs_file = os.path.join(XML_FOLDER, r["wcs_har"])
        mic_file = os.path.join(XML_FOLDER, r["micro_har"])
        print(f"\n• FS-HAR  {r['wcs_har']} ↔ {r['micro_har']}")
        wcs_entries = extract_json_from_har(wcs_file)
        mic_entries = extract_json_from_har(mic_file)
        for i, (e1, e2) in enumerate(zip(wcs_entries, mic_entries)):
            print(f"  ⤷ Comparing entry {i+1}")
            diffs.update(compare_json(e1, e2, excl))
    write_csv(diffs, out)

def process_case1(inp=INPUT_CSV_CASE1, out="all_differences_case1.csv"):
    excl = load_excluded(); diffs = set()
    for r in csv.DictReader(open(inp, encoding="utf-8")):
        wp, mp = os.path.join(XML_FOLDER, r["wcs_xml"]), os.path.join(XML_FOLDER, r["micro_xml"])
        print(f"\n• FS-XML  {r['wcs_xml']} ↔ {r['micro_xml']}")
        try:
            wd, md = map(flatten_elements, (ET.parse(wp).getroot(), ET.parse(mp).getroot()))
            diffs.update(compare_xml_dicts(wd, md, excl))
        except ET.ParseError as e:
            print("  XML parse error:", e)
    write_csv(diffs, out)

def process_case2(out="all_differences_case2.csv", pair_csv=ORDER_PAIR_CSV):
    conn = mysql.connector.connect(**DB_CONFIG_xml)
    excl = load_excluded(); diffs = set()
    for r in csv.DictReader(open(pair_csv, encoding="utf-8")):
        wcs_id, mic_id = r["wcs_order_id"], r["micro_order_id"]
        print(f"\n• DB-XML  {wcs_id} ↔ {mic_id}")
        wx, mx = fetch_xml(conn, wcs_id), fetch_xml(conn, mic_id)
        if not wx or not mx:
            print("  missing XML – skipped")
            continue
        diffs.update(compare_xml_dicts(flatten_elements(ET.fromstring(wx)), flatten_elements(ET.fromstring(mx)), excl))
    conn.close()
    write_csv(diffs, out)

def process_case3(inp=INPUT_CSV_JSON, out="all_differences_case3.csv"):
    excl = load_excluded(); diffs = set()
    for r in csv.DictReader(open(inp, encoding="utf-8")):
        wp, mp = os.path.join(XML_FOLDER, r["wcs_json"]), os.path.join(XML_FOLDER, r["micro_json"])
        print(f"\n• FS-JSON {r['wcs_json']} ↔ {r['micro_json']}")
        try:
            j1 = json.load(open(wp, "r", encoding="utf-8"))
            j2 = json.load(open(mp, "r", encoding="utf-8"))
            diffs.update(compare_json(j1, j2, excl))
        except Exception as e:
            print("  JSON error:", e)
    write_csv(diffs, out)

def process_case4(out="all_differences_case4.csv", pair_csv=ORDER_PAIR_JSON):
    conn = mysql.connector.connect(**DB_CONFIG_json)
    excl = load_excluded(); diffs = set()
    for r in csv.DictReader(open(pair_csv, encoding="utf-8")):
        wcs_id, mic_id = r["wcs_order_id"], r["micro_order_id"]
        print(f"\n• DB-JSON {wcs_id} ↔ {mic_id}")
        j1, j2 = fetch_json(conn, wcs_id), fetch_json(conn, mic_id)
        if not j1 or not j2:
            print("  missing JSON – skipped")
            continue
        try:
            j1 = json.loads(j1) if isinstance(j1, str) else j1
            j2 = json.loads(j2) if isinstance(j2, str) else j2
            diffs.update(compare_json(j1, j2, excl))
        except json.JSONDecodeError as e:
            print("  decode error:", e)
    conn.close()
    write_csv(diffs, out)

def main():
    print("Select source:")
    print("  1: File")
    print("  2: DB")
    src = input("Your choice: ").strip()

    print("\nSelect format:")
    print("  1: XML")
    print("  2: JSON")
    print("  3: HAR")
    fmt = input("Your choice: ").strip()

    if src == "1" and fmt == "1": process_case1()
    elif src == "2" and fmt == "1": process_case2()
    elif src == "1" and fmt == "2": process_case3()
    elif src == "2" and fmt == "2": process_case4()
    elif src == "1" and fmt == "3": process_case5()
    else: print("❌ Invalid combination.")

if __name__ == "__main__":
    main()
