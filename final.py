import xml.etree.ElementTree as ET
import csv, os, json, mysql.connector

XML_FOLDER            = r"C:\Users\user\Desktop\xmls_files1"
EXCLUDED_ATTRIBUTES   = "excluded_attributes.csv"
INPUT_CSV_CASE1       = "input.csv"                 
ORDER_PAIR_CSV        = "orders_to_compare.csv"     
INPUT_CSV_JSON        = "input_json.csv"            
ORDER_PAIR_JSON       = "orders_to_compare_json.csv"
DB_CONFIG = dict(
    host     = "localhost",
    user     = "root",
    password = "KONOHA777",
    database = "json_db"          
)
DEBUG = False


def strip_ns(tag): return tag.split('}', 1)[-1] if '}' in tag else tag

TAG_MAPPING, ATTR_MAPPING = {}, {}
IGNORE_TAGS = {"ApplicationArea", "Process", "ActionCriteria", "ActionExpression"}

def canonical_tag(t):  return TAG_MAPPING.get(t,  t)
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
    if DEBUG: print(">> Flattened paths:", len(elements))
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

def compare_json(a, b):
    fa, fb = flatten_json(a), flatten_json(b)
    diffs = []
    for k in sorted(set(fa)|set(fb)):
        v1, v2 = fa.get(k, "-"), fb.get(k, "-")
        if v1 != v2:
            diffs.append((k,
                          "Value mismatch" if k in fa and k in fb else "Missing key",
                          v1, v2))
    return diffs

def load_excluded(csv_path=EXCLUDED_ATTRIBUTES):
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            return {canonical_attr(r['attribute'].strip())
                    for r in csv.DictReader(f) if r['attribute'].strip()}
    except FileNotFoundError:
        print("! Attribute exclusion file not found – continuing without.")
        return set()

def compare_xml_dicts(wcs, mic, excl):
    diffs = []
    for p,g in wcs.items():
        if p not in mic:
            diffs.append((g["attrib"].get("name", p.split("/")[-1]),"Tag missing",g["text"],"-"))
            continue
        m = mic[p]
        for attr, wv in g["attrib"].items():
            if attr in excl: continue
            mv = m["attrib"].get(attr)
            if mv is None:        diffs.append((attr,"Attribute missing",wv,"-"))
            elif mv != wv:        diffs.append((attr,"Attribute mismatch",wv,mv))
        if g["text"] != m["text"]:
            diffs.append(("(text)","Text mismatch",g["text"],m["text"]))
    for p in mic:
        if p not in wcs:
            g = mic[p]
            diffs.append((g["attrib"].get("name", p.split("/")[-1]),"Extra tag","-",g["text"]))
    return diffs

def write_csv(rows, path):
    with open(path,"w",newline="",encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["attribute","difference type","wcs value","microservice value"])
        w.writerows(rows)
    print(f"\n✔ {len(rows)} difference rows written → {path}")


def process_case1(inp=INPUT_CSV_CASE1, out="all_differences_case1.csv"):
    excl = load_excluded(); diffs=[]
    for r in csv.DictReader(open(inp,encoding="utf-8")):
        wp, mp = (os.path.join(XML_FOLDER, r["wcs_xml"]),
                  os.path.join(XML_FOLDER, r["micro_xml"]))
        print(f"\n• FS-XML  {r['wcs_xml']} ↔ {r['micro_xml']}")
        try:
            wd, md = map(flatten_elements,
                         (ET.parse(wp).getroot(), ET.parse(mp).getroot()))
            diffs += compare_xml_dicts(wd, md, excl)
        except ET.ParseError as e: print("  XML parse error:",e)
    write_csv(diffs, out)

def fetch_xml(conn, oid):
    cur=conn.cursor(); cur.execute("SELECT xml_content FROM orders WHERE order_id=%s",(oid,))
    r=cur.fetchone(); return r and r[0]

def fetch_json(conn, oid):
    cur=conn.cursor(); cur.execute("SELECT json_content FROM orders WHERE order_id=%s",(oid,))
    r=cur.fetchone();  return r and r[0]       

def process_case2(out="all_differences_case2.csv", pair_csv=ORDER_PAIR_CSV):
    conn=mysql.connector.connect(**DB_CONFIG)
    excl=load_excluded(); diffs=[]
    for wcs_id,mic_id in [(r["wcs_order_id"],r["micro_order_id"])
                          for r in csv.DictReader(open(pair_csv,encoding="utf-8"))]:
        print(f"\n• DB-XML  {wcs_id} ↔ {mic_id}")
        wx, mx = fetch_xml(conn,wcs_id), fetch_xml(conn,mic_id)
        if not wx or not mx: print("  missing XML – skipped"); continue
        diffs += compare_xml_dicts(flatten_elements(ET.fromstring(wx)),
                                   flatten_elements(ET.fromstring(mx)), excl)
    conn.close(); write_csv(diffs,out)

def process_case3(inp=INPUT_CSV_JSON, out="all_differences_case3.csv"):
    diffs=[]
    for r in csv.DictReader(open(inp,encoding="utf-8")):
        wp, mp = (os.path.join(XML_FOLDER, r["wcs_json"]),
                  os.path.join(XML_FOLDER, r["micro_json"]))
        print(f"\n• FS-JSON {r['wcs_json']} ↔ {r['micro_json']}")
        try:
            j1, j2 = json.load(open(wp,"r",encoding="utf-8")), \
                     json.load(open(mp,"r",encoding="utf-8"))
            diffs += compare_json(j1,j2)
        except Exception as e: print("  JSON error:",e)
    write_csv(diffs,out)

def process_case4(out="all_differences_case4.csv", pair_csv=ORDER_PAIR_JSON):
    conn=mysql.connector.connect(**DB_CONFIG); diffs=[]
    for wcs_id,mic_id in [(r["wcs_order_id"],r["micro_order_id"])
                          for r in csv.DictReader(open(pair_csv,encoding="utf-8"))]:
        print(f"\n• DB-JSON {wcs_id} ↔ {mic_id}")
        j1, j2 = fetch_json(conn,wcs_id), fetch_json(conn,mic_id)
        if not j1 or not j2: print("  missing JSON – skipped"); continue
        try:
            if isinstance(j1,str): j1=json.loads(j1)
            if isinstance(j2,str): j2=json.loads(j2)
        except json.JSONDecodeError as e:
            print("  decode error:",e); continue
        diffs += compare_json(j1,j2)
    conn.close(); write_csv(diffs,out)

def main():
    src = input("Select source  (1-File  2-DB)  : ").strip()
    fmt = input("Select format (1-XML   2-JSON): ").strip()
    if (src,fmt)==("1","1"): process_case1()
    elif (src,fmt)==("2","1"): process_case2()
    elif (src,fmt)==("1","2"): process_case3()
    elif (src,fmt)==("2","2"): process_case4()
    else: print("Invalid choice.")

if __name__ == "__main__":
    main()
