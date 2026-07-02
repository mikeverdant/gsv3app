"""GSV3 daily data pipeline (GitHub Actions).
Flow: pull imports/latest.ndjson.gz (Supabase) -> parse -> merge with GitHub CSV
-> dedup -> cache-first geocode (write-back) -> upload events.json -> push CSV.
Secrets come from environment variables (GitHub Actions secrets vault).
"""
import os, sys, json, csv, io, re, time, base64, gzip, unicodedata
from datetime import date, datetime
import requests

# ---------- config ----------
SUPABASE_URL = "https://hmluygfhvdegmealscky.supabase.co"
GITHUB_REPO = "mikeverdant/gsv3app"
GITHUB_CSV_PATH = "portland-metro.csv"
GITHUB_BRANCH = "main"
EVENTS_BUCKET = "events"
IMPORTS_BUCKET = "imports"
IMPORT_NAME = "latest.ndjson.gz"
JSON_NAME = "events.json"
CACHE_TABLE = "geocode_cache"
DAILY_GEOCODE_LIMIT = 4500

APP_COLUMNS = ["featured","date","start_time","event_name","venue","venue_address",
               "venue_lat","venue_lng","venue_notes","venue_map_status","region",
               "url","category","price","description"]

GITHUB_TOKEN = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
LOCATIONIQ_KEY = os.environ.get("LOCATIONIQ_KEY")
missing = [n for n, v in [("GITHUB_TOKEN/GH_TOKEN", GITHUB_TOKEN),
                          ("SUPABASE_SERVICE_KEY", SUPABASE_KEY),
                          ("LOCATIONIQ_KEY", LOCATIONIQ_KEY)] if not v]
if missing:
    sys.exit(f"Missing secrets: {missing}")

SB = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
GH = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}

# ---------- helpers ----------
def norm_text(s):
    if s is None: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", s.lower())).strip()

def venue_key(venue, address=""):
    v, a = norm_text(venue), norm_text(address)
    return f"{v}|{a}" if a else v

def filled_count(r):
    return sum(1 for c in APP_COLUMNS if (r.get(c) or "").strip())

def blank_row():
    return {c: "" for c in APP_COLUMNS}

# ---------- category keywords (mirrors app inferCategory) ----------
CATEGORY_KEYWORDS = {
  "Music": ["music","concert","band","live","jazz","bluegrass","acoustic","folk","hip-hop","electronic","dj","orchestra","choir","symphony","recital","vinyl","album","song","sing","perform"],
  "Comedy": ["comedy","improv","standup","stand-up","comic","laugh","humor","sketch"],
  "Theater": ["theater","theatre","play","musical","stage","opera","ballet","storytelling","spoken word","cabaret","burlesque"],
  "Film": ["film","cinema","screening","movie","documentary"],
  "Arts": ["art","gallery","exhibit","museum","poetry","literary","reading","author","photography","mural"],
  "Dance": ["salsa","bachata","swing dance","tango","lindy","ballroom","milonga"],
  "Talks": ["lecture","talk","panel","book club","keynote","seminar","symposium"],
  "Food & Drink": ["food","drink","beer","wine","cocktail","tasting","dining","brunch","dinner","restaurant","culinary","chef","whiskey","spirits","bar"],
  "Markets": ["market","farmers","fair","craft","vendor","artisan","bazaar"],
  "Nightlife": ["nightlife","club","lounge","party","rave","techno","house","late night"],
  "Community": ["fundraiser","benefit","nonprofit","volunteer","civic","cultural","heritage","meetup","festival","parade","rally"],
  "Family": ["family","kids","children","all ages","storytime"],
  "Sports": ["sports","game","tournament","marathon","race","match"],
  "Wellness": ["yoga","meditation","wellness","mindfulness","sound bath","breathwork"],
}

def infer_category(name, venue, genres, description):
    text = f"{name} {venue} {genres} {description}".lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(k in text for k in kws): return cat
    return "Event"

# ---------- raw record parsing ----------
def _get(rec, *names):
    """Case/format-tolerant field lookup on a raw record dict."""
    lower = {re.sub(r"[^a-z0-9]", "", k.lower()): v for k, v in rec.items() if isinstance(k, str)}
    for n in names:
        v = lower.get(re.sub(r"[^a-z0-9]", "", n.lower()))
        if v not in (None, "", [], {}): return v
    return ""

LABEL_RE = re.compile(r"^(Performers|Event Dates|Date & Time|Price|Venue|Location|Contact Information|Description|Genres|Source URL)\s*:\s*(.*)$", re.I | re.M)

def _labels_from_text(text):
    out = {}
    if not isinstance(text, str): return out
    for m in LABEL_RE.finditer(text):
        key = re.sub(r"[^a-z]", "", m.group(1).lower())
        val = m.group(2).strip()
        if val and val.lower() not in ("not provided in source.", "not provided in source"):
            out[key] = val
    return out

def _parse_date(val):
    """Return (YYYY-MM-DD, HH:MM) from ISO-ish values; ('','') if unusable."""
    if isinstance(val, list) and val: val = val[0]
    if isinstance(val, dict):
        val = val.get("start") or val.get("date") or ""
    s = str(val or "").strip()
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})[T ]?(\d{2}):(\d{2})?", s)
    if m:
        d = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        t = f"{m.group(4)}:{m.group(5) or '00'}"
        return d, ("" if t == "00:00" else t)
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return m.group(0), ""
    return "", ""

def parse_import_records(lines):
    rows, quarantine = [], []
    for ln in lines:
        ln = ln.strip()
        if not ln: continue
        try:
            rec = json.loads(ln)
        except Exception:
            quarantine.append({"reason": "bad json", "raw": ln[:300]}); continue
        if not isinstance(rec, dict):
            quarantine.append({"reason": "not an object", "raw": str(rec)[:300]}); continue

        labels = _labels_from_text(_get(rec, "claim", "text", "content", "body"))
        name = str(_get(rec, "event_name", "name", "title", "event") or "").strip()
        d, t = _parse_date(_get(rec, "event_dates", "date", "start_date", "start", "datetime"))
        if not d:
            d2, t2 = _parse_date(labels.get("eventdates", ""))
            d, t = d or d2, t or t2
        venue = str(_get(rec, "venue", "venue_name") or labels.get("venue", "")).strip()
        addr = str(_get(rec, "venue_address", "address", "location") or labels.get("location", "")).strip()
        url = str(_get(rec, "url", "source_url", "link") or labels.get("sourceurl", "")).strip()
        price = str(_get(rec, "price") or labels.get("price", "")).strip()
        desc = str(_get(rec, "description") or labels.get("description", "")).strip()
        genres = str(_get(rec, "genres", "genre") or labels.get("genres", "")).strip()
        cat = str(_get(rec, "category") or "").strip() or infer_category(name, venue, genres, desc)
        stime = str(_get(rec, "start_time", "time") or "").strip() or t

        if not name or not d:
            quarantine.append({"reason": f"missing {'name' if not name else 'date'}",
                               "raw": json.dumps(rec, default=str)[:300]})
            continue
        # If the address is just a city name, keep it but don't fake a street address.
        row = blank_row()
        row.update({"date": d, "start_time": stime, "event_name": name, "venue": venue,
                    "venue_address": addr, "url": url, "category": cat,
                    "price": price, "description": desc})
        rows.append(row)
    return rows, quarantine

# ---------- steps ----------
def fetch_import():
    r = requests.get(f"{SUPABASE_URL}/storage/v1/object/{IMPORTS_BUCKET}/{IMPORT_NAME}", headers=SB)
    if r.status_code == 404 or (r.status_code == 400 and b"not_found" in r.content.lower()):
        print("No import file found - refresh-only run."); return [], []
    r.raise_for_status()
    data = r.content
    try:
        data = gzip.decompress(data)
    except OSError:
        pass  # already plain text
    rows, quarantine = parse_import_records(data.decode("utf-8", errors="replace").splitlines())
    print(f"Import: {len(rows)} events parsed, {len(quarantine)} quarantined.")
    if quarantine:
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=["reason", "raw"]); w.writeheader(); w.writerows(quarantine)
        requests.post(f"{SUPABASE_URL}/storage/v1/object/{IMPORTS_BUCKET}/quarantine.csv",
                      headers={**SB, "Content-Type": "text/csv", "x-upsert": "true"},
                      data=buf.getvalue().encode())
    return rows, quarantine

def fetch_github_csv():
    r = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_CSV_PATH}?ref={GITHUB_BRANCH}", headers=GH)
    r.raise_for_status()
    f = r.json()
    text = base64.b64decode(f["content"]).decode() if f.get("content") else requests.get(f["download_url"]).text
    rows = list(csv.DictReader(io.StringIO(text)))
    for row in rows:
        for c in APP_COLUMNS: row.setdefault(c, "")
    print(f"GitHub CSV: {len(rows)} rows (sha {f['sha'][:7]}).")
    return rows, f["sha"]

def dedup(existing, new):
    key = lambda r: (r.get("date","").strip(), r.get("start_time","").strip(), norm_text(r.get("event_name")))
    merged, internal, dropped = {}, 0, 0
    for r in existing:
        k = key(r)
        if k in merged:
            internal += 1
            if filled_count(r) <= filled_count(merged[k]): continue
        merged[k] = r
    for r in new:
        k = key(r)
        if k in merged:
            dropped += 1
            if filled_count(r) > filled_count(merged[k]):
                keep = merged[k]
                for c in APP_COLUMNS:
                    if not (r.get(c) or "").strip() and (keep.get(c) or "").strip():
                        r[c] = keep[c]
                merged[k] = r
        else:
            merged[k] = r
    rows = list(merged.values())
    print(f"Merged: {len(rows)} unique ({dropped} import dupes folded, {internal} internal cleaned).")
    return rows, dropped

def load_cache():
    cache_rows, page, step = [], 0, 1000
    while True:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{CACHE_TABLE}?select=*",
                         headers={**SB, "Range": f"{page*step}-{page*step+step-1}"})
        r.raise_for_status()
        batch = r.json(); cache_rows.extend(batch)
        if len(batch) < step: break
        page += 1
    if not cache_rows: sys.exit("geocode_cache empty - aborting to avoid paid re-geocoding.")
    cols = list(cache_rows[0].keys())
    def pick(*pats):
        for p in pats:
            for c in cols:
                if re.search(p, c, re.I): return c
        return None
    cmap = {"venue": pick(r"^venue$", r"venue.*name", r"^name$", r"venue", r"place", r"query", r"key"),
            "addr": pick(r"address"), "lat": pick(r"^lat", r"latitude"),
            "lng": pick(r"^lng", r"^lon", r"longitude"), "city": pick(r"^city"),
            "state": pick(r"state"), "country": pick(r"country")}
    if not (cmap["venue"] and cmap["lat"] and cmap["lng"]):
        sys.exit(f"Cannot map cache columns: {cols}")
    cache = {}
    for cr in cache_rows:
        lat, lng = cr.get(cmap["lat"]), cr.get(cmap["lng"])
        if lat in (None, "", 0) or lng in (None, "", 0): continue
        e = {"lat": lat, "lng": lng, "city": cr.get(cmap["city"]) or "",
             "state": cr.get(cmap["state"]) or "", "country": cr.get(cmap["country"]) or ""}
        cache.setdefault(venue_key(cr.get(cmap["venue"]), cr.get(cmap["addr"]) if cmap["addr"] else ""), e)
        cache.setdefault(venue_key(cr.get(cmap["venue"])), e)
    print(f"Cache: {len(cache_rows)} rows loaded.")
    return cache, cmap

def apply_coords(r, hit):
    r["venue_lat"], r["venue_lng"] = str(hit["lat"]), str(hit["lng"])
    if not (r.get("region") or "").strip() and hit["city"]: r["region"] = hit["city"]
    r["venue_map_status"] = "show"

def geocode(all_rows, cache, cmap):
    lookup = lambda r: cache.get(venue_key(r.get("venue"), r.get("venue_address"))) or cache.get(venue_key(r.get("venue")))
    hits = 0
    for r in all_rows:
        if (r.get("venue_lat") or "").strip() and (r.get("venue_lng") or "").strip(): continue
        h = lookup(r)
        if h: apply_coords(r, h); hits += 1
    uniq = {}
    for r in all_rows:
        if (r.get("venue_lat") or "").strip(): continue
        if (r.get("venue") or "").strip():
            uniq.setdefault(venue_key(r.get("venue"), r.get("venue_address")), r)
    todo = list(uniq.items())[:DAILY_GEOCODE_LIMIT]
    print(f"Cache hits: {hits}. New venues to geocode: {len(uniq)} (running {len(todo)}).")
    geocoded, failed, writeback = 0, 0, []
    for k, r in todo:
        q = ", ".join(x for x in [r.get("venue"), r.get("venue_address")] if (x or "").strip())
        try:
            g = requests.get("https://us1.locationiq.com/v1/search",
                             params={"key": LOCATIONIQ_KEY, "q": q, "format": "json",
                                     "limit": 1, "addressdetails": 1}, timeout=15)
            if g.status_code == 200 and g.json():
                top = g.json()[0]; a = top.get("address", {})
                city = a.get("city") or a.get("town") or a.get("village") or a.get("suburb") or ""
                e = {"lat": top["lat"], "lng": top["lon"], "city": city,
                     "state": a.get("state",""), "country": a.get("country","")}
                cache[k] = e; cache[venue_key(r.get("venue"))] = e
                wb = {cmap["venue"]: r.get("venue"), cmap["lat"]: float(top["lat"]), cmap["lng"]: float(top["lon"])}
                if cmap["addr"]: wb[cmap["addr"]] = r.get("venue_address") or ""
                if cmap["city"]: wb[cmap["city"]] = city
                if cmap["state"]: wb[cmap["state"]] = a.get("state","")
                if cmap["country"]: wb[cmap["country"]] = a.get("country","")
                writeback.append(wb); geocoded += 1
            else:
                failed += 1
        except Exception:
            failed += 1
        time.sleep(0.6)
    for r in all_rows:
        if (r.get("venue_lat") or "").strip(): continue
        h = lookup(r)
        if h: apply_coords(r, h)
    for j in range(0, len(writeback), 200):
        w = requests.post(f"{SUPABASE_URL}/rest/v1/{CACHE_TABLE}",
                          headers={**SB, "Content-Type": "application/json", "Prefer": "return=minimal"},
                          data=json.dumps(writeback[j:j+200]))
        if w.status_code >= 300: print(f"Cache write-back warning ({w.status_code}): {w.text[:200]}")
    print(f"Geocoded {geocoded} new venues (failed: {failed}).")
    return geocoded, failed

def publish_json(all_rows):
    today = date.today().isoformat()
    upcoming = sorted([r for r in all_rows if (r.get("date") or "") >= today],
                      key=lambda r: (r.get("date",""), r.get("start_time","")))
    payload = json.dumps([{c: (r.get(c) or "") for c in APP_COLUMNS} for r in upcoming],
                         ensure_ascii=False, separators=(",", ":")).encode()
    u = requests.post(f"{SUPABASE_URL}/storage/v1/object/{EVENTS_BUCKET}/{JSON_NAME}",
                      headers={**SB, "Content-Type": "application/json", "x-upsert": "true"}, data=payload)
    if u.status_code >= 300:
        u = requests.put(f"{SUPABASE_URL}/storage/v1/object/{EVENTS_BUCKET}/{JSON_NAME}",
                         headers={**SB, "Content-Type": "application/json", "x-upsert": "true"}, data=payload)
    u.raise_for_status()
    print(f"events.json: {len(upcoming)} upcoming, {len(payload)/1e6:.1f} MB uploaded.")
    return len(upcoming)

def push_csv(all_rows, sha):
    all_rows.sort(key=lambda r: (r.get("date",""), r.get("start_time","")))
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=APP_COLUMNS, extrasaction="ignore")
    w.writeheader()
    for r in all_rows: w.writerow({c: (r.get(c) or "") for c in APP_COLUMNS})
    body = {"message": f"daily pipeline: {len(all_rows)} events ({datetime.utcnow().isoformat()}Z)",
            "content": base64.b64encode(buf.getvalue().encode()).decode(),
            "sha": sha, "branch": GITHUB_BRANCH}
    p = requests.put(f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_CSV_PATH}",
                     headers=GH, data=json.dumps(body))
    p.raise_for_status()
    print(f"Pushed CSV: {len(all_rows)} rows.")

def main():
    new_rows, quarantine = fetch_import()
    existing, sha = fetch_github_csv()
    all_rows, dropped = dedup(existing, new_rows)
    cache, cmap = load_cache()
    geocoded, failed = geocode(all_rows, cache, cmap)
    upcoming = publish_json(all_rows)
    push_csv(all_rows, sha)
    print("=" * 40)
    print(f"DONE. total={len(all_rows)} upcoming={upcoming} imported={len(new_rows)} "
          f"dupes_folded={dropped} quarantined={len(quarantine)} geocoded_new={geocoded} geocode_failed={failed}")

if __name__ == "__main__":
    main()
