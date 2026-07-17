"""GSV3 daily data pipeline (GitHub Actions).
Flow: pull imports/latest.ndjson.gz (Supabase) -> parse -> merge with GitHub CSV
-> dedup -> cache-first geocode (write-back) -> upload events.json -> push CSV.
Secrets come from environment variables (GitHub Actions secrets vault).
"""
import os, sys, json, csv, io, re, time, base64, gzip, unicodedata
from datetime import date, datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import requests
from timezonefinder import TimezoneFinder

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


# ---------- geographic consistency ----------
COUNTRY_ALIASES = {
 "united states":"united states","united states of america":"united states","usa":"united states","u.s.a":"united states","u.s.":"united states","u.s.a.":"united states","us":"united states","america":"united states",
 "united kingdom":"united kingdom","uk":"united kingdom","england":"united kingdom","scotland":"united kingdom","wales":"united kingdom","northern ireland":"united kingdom","great britain":"united kingdom",
 "south africa":"south africa","canada":"canada","mexico":"mexico","australia":"australia","new zealand":"new zealand","ireland":"ireland","france":"france","germany":"germany","deutschland":"germany",
 "italy":"italy","italia":"italy","spain":"spain","portugal":"portugal","netherlands":"netherlands","the netherlands":"netherlands","holland":"netherlands","belgium":"belgium","switzerland":"switzerland",
 "austria":"austria","sweden":"sweden","norway":"norway","denmark":"denmark","finland":"finland","iceland":"iceland","poland":"poland","czech republic":"czechia","czechia":"czechia","slovakia":"slovakia",
 "hungary":"hungary","romania":"romania","bulgaria":"bulgaria","greece":"greece","turkey":"turkey","croatia":"croatia","serbia":"serbia","slovenia":"slovenia","ukraine":"ukraine","russia":"russia",
 "japan":"japan","china":"china","south korea":"south korea","korea":"south korea","india":"india","thailand":"thailand","vietnam":"vietnam","indonesia":"indonesia","malaysia":"malaysia","singapore":"singapore",
 "philippines":"philippines","taiwan":"taiwan","hong kong":"hong kong","israel":"israel","united arab emirates":"united arab emirates","uae":"united arab emirates","saudi arabia":"saudi arabia","qatar":"qatar",
 "egypt":"egypt","morocco":"morocco","nigeria":"nigeria","kenya":"kenya","ghana":"ghana","brazil":"brazil","brasil":"brazil","argentina":"argentina","chile":"chile","colombia":"colombia","peru":"peru",
 "uruguay":"uruguay","ecuador":"ecuador","costa rica":"costa rica","panama":"panama","guatemala":"guatemala","dominican republic":"dominican republic","puerto rico":"puerto rico","jamaica":"jamaica",
 "cuba":"cuba","estonia":"estonia","latvia":"latvia","lithuania":"lithuania","luxembourg":"luxembourg","malta":"malta","cyprus":"cyprus","scotland":"united kingdom",
}
US_STATES = {
 "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA","colorado":"CO","connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA","hawaii":"HI","idaho":"ID",
 "illinois":"IL","indiana":"IN","iowa":"IA","kansas":"KS","kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD","massachusetts":"MA","michigan":"MI","minnesota":"MN","mississippi":"MS",
 "missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV","new hampshire":"NH","new jersey":"NJ","new mexico":"NM","new york":"NY","north carolina":"NC","north dakota":"ND","ohio":"OH",
 "oklahoma":"OK","oregon":"OR","pennsylvania":"PA","rhode island":"RI","south carolina":"SC","south dakota":"SD","tennessee":"TN","texas":"TX","utah":"UT","vermont":"VT","virginia":"VA",
 "washington":"WA","west virginia":"WV","wisconsin":"WI","wyoming":"WY","district of columbia":"DC","washington dc":"DC","washington d.c.":"DC",
}
STATE_BY_ABBREV = {v: k for k, v in US_STATES.items()}
_ABBREV_RE = re.compile(r",\s*([A-Z]{2})\b")

STREET_SUFFIX = r"(?:av(?:e(?:nue)?)?|st(?:reet)?|blvd|boulevard|r(?:oa)?d|dr(?:ive)?|way|lane|ln|place|pl|court|ct|hwy|highway|pkwy|parkway|cir(?:cle)?|ter(?:race)?)"

def _geo_tokens3(text):
    """Returns (countries, strong_states, weak_states).
    strong = full state name; weak = 2-letter abbrev (can collide with country codes)."""
    t = " " + re.sub(r"\s+", " ", str(text or "")).strip() + " "
    low = t.lower()
    countries, strong, weak = set(), set(), set()
    for alias, canon in COUNTRY_ALIASES.items():
        if len(alias) <= 3:
            if re.search(r"[,\s]" + re.escape(alias) + r"[\s,.!?]", low): countries.add(canon)
        elif f" {alias} " in low or f" {alias}," in low or f" {alias}." in low or low.rstrip().endswith(" " + alias):
            countries.add(canon)
    for name, ab in US_STATES.items():
        # a state name immediately followed by a street suffix is a street, not a state
        if re.search(r"\b" + re.escape(name) + r"\b(?!\s+" + STREET_SUFFIX + r"\b)", low):
            strong.add(ab)
    for m in _ABBREV_RE.finditer(t):
        if m.group(1) in STATE_BY_ABBREV: weak.add(m.group(1))
    for m in re.finditer(r"\b([A-Z]{2})\s+\d{5}(?:-\d{4})?\b", t):
        if m.group(1) in STATE_BY_ABBREV: strong.add(m.group(1))   # "VT 05403" / "Newport KY 41071": state code + ZIP is unambiguous
    if re.search(r"\bd\.?\s?c\.?[\s,.!?]", low): strong.add("DC")
    if strong: countries.add("united states")
    return countries, strong, weak

def geo_tokens(text):
    c, s, w = _geo_tokens3(text)
    return c, (s | w)

def entry_matches_text(entry, text):
    """True if a geocode entry (country/state) is consistent with location text.
    No recognizable tokens = cannot judge = treated as consistent."""
    countries, strong, weak = _geo_tokens3(text)
    states = strong | weak
    ec = str(entry.get("country") or "").lower().strip()
    ec = COUNTRY_ALIASES.get(ec, ec)
    es = str(entry.get("state") or "").strip()
    es_ab = US_STATES.get(es.lower(), es.upper() if len(es) == 2 else "")
    if countries and ec and ec not in countries: return False
    if states:
        if ec and ec != "united states":
            return not strong   # only a full state name outvotes a foreign entry; ', DE' codes don't
        if es_ab and es_ab not in states: return False
    return True

# Conservative US state bounding boxes (lat_min, lat_max, lon_min, lon_max)
STATE_BBOX = {
 "AL":(30.1,35.1,-88.5,-84.9),"AK":(51.2,71.4,-179.2,-129.9),"AZ":(31.3,37.1,-114.9,-109.0),
 "AR":(33.0,36.5,-94.7,-89.6),"CA":(32.5,42.1,-124.5,-114.1),"CO":(36.9,41.1,-109.1,-102.0),
 "CT":(40.9,42.1,-73.8,-71.7),"DE":(38.4,39.9,-75.8,-75.0),"FL":(24.4,31.1,-87.7,-79.9),
 "GA":(30.3,35.1,-85.7,-80.8),"HI":(18.9,22.3,-160.3,-154.8),"ID":(41.9,49.1,-117.3,-111.0),
 "IL":(36.9,42.6,-91.6,-87.0),"IN":(37.7,41.8,-88.2,-84.7),"IA":(40.3,43.6,-96.7,-90.1),
 "KS":(36.9,40.1,-102.1,-94.6),"KY":(36.4,39.2,-89.6,-81.9),"LA":(28.9,33.1,-94.1,-88.8),
 "ME":(43.0,47.5,-71.1,-66.9),"MD":(37.9,39.8,-79.5,-75.0),"MA":(41.2,42.9,-73.6,-69.9),
 "MI":(41.6,48.3,-90.5,-82.1),"MN":(43.4,49.4,-97.3,-89.5),"MS":(30.1,35.1,-91.7,-88.0),
 "MO":(35.9,40.7,-95.8,-89.1),"MT":(44.3,49.1,-116.1,-104.0),"NE":(39.9,43.1,-104.1,-95.3),
 "NV":(35.0,42.1,-120.1,-114.0),"NH":(42.6,45.4,-72.6,-70.6),"NJ":(38.9,41.4,-75.6,-73.9),
 "NM":(31.3,37.1,-109.1,-103.0),"NY":(40.4,45.1,-79.8,-71.8),"NC":(33.8,36.6,-84.4,-75.4),
 "ND":(45.9,49.1,-104.1,-96.5),"OH":(38.4,42.0,-84.9,-80.5),"OK":(33.6,37.1,-103.1,-94.4),
 "OR":(41.9,46.3,-124.7,-116.4),"PA":(39.7,42.3,-80.6,-74.6),"RI":(41.1,42.1,-71.9,-71.1),
 "SC":(32.0,35.3,-83.4,-78.5),"SD":(42.4,45.9,-104.1,-96.4),"TN":(34.9,36.7,-90.4,-81.6),
 "TX":(25.8,36.6,-106.7,-93.5),"UT":(36.9,42.1,-114.1,-109.0),"VT":(42.7,45.1,-73.5,-71.4),
 "VA":(36.5,39.5,-83.7,-75.2),"WA":(45.5,49.1,-124.9,-116.9),"WV":(37.1,40.7,-82.7,-77.7),
 "WI":(42.4,47.1,-92.9,-86.7),"WY":(40.9,45.1,-111.1,-104.0),"DC":(38.8,39.0,-77.2,-76.9),
}
BBOX_MARGIN = 0.3  # ~33 km slack so border venues never get falsely cleared

def _in_bbox(lat, lng, box):
    a, b, c, d = box
    return (a - BBOX_MARGIN) <= lat <= (b + BBOX_MARGIN) and (c - BBOX_MARGIN) <= lng <= (d + BBOX_MARGIN)

def _in_us(lat, lng):
    return (_in_bbox(lat, lng, (24.4, 49.4, -125.0, -66.9))
            or _in_bbox(lat, lng, STATE_BBOX["AK"]) or _in_bbox(lat, lng, STATE_BBOX["HI"]))

def coords_contradict_text(lat, lng, text):
    """True only when coordinates PROVABLY contradict the location text.
    Weak (abbrev-only) evidence counts only for pins inside the US, so foreign
    country codes like ', DE' can never clear a correct foreign pin. Foreign-vs-
    foreign and unknowns return False (cannot judge = do not touch)."""
    try:
        lat, lng = float(lat), float(lng)
    except (TypeError, ValueError):
        return False
    countries, strong, weak = _geo_tokens3(text)
    states = strong | weak
    if states:
        if any(_in_bbox(lat, lng, STATE_BBOX[s]) for s in states if s in STATE_BBOX):
            return False
        return bool(strong) or _in_us(lat, lng)
    if countries == {"united states"}:
        return not _in_us(lat, lng)
    return False

# Records that are stories ABOUT events (hoaxes, rumors, denials), not events.
NON_EVENT_RE = re.compile(
    r"\b(fake news|real or fake|hoax|debunk\w*|rumou?rs?|not true|untrue|falsely|"
    r"denies|denied|will not (?:be )?(?:perform\w*|appear\w*|present|attend\w*|happen\w*|there)|"
    r"won'?t (?:be )?(?:perform\w*|appear\w*|present|attend\w*)|"
    r"uncertaint\w* (?:about|surrounding|over) (?:his|her|their|the) (?:performance|appearance|attendance)|"
    r"no plans to|never (?:planned|scheduled)|scam(?:mers?)?|misinformation)\b", re.I)

# Products sold alongside events (gift cards, parking passes, shuttles), not
# events themselves. Matched against the NAME ONLY: a description saying
# "free parking available" must never kill a real event.
MERCH_RE = re.compile(
    r"\b(gift ?cards?|parking pass(?:es)?|parking only|park (?:&|and) ride|"
    r"shuttle (?:to|bus|service)|camping pass(?:es)?|vip upgrade|"
    r"meet (?:&|and) greet upgrade)\b", re.I)

def looks_like_non_event(name, desc):
    if MERCH_RE.search(name or ""): return True
    return bool(NON_EVENT_RE.search(f"{name} {desc}"))

# ---------- category keywords (mirrors app inferCategory) ----------
#
# Ordered most-specific first: ties break toward the earlier entry, so a "jazz
# brunch" lands in Music while a plain "brunch" stays in Food & Drink.
#
# Two rules learned the hard way, do not break them:
#
#  1. MATCHING IS WORD-BOUNDARY, NOT SUBSTRING. The old version used
#     `k in text`, so "improv" matched "improve" (a sewing club became Comedy),
#     "match" matched "matcha" (a tea pop-up became Sports), and "house" matched
#     "Charlton House" (a history tour became Nightlife).
#
#  2. NO GENERIC VERBS. The old Music list held "perform" and "live", which
#     appear in nearly every event blurb ever written. Combined with
#     first-match-wins, Music swallowed comedians, ballets, farmers markets and
#     even a bus shuttle before the right category was ever tested.
CATEGORY_KEYWORDS = {
  "Wellness":     ["yoga","meditation","wellness","mindfulness","sound bath","breathwork","reiki","pilates","tai chi","sauna"],
  "Comedy":       ["comedy","comedian","comedians","improv","standup","stand-up","open mic","sketch comedy"],
  "Film":         ["film","films","cinema","screening","movie","documentary","imax","film festival"],
  "Dance":        ["salsa","bachata","swing dance","tango","lindy","ballroom","milonga","ballet","dance party","line dancing","dance troupe"],
  "Theater":      ["theater","theatre","musical","opera","cabaret","burlesque","broadway","playhouse","pantomime"],
  "Sports":       ["rodeo","tournament","marathon","5k","10k","fun run","racing","regatta","derby"],
  "Markets":      ["market","farmers market","flea market","craft fair","vendor market","artisan","bazaar","swap meet","flohmarkt"],
  "Food & Drink": ["food truck","beer","wine","cocktail","cocktails","tasting","brewery","distillery","winery","culinary","chef","whiskey","bbq","ribfest","food festival","brunch","matcha","coffee"],
  "Talks":        ["lecture","panel","book club","keynote","seminar","symposium","workshop","author talk"],
  "Family":       ["storytime","story time","kids","children","toddler","toddlers","babies","family fun","all ages"],
  "Arts":         ["gallery","exhibition","exhibit","museum","poetry","literary","photography","mural","sculpture","art show","art fair"],
  "Nightlife":    ["nightclub","nightlife","dj set","rave","techno","house music","late night","club night","after party"],
  "Music":        ["music","concert","concerts","live music","band","bands","jazz","blues","bluegrass","acoustic","hip-hop","orchestra","choir","symphony","recital","quartet","dj","gig","songwriter","metal","rock","punk","indie","country","folk","electronic","edm","rap","r&b","soul","funk","reggae","classical","pop","tribute","ensemble","philharmonic","tour","album"],
  "Community":    ["fundraiser","benefit","nonprofit","volunteer","civic","heritage","meetup","meet-up","parade","rally","festival","street fair"],
}

_CAT_PATTERNS = {
    cat: [re.compile(r"\b" + re.escape(k) + r"\b") for k in kws]
    for cat, kws in CATEGORY_KEYWORDS.items()
}

# Terms that, appearing in the event NAME, are definitional on their own:
# "concert" means a concert, "farmers market" means a farmers market. These
# have essentially no second meaning, so a single name hit decides. Everything
# else (theater, festival, civic, party, band names...) is ambiguous and can
# only SUPPORT a category, never decide it alone.
CATEGORY_DEFINITIONAL = {
  "Wellness":     ["yoga","meditation","sound bath","breathwork","reiki","pilates","tai chi"],
  "Comedy":       ["comedy","comedian","comedians","standup","stand-up","improv"],
  "Film":         ["screening","documentary","movie","film festival"],
  "Dance":        ["salsa","bachata","tango","milonga","swing dance","line dancing","ballet","ballroom"],
  "Theater":      ["musical","opera","cabaret","burlesque","pantomime"],
  "Sports":       ["rodeo","marathon","5k","10k","fun run","regatta"],
  "Markets":      ["farmers market","flea market","craft fair","swap meet","bazaar","flohmarkt","vendor market"],
  "Food & Drink": ["food truck","tasting","brewery","distillery","winery","ribfest","food festival","food fest","bbq"],
  "Talks":        ["lecture","keynote","seminar","symposium","book club","author talk"],
  "Family":       ["storytime","story time"],
  "Arts":         ["exhibition","exhibit","art show","art fair","poetry"],
  "Nightlife":    ["nightclub","dj set","rave","club night","after party","techno","house music"],
  "Music":        ["concert","concerts","live music","music","symphony","orchestra","philharmonic","recital","choir","quartet",
                   "jazz","blues","bluegrass","reggae","hip-hop","punk rock","heavy metal","indie rock","edm","dj set","songwriter","tribute to"],
  "Markets":      ["farmers market","flea market","craft fair","swap meet","bazaar","vendor market","market"],
  "Community":    ["fundraiser","parade","street fair"],
}

_DEF_PATTERNS = {
    cat: [re.compile(r"\b" + re.escape(k) + r"\b") for k in kws]
    for cat, kws in CATEGORY_DEFINITIONAL.items()
}

# Words that appear in VENUE names but predict nothing, because these venues
# host everything: a "Theater" hosts rock, comedy and ballet; an "Auditorium"
# hosts all of the above plus graduations. Their venue hits are ignored so a
# venue's name can never decide a category by itself. Specific-use venue words
# (comedy club, brewery, museum, library) are NOT here: those genuinely predict.
# Terms a DESCRIPTION may prove a category with. Far narrower than the name
# list: a blurb saying "music" proves nothing (almost all of them do, which is
# how a French street market got tagged Music), but "A concert featuring..."
# states what the event actually is.
CATEGORY_DEFINITIONAL_DESC = {
  "Music":        ["concert","concerts","live music performance"],
  "Comedy":       ["comedians","comedy show","stand-up comedy","standup comedy"],
  "Film":         ["screening","film screening"],
  "Theater":      ["stage production"],
  "Markets":      ["farmers market","flea market"],
  "Family":       ["storytime","story time","playgroup","toddler","toddlers"],
}

_DEF_DESC_PATTERNS = {
    cat: [re.compile(r"\b" + re.escape(k) + r"\b") for k in kws]
    for cat, kws in CATEGORY_DEFINITIONAL_DESC.items()
}

# The opposite of the stopwords: venue phrases that ARE single-use enough to
# decide on their own. A room with "Comedy" in its name hosts comedy; a cinema
# shows films. Deliberately tiny, because most venues host anything.
CATEGORY_DEFINITIONAL_VENUE = {
  "Comedy":    ["comedy"],
  "Film":      ["cinema","cineplex","movie theater","drive-in"],
  "Nightlife": ["nightclub"],
  "Music":     ["music hall","concert hall","jazz club"],
  "Wellness":  ["yoga studio","wellness center"],
}

_DEF_VENUE_PATTERNS = {
    cat: [re.compile(r"\b" + re.escape(k) + r"\b") for k in kws]
    for cat, kws in CATEGORY_DEFINITIONAL_VENUE.items()
}

VENUE_STOPWORD_RE = re.compile(
    r"\b(theater|theatre|auditorium|amphitheat(?:er|re)|arena|coliseum|stadium|"
    r"hall|center|centre|pavilion|plaza|park|room|stage|garden|casino|civic|"
    r"memorial|field|bowl|dome|complex|venue|space|studios?)\b", re.I)


def infer_category(name, venue, genres, description):
    """Score every category; a category may WIN only with corroboration.

    Name hits score 5, venue 2, description 1 (the name carries the real
    signal; descriptions are boilerplate). But scoring alone is not enough:
    a category qualifies to win only when EITHER
      (a) a definitional term appears in the name ("concert", "standup",
          "farmers market" — words with no second meaning), OR
      (b) it has two or more independent keyword hits across name, venue,
          and description — evidence that corroborates itself.
    One ambiguous word never decides. A band called "Puppy Pool Party" at a
    community center matches nothing definitional and nothing twice, so it
    stays "Event" instead of becoming a pool party. A venue named "...Theatre"
    can no longer drag a Vince Gill concert into Theater by itself.

    If the two best qualifying categories tie, the answer is "Event": refusal
    beats a coin flip, same as geocoding."""
    n = (name or "").lower()
    v = (venue or "").lower()
    d = f"{genres or ''} {description or ''}".lower()
    best, best_score, second_score = "Event", 0, 0
    for cat, pats in _CAT_PATTERNS.items():
        score = 0
        spans = {"n": [], "v": [], "d": []}
        for p in pats:
            hit_n, hit_v, hit_d = p.search(n), p.search(v), p.search(d)
            # A generic venue word (Theater, Auditorium, Civic...) says nothing
            # about what is happening inside it.
            if hit_v and VENUE_STOPWORD_RE.fullmatch(hit_v.group(0)):
                hit_v = None
            if hit_n: score += 5; spans["n"].append(hit_n.span())
            if hit_v: score += 2; spans["v"].append(hit_v.span())
            if hit_d: score += 1; spans["d"].append(hit_d.span())
        # Count INDEPENDENT evidence, not raw matches. Two things fake
        # corroboration and both have burned us:
        #   - the same word in two fields (descriptions echo venue names:
        #     "...at the Roseland Theater"), and
        #   - two overlapping keywords on the same words ("music" and "live
        #     music" both hit one phrase), which put a food festival in Music.
        # So: merge overlapping spans within a field, then count distinct
        # matched TEXT across fields.
        seen_text = set()
        for field, text in (("n", n), ("v", v), ("d", d)):
            merged = []
            for s, e in sorted(spans[field]):
                if merged and s < merged[-1][1]:
                    merged[-1] = (merged[-1][0], max(merged[-1][1], e))
                else:
                    merged.append((s, e))
            for s, e in merged:
                seen_text.add(text[s:e].strip())
        distinct = len(seen_text)
        # A definitional term proves the category on its own, and must carry
        # weight: "food fest" proved Food & Drink but scored 0 and so could
        # never win. Definitional matches now floor the score by field.
        definitional = False
        if any(p.search(n) for p in _DEF_PATTERNS.get(cat, [])):
            definitional = True; score = max(score, 5)
        if any(p.search(v) for p in _DEF_VENUE_PATTERNS.get(cat, [])):
            definitional = True; score = max(score, 2)
        if any(p.search(d) for p in _DEF_DESC_PATTERNS.get(cat, [])):
            definitional = True; score = max(score, 1)
        if not definitional and distinct < 2:
            continue  # one ambiguous signal never decides
        if score > best_score:
            best, best_score, second_score = cat, score, best_score
        elif score > second_score:
            second_score = score
    if best_score > 0 and best_score == second_score:
        return "Event"  # two categories equally likely -> honest refusal
    return best


# ---------- MusicBrainz artist lookup (free, no key, 1 req/sec) ----------
#
# Recovers artist-name-only events ("Arlo Parks", "SOFI TUKKER at Warfield")
# that the keyword inferrer honestly leaves in "Event" because nothing in the
# text says music. Same refuse-over-wrong rules as geocoding:
#   - only clean 2+ word candidate names are queried (one common word like
#     "Sunrise" will exact-match some obscure artist and lie)
#   - the MusicBrainz match must be EXACT (score 100 + normalized-name equal)
#   - the artist must carry at least one genre/tag (a bare name row in their
#     database is not evidence the event is a concert)
# Results cache to mb_artists.json (hits AND misses), so each artist costs one
# request ever. Per-run lookups are capped; the backlog converges over runs.

MB_CACHE_NAME = "mb_artists.json"
MB_CAP = 300          # max fresh lookups per run (~6 min at 1.1s each)
MB_UA = {"User-Agent": "GrooveSeeker/1.0 (https://gsv3.ai)"}

# Trailing junk stripped from names before treating them as artist candidates.
_MB_SPLIT_RE = re.compile(
    r"\s+(?:at|@|w/|with|ft\.?|feat\.?|featuring|presents?|live at|live in|in concert)\s+.*$"
    r"|\s+\d{1,3}(?:st|nd|rd|th)\s+anniversary\b.*$"
    r"|\s+(?:anniversary|world|farewell|reunion|north american)\s+tour\b.*$"
    r"|\s+tour\s*(?:20\d\d)?$"
    r"|\s*[|•~:].*$|\s+-\s+.*$|\s*\(.*\)\s*$", re.I)

def _mb_candidate(name):
    """Reduce an event name to a plausible artist name, or "" to skip."""
    n = _MB_SPLIT_RE.sub("", (name or "").strip()).strip(" -–,")
    toks = norm_text(n).split()
    if len(toks) < 2 or len(toks) > 6: return ""   # 1-word names are match-bait
    if any(t.isdigit() and len(t) == 4 for t in toks): return ""  # years = tour titles
    return n

def _mb_lookup(name):
    """One artist search. Returns genre string on a proven match, "" otherwise."""
    q = 'artist:"' + name.replace('"', "") + '"'
    r = requests.get("https://musicbrainz.org/ws/2/artist/",
                     params={"query": q, "fmt": "json", "limit": 1},
                     headers=MB_UA, timeout=15)
    if r.status_code != 200: return None   # None = transient failure, retry later
    arts = (r.json() or {}).get("artists") or []
    if not arts: return ""
    a = arts[0]
    if int(a.get("score", 0)) < 100: return ""
    if norm_text(a.get("name")) != norm_text(name): return ""
    tags = [t.get("name", "") for t in (a.get("tags") or []) if t.get("name")]
    if not tags: return ""
    return tags[0]

def enrich_artists(all_rows):
    """Second pass over rows still in "Event": exact artist match -> Music.
    The matched genre is kept in the cache for the future subcategory feature."""
    cache = _load_json_map(MB_CACHE_NAME, {})
    looked, matched, hits = 0, 0, 0
    for r in all_rows:
        if (r.get("category") or "").strip() != "Event": continue
        cand = _mb_candidate(r.get("event_name"))
        if not cand: continue
        key = norm_text(cand)
        if key in cache:
            if cache[key].get("genre"):
                r["category"] = "Music"; hits += 1
            continue
        if looked >= MB_CAP: continue
        try:
            genre = _mb_lookup(cand)
        except Exception:
            genre = None
        time.sleep(1.1)
        looked += 1
        if genre is None: continue          # transient failure: not cached, retried next run
        cache[key] = {"genre": genre}       # "" caches a proven miss
        if genre:
            r["category"] = "Music"; matched += 1
    _save_json_map(MB_CACHE_NAME, cache)
    print(f"artists: {looked} looked up, {matched} new matches, {hits} cache hits -> Music")
    return matched + hits


def recategorize_all(all_rows):
    """Recompute `category` for EVERY row, existing and new, on every run.

    Category is derived purely from name/venue/description, all of which are
    already in the CSV, so this is deterministic and needs no network calls.
    Running it over the whole set means rows imported under the old buggy
    inferrer self-heal, and any future tweak to CATEGORY_KEYWORDS reapplies to
    the entire catalogue automatically instead of only to new arrivals."""
    changed = 0
    counts = {}
    for r in all_rows:
        before = (r.get("category") or "").strip()
        after = infer_category(r.get("event_name"), r.get("venue"), "", r.get("description"))
        if after != before:
            r["category"] = after
            changed += 1
        counts[after] = counts.get(after, 0) + 1
    top = ", ".join(f"{c}={n}" for c, n in sorted(counts.items(), key=lambda x: -x[1])[:6])
    print(f"recategorize: {changed} rows changed | {top}")
    return changed


# ---------- URL rescue for date-quarantined records ----------
URL_DATES_NAME = "url_dates.json"
URL_FAILS_NAME = "url_rescue_failures.json"
RESCUE_CAP = 250          # max page fetches per run
RESCUE_DELAY = 0.5

JSONLD_RE = re.compile(r'"startDate"\s*:\s*"([^"]+)"')
TIMEATTR_RE = re.compile(r'<time[^>]+datetime="([^"]+)"', re.I)
META_RE = re.compile(r'<meta[^>]+(?:property|itemprop|name)="[^"]*(?:start_?[Dd]ate|start_?time)[^"]*"[^>]+content="([^"]+)"', re.I)

def _load_json_map(name, default):
    r = requests.get(f"{SUPABASE_URL}/storage/v1/object/{IMPORTS_BUCKET}/{name}", headers=SB)
    if r.status_code != 200: return default
    try: return json.loads(r.content)
    except Exception: return default

def _save_json_map(name, obj):
    requests.post(f"{SUPABASE_URL}/storage/v1/object/{IMPORTS_BUCKET}/{name}",
                  headers={**SB, "Content-Type": "application/json", "x-upsert": "true"},
                  data=json.dumps(obj))

def _date_from_page(html):
    for rx in (JSONLD_RE, TIMEATTR_RE, META_RE):
        for m in rx.finditer(html):
            d, t, off = _parse_date(m.group(1))
            if d: return d, t, off
    return "", "", ""

def rescue_by_url(quarantine):
    """Try to recover date-missing records by reading their source pages."""
    candidates = []
    for q in quarantine:
        if not q["reason"].startswith("missing date"): continue
        try: rec = json.loads(q["raw_full"])
        except Exception: continue
        url = str(_get(rec, "url", "source_url", "link") or "").strip()
        if url.startswith("http"): candidates.append((q, rec, url))
    if not candidates: return [], quarantine

    url_dates = _load_json_map(URL_DATES_NAME, {})
    url_fails = set() if date.today().weekday() == 0 else set(_load_json_map(URL_FAILS_NAME, []))
    rescued_rows, still_quarantined, fetched = [], [], 0
    handled = set()

    for q, rec, url in candidates:
        handled.add(id(q))
        d, t, off = "", "", ""
        if url in url_dates:
            cached = url_dates[url]
            # Entries cached before timezone handling existed are [d, t] with no
            # offset; newer ones are [d, t, off]. Both shapes must load.
            d, t = cached[0], cached[1]
            off = cached[2] if len(cached) > 2 else ""
        elif url in url_fails or fetched >= RESCUE_CAP:
            still_quarantined.append(q); continue
        else:
            fetched += 1
            try:
                pg = requests.get(url, timeout=10,
                                  headers={"User-Agent": "Mozilla/5.0 (GSV3 event pipeline)"})
                if pg.status_code == 200:
                    d, t, off = _date_from_page(pg.text[:400000])
            except Exception:
                pass
            time.sleep(RESCUE_DELAY)
            if d: url_dates[url] = [d, t, off]
            else: url_fails.add(url)
        if not d:
            still_quarantined.append(q); continue
        row = _record_to_row(rec, d, t, off)
        if isinstance(row, dict): rescued_rows.append(row)
        else: still_quarantined.append(q)

    remaining = [q for q in quarantine if id(q) not in handled] + still_quarantined
    _save_json_map(URL_DATES_NAME, url_dates)
    _save_json_map(URL_FAILS_NAME, sorted(url_fails))
    print(f"URL rescue: {len(rescued_rows)} recovered ({fetched} pages fetched), {len(remaining)} still quarantined.")
    return rescued_rows, remaining

# ---------- raw record parsing ----------
def _get(rec, *names):
    """Case/format-tolerant field lookup on a raw record dict."""
    lower = {re.sub(r"[^a-z0-9]", "", k.lower()): v for k, v in rec.items() if isinstance(k, str)}
    for n in names:
        v = lower.get(re.sub(r"[^a-z0-9]", "", n.lower()))
        if v not in (None, "", [], {}): return v
    return ""

LABELS = ("Performers", "Event Dates", "Date & Time", "Price", "Venue", "Location",
          "Contact Information", "Description", "Genres", "Source URL")
LABEL_RE = re.compile(
    r"^(" + "|".join(re.escape(l) for l in LABELS) + r")\s*:[ \t]*\n?((?:.+\n?)*?)(?=\n\s*\n|^(?:" + "|".join(re.escape(l) for l in LABELS) + r")\s*:|\Z)",
    re.I | re.M)
ANY_LABEL_RE = re.compile(r"^(?:" + "|".join(re.escape(l) for l in LABELS) + r")\s*:", re.I | re.M)

def _labels_from_text(text):
    out = {}
    if not isinstance(text, str): return out
    for m in LABEL_RE.finditer(text):
        key = re.sub(r"[^a-z]", "", m.group(1).lower())
        val = re.sub(r"\s+", " ", m.group(2)).strip()
        if val and val.lower() not in ("not provided in source.", "not provided in source"):
            out[key] = val
    return out

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January","February","March","April","May","June","July","August",
     "September","October","November","December"], 1)}
MONTHS.update({m[:3].lower(): i for m, i in [(k.capitalize(), v) for k, v in MONTHS.items()]})
_MON = r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
DATE_PATTERNS = [
    re.compile(r"(\d{4})-(\d{2})-(\d{2})"),                                    # ISO
    re.compile(_MON + r"\.?\s+(\d{1,2})(?!\d)(?:st|nd|rd|th)?(?:\s*[-–]\s*\d{1,2})?(?:\s*,?\s*(\d{4}))?", re.I),  # July 1, 2026 / July 14-16
    re.compile(r"(?<!\d)(\d{1,2})(?:st|nd|rd|th)?\s+" + _MON + r"\.?(?:\s*,?\s*(\d{4}))?", re.I),                  # 1 July 2026
]
TIME_RE = re.compile(r"\b(\d{1,2}):(\d{2})\s*([ap])\.?m?\.?\b|\b(\d{1,2})\s*([ap])\.?m\.?\b|\b([01]?\d|2[0-3]):([0-5]\d)\b", re.I)

def _plausible(y, mo, d):
    try:
        dt = date(y, mo, d)
    except ValueError:
        return None
    today = date.today()
    if (dt - today).days < -1 or (dt - today).days > 1100: return None
    return dt.isoformat()

def _pick_year(mo, d):
    today = date.today()
    for y in (today.year, today.year + 1):
        iso = _plausible(y, mo, d)
        if iso: return iso
    return None


_ORDINAL_WORDS = {}
for i, w in enumerate(["first","second","third","fourth","fifth","sixth","seventh","eighth","ninth","tenth",
    "eleventh","twelfth","thirteenth","fourteenth","fifteenth","sixteenth","seventeenth","eighteenth",
    "nineteenth","twentieth"], 1): _ORDINAL_WORDS[w] = i
for i, w in [(21,"twenty-first"),(22,"twenty-second"),(23,"twenty-third"),(24,"twenty-fourth"),
    (25,"twenty-fifth"),(26,"twenty-sixth"),(27,"twenty-seventh"),(28,"twenty-eighth"),
    (29,"twenty-ninth"),(30,"thirtieth"),(31,"thirty-first")]: _ORDINAL_WORDS[w] = i
_YEAR_WORDS = {"twenty-five":2025,"twenty five":2025,"twenty-six":2026,"twenty six":2026,
               "twenty-seven":2027,"twenty seven":2027,"twenty-eight":2028,"twenty eight":2028}
_SPELLED_RE = re.compile(
    r"\b(" + "|".join(_ORDINAL_WORDS) + r")\s+(?:of\s+)?" + _MON +
    r"(?:\s*,?\s*two\s+thousand\s+(?:and\s+)?(" + "|".join(_YEAR_WORDS) + r"))?", re.I)

def _spelled_date(text):
    m = _SPELLED_RE.search(str(text or "").replace("\u2013", "-"))
    if not m: return ""
    day = _ORDINAL_WORDS.get(m.group(1).lower().replace(" ", "-"))
    mo = MONTHS.get(m.group(2)[:3].lower())
    yw = (m.group(3) or "").lower().replace(" ", "-")
    if not (day and mo): return ""
    if yw: return _plausible(_YEAR_WORDS[yw], mo, day) or ""
    return _pick_year(mo, day) or ""

def _human_date(text):
    if not text: return ""
    s = str(text)
    m = DATE_PATTERNS[0].search(s)
    if m:
        return _plausible(int(m.group(1)), int(m.group(2)), int(m.group(3))) or ""
    m = DATE_PATTERNS[1].search(s)
    if m:
        mo = MONTHS.get(m.group(1)[:3].lower()); d = int(m.group(2)); y = m.group(3)
        if mo: return (_plausible(int(y), mo, d) if y else _pick_year(mo, d)) or ""
    m = DATE_PATTERNS[2].search(s)
    if m:
        d = int(m.group(1)); mo = MONTHS.get(m.group(2)[:3].lower()); y = m.group(3)
        if mo: return (_plausible(int(y), mo, d) if y else _pick_year(mo, d)) or ""
    return _spelled_date(s)

def _human_time(text):
    if not text: return ""
    m = TIME_RE.search(str(text))
    if not m: return ""
    if m.group(1):
        hh, mm, ap = int(m.group(1)), int(m.group(2)), m.group(3).lower()
        if ap == "p" and hh < 12: hh += 12
        if ap == "a" and hh == 12: hh = 0
    elif m.group(4):
        hh, mm, ap = int(m.group(4)), 0, m.group(5).lower()
        if ap == "p" and hh < 12: hh += 12
        if ap == "a" and hh == 12: hh = 0
    else:
        hh, mm = int(m.group(6)), int(m.group(7))
    if hh > 23 or mm > 59: return ""
    return f"{hh:02d}:{mm:02d}"

def _parse_date(val):
    """Return (YYYY-MM-DD, HH:MM, tz_offset) from ISO-ish STRING values only.

    tz_offset is the timezone marker found on the source timestamp: "Z" / "+00:00"
    for UTC, "-07:00" style for a fixed offset, or "" when the source gave a naive
    wall-clock time.

    WHY THIS MATTERS: sources like allevents.in publish event times in UTC. The
    old version of this function matched only the date and HH:MM digits and threw
    the offset away, so a show at 8pm Pacific (2026-07-14T03:00:00Z) was stored as
    "2026-07-14 / 03:00" — wrong day, wrong time. Keeping the offset lets
    localize_times() convert to the venue's actual local clock after geocoding.

    Numeric epoch values are ignored on purpose: the export's epoch field is the
    scrape timestamp, not the event date, and a wrong date is worse than no date."""
    if isinstance(val, list) and val: val = val[0]
    if isinstance(val, dict): val = val.get("start") or val.get("date") or ""
    if isinstance(val, (int, float)): return "", "", ""
    s = str(val or "").strip()
    if re.fullmatch(r"\d{10,16}", s): return "", "", ""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::\d{2})?(?:\.\d+)?\s*(Z|[+-]\d{2}:?\d{2})?", s)
    if m:
        iso = _plausible(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        t = f"{m.group(4)}:{m.group(5)}"
        off = (m.group(6) or "").strip()
        if not iso:
            return "", "", ""
        # A bare 00:00 with NO timezone marker is the classic "date only, no time
        # supplied" case, so it stays blank. But 00:00 WITH an offset (e.g.
        # 2026-07-14T00:00:00Z) is a real instant that localizes to a real
        # evening time, so it must be kept.
        if t == "00:00" and not off:
            return iso, "", ""
        return iso, t, off
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return (_plausible(int(m.group(1)), int(m.group(2)), int(m.group(3))) or ""), "", ""
    return "", "", ""


_TF = TimezoneFinder()
_TZ_CACHE = {}


def _tz_for(lat, lng):
    """Venue-local IANA timezone from coordinates, memoised per run.
    timezonefinder does a point-in-polygon lookup against the tz boundary map,
    so this is offline (no API cost) but not free — hence the cache."""
    try:
        latf, lngf = float(lat), float(lng)
    except (TypeError, ValueError):
        return None
    key = (round(latf, 3), round(lngf, 3))
    if key in _TZ_CACHE:
        return _TZ_CACHE[key]
    name = None
    try:
        name = _TF.timezone_at(lat=latf, lng=lngf)
    except Exception:
        name = None
    zone = None
    if name:
        try:
            zone = ZoneInfo(name)
        except Exception:
            zone = None
    _TZ_CACHE[key] = zone
    return zone


def _offset_to_tzinfo(off):
    """Turn a captured offset marker into a tzinfo. "" means naive."""
    if not off:
        return None
    if off.upper() == "Z":
        return timezone.utc
    m = re.fullmatch(r"([+-])(\d{2}):?(\d{2})", off)
    if not m:
        return None
    sign = 1 if m.group(1) == "+" else -1
    delta = timedelta(hours=int(m.group(2)), minutes=int(m.group(3)))
    return timezone(sign * delta)


def localize_times(all_rows):
    """Convert absolute (offset-bearing) source timestamps into the venue's local
    wall clock. Runs AFTER geocoding, because the venue's timezone comes from its
    coordinates.

    Rows are only touched when we have all three of: a stored offset, a real
    date+time, and coordinates. Anything else is left exactly as-is — a wrong
    time is worse than an unconverted one.

    tz_offset is a working field only; publish_json/push_csv write APP_COLUMNS,
    so it never reaches events.json or the CSV."""
    converted = skipped = 0
    for r in all_rows:
        off = (r.get("tz_offset") or "").strip()
        if not off:
            continue
        d = (r.get("date") or "").strip()
        t = (r.get("start_time") or "").strip()
        if not d or not re.fullmatch(r"\d{2}:\d{2}", t):
            continue
        src_tz = _offset_to_tzinfo(off)
        if src_tz is None:
            continue
        zone = _tz_for(r.get("venue_lat"), r.get("venue_lng"))
        if zone is None:
            skipped += 1
            continue
        try:
            aware = datetime.fromisoformat(f"{d}T{t}:00").replace(tzinfo=src_tz)
        except ValueError:
            continue
        local = aware.astimezone(zone)
        r["date"] = local.date().isoformat()
        r["start_time"] = local.strftime("%H:%M")
        r["tz_offset"] = ""  # converted; don't double-convert on a later run
        converted += 1
    print(f"localize: converted={converted} no_tz_for_coords={skipped}")
    return converted


def _record_to_row(rec, d, t, off=""):
    labels = _labels_from_text(_get(rec, "claim", "text", "content", "body", "description"))
    name = str(_get(rec, "event_name", "event_title", "name", "title", "event") or "").strip()
    if not name or not d: return None
    probe_desc = str(_get(rec, "description", "claim", "text", "content", "body") or "")
    if looks_like_non_event(name, probe_desc[:1200]): return "NON_EVENT"
    venue = str(_get(rec, "venue", "venue_name") or labels.get("venue", "")).strip()
    addr = str(_get(rec, "venue_address", "address", "location") or labels.get("location", "")).strip()
    url = str(_get(rec, "url", "source_url", "link") or labels.get("sourceurl", "")).strip()
    price = str(_get(rec, "price") or labels.get("price", "")).strip()
    raw_desc = str(_get(rec, "description") or "").strip()
    desc = labels.get("description", "").strip() or ("" if ANY_LABEL_RE.search(raw_desc) else raw_desc)
    genres = str(_get(rec, "genres", "genre") or labels.get("genres", "")).strip()
    # Deliberately IGNORE any category the source supplied. Feeds come from
    # ~1,700 domains with their own taxonomies, and taking theirs verbatim is
    # what filed Jimmy Eat World under Theater and Young the Giant under Family.
    # One inferrer, one taxonomy, consistent everywhere.
    cat = infer_category(name, venue, genres, desc)
    # A start_time supplied as its own field is a local wall clock already, so
    # it carries no offset. Only the timestamp parsed by _parse_date does.
    explicit = str(_get(rec, "start_time", "time") or "").strip()
    stime = explicit or t
    row_off = "" if explicit else off
    if not venue and not addr: return "NO_VENUE"
    row = blank_row()
    row.update({"date": d, "start_time": stime, "event_name": name, "venue": venue,
                "venue_address": addr, "url": url, "category": cat,
                "price": price, "description": desc, "tz_offset": row_off})
    return row

def parse_import_records(lines, blocked_domains=None):
    blocked_domains = blocked_domains or set()
    rows, quarantine = [], []
    for ln in lines:
        ln = ln.strip()
        if not ln: continue
        try:
            rec = json.loads(ln)
        except Exception:
            quarantine.append({"reason": "bad json", "raw": ln[:300], "raw_full": ""}); continue
        if not isinstance(rec, dict):
            quarantine.append({"reason": "not an object", "raw": str(rec)[:300], "raw_full": ""}); continue
        rec_url = str(_get(rec, "url", "source_url", "link") or "")
        if _domain(rec_url) in blocked_domains:
            full = json.dumps(rec, default=str)
            quarantine.append({"reason": "blocked domain", "raw": full[:300], "raw_full": full})
            continue
        labels = _labels_from_text(_get(rec, "claim", "text", "content", "body", "description"))
        name = str(_get(rec, "event_name", "event_title", "name", "title", "event") or "").strip()
        d, t, off = _parse_date(_get(rec, "event_dates", "date", "start_date", "start", "datetime"))
        if not d:
            srcv = labels.get("datetime") or labels.get("eventdates") or ""
            d = _human_date(srcv)
            t = t or _human_time(srcv)
            off = ""  # human-readable strings are local wall clock, never UTC
        row = _record_to_row(rec, d, t, off) if (name and d) else None
        if isinstance(row, dict):
            rows.append(row)
        else:
            full = json.dumps(rec, default=str)
            reason = ("suspected non-event (hoax/rumor language)" if row == "NON_EVENT"
                      else "no venue or location" if row == "NO_VENUE"
                      else f"missing {'name' if not name else 'date'}")
            quarantine.append({"reason": reason, "raw": full[:300], "raw_full": full})
    return rows, quarantine


# ---------- second-pass location verification ----------
from urllib.parse import urlparse, urljoin

TICKET_DOMAINS = ("ticketmaster.", "eventbrite.", "dice.fm", "axs.com", "seetickets.",
                  "etix.com", "ticketweb.", "bandsintown.", "songkick.", "tixr.com",
                  "showclix.", "universe.com", "ticketfly.", "eventix.", "billetto.",
                  "skiddle.", "wegottickets.", "ticketsource.", "prekindle.", "seatgeek.")
VERIFY_CAP = 150
HREF_RE = re.compile(r'href="(https?://[^"]+)"', re.I)
JSONLD_BLOCK_RE = re.compile(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', re.I | re.S)

def _domain(url):
    try:
        d = urlparse(url).netloc.lower()
        return d[4:] if d.startswith("www.") else d
    except Exception:
        return ""


# ---------- Tier 1: coordinates published by the source itself ----------
GEO_JSONLD_RE = re.compile(r'"latitude"\s*:\s*"?(-?\d{1,3}\.\d+)"?\s*,\s*"longitude"\s*:\s*"?(-?\d{1,3}\.\d+)"?')
GMAPS_PB_RE = re.compile(r"google\.[a-z.]+/maps/embed\?pb=[^\"\x27]*!2d(-?\d{1,3}\.\d+)![^\"\x27]*?3d(-?\d{1,3}\.\d+)")
GMAPS_Q_RE = re.compile(r"maps\.google\.[a-z.]+/[^\"\x27]*?[?&](?:q|ll)=(-?\d{1,3}\.\d+),(-?\d{1,3}\.\d+)")
GMAPS_AT_RE = re.compile(r"google\.[a-z.]+/maps/[^\"\x27]*?@(-?\d{1,3}\.\d+),(-?\d{1,3}\.\d+)")
OSM_RE = re.compile(r"openstreetmap\.org/[^\"\x27]*?mlat=(-?\d{1,3}\.\d+)&(?:amp;)?mlon=(-?\d{1,3}\.\d+)")

def _valid_latlng(lat, lng):
    try:
        lat, lng = float(lat), float(lng)
    except (TypeError, ValueError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lng <= 180): return None
    if abs(lat) < 0.01 and abs(lng) < 0.01: return None   # null island
    return lat, lng

def _coords_from_page(html):
    """Extract coordinates the source itself published. Trust order:
    schema.org GeoCoordinates in JSON-LD, then Google Maps embeds, then OSM embeds.
    JSON-LD is lat,lng; the Google pb format is !2d=LNG !3d=LAT (swapped)."""
    m = GEO_JSONLD_RE.search(html)
    if m:
        v = _valid_latlng(m.group(1), m.group(2))
        if v: return v
    m = GMAPS_PB_RE.search(html)
    if m:
        v = _valid_latlng(m.group(2), m.group(1))   # pb: 2d is longitude, 3d is latitude
        if v: return v
    for rx in (GMAPS_Q_RE, GMAPS_AT_RE, OSM_RE):
        m = rx.search(html)
        if m:
            v = _valid_latlng(m.group(1), m.group(2))
            if v: return v
    return None

def _locality_from_jsonld(html):
    m = re.search(r'"addressLocality"\s*:\s*"([^"]{2,60})"', html)
    return m.group(1).strip() if m else ""

def _addr_from_jsonld(html):
    """STRICT: build an address only from schema.org structured location data.
    Returns (venue_name, address_string) or ('','')."""
    for m in JSONLD_BLOCK_RE.finditer(html):
        try:
            data = json.loads(m.group(1).strip())
        except Exception:
            continue
        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list): stack.extend(node); continue
            if not isinstance(node, dict): continue
            loc = node.get("location")
            if loc:
                stack_loc = loc if isinstance(loc, list) else [loc]
                for pl in stack_loc:
                    if not isinstance(pl, dict): continue
                    a = pl.get("address")
                    if isinstance(a, str) and len(a.strip()) > 8:
                        return str(pl.get("name") or "").strip(), a.strip()
                    if isinstance(a, dict):
                        parts = [a.get("streetAddress"), a.get("addressLocality"),
                                 a.get("addressRegion"), a.get("postalCode"), a.get("addressCountry")]
                        parts = [str(x).strip() for x in parts if x and str(x).strip()]
                        locality_bits = sum(1 for x in [a.get("addressLocality"), a.get("addressRegion"), a.get("addressCountry")] if x)
                        if parts and locality_bits >= 1 and (a.get("streetAddress") or locality_bits >= 2):
                            return str(pl.get("name") or "").strip(), ", ".join(parts)
            for v in node.values():
                if isinstance(v, (dict, list)): stack.append(v)
    return "", ""

def _fetch_page(url):
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0 (GSV3 event pipeline)"})
        return r.text[:500000] if r.status_code == 200 else ""
    except Exception:
        return ""

def verify_unpinnable(all_rows):
    """Escalating verification for rows with no usable address:
    1. source page structured markup -> address
    2. ticketing link on source page -> ticket page structured markup -> address
    Verified addresses flow into the normal (validated) geocode step this same run.
    Everything still unresolved is preserved and reported, never dropped."""
    verified_map = _load_json_map("verified_locations.json", {})
    fails = set() if date.today().weekday() == 0 else set(_load_json_map("verify_failures.json", []))
    candidates = []
    for r in all_rows:
        if (r.get("venue_lat") or "").strip(): continue
        addr = (r.get("venue_address") or "").strip()
        if addr and norm_text(addr) != norm_text(r.get("venue")): continue
        url = (r.get("url") or "").strip()
        if url.startswith("http"): candidates.append((r, url))
    if not candidates:
        print("Verification: no unpinnable rows with URLs."); return

    def apply_found(r, found):
        vname, vaddr, lat, lng, city = (found + ["", "", "", "", ""])[:5]
        applied = False
        if lat and lng and _valid_latlng(lat, lng):
            guard_text = " ".join(x for x in [r.get("venue_address"), vaddr] if x)
            if not coords_contradict_text(lat, lng, guard_text):
                r["venue_lat"], r["venue_lng"] = str(lat), str(lng)
                r["venue_map_status"] = "show"
                if city and not (r.get("region") or "").strip(): r["region"] = city
                applied = True
        if vaddr and not applied:
            r["venue_address"] = vaddr; applied = True
        if vaddr and not (r.get("venue_address") or "").strip(): r["venue_address"] = vaddr
        if vname and not (r.get("venue") or "").strip(): r["venue"] = vname
        return applied

    fetched, recovered = 0, 0
    for r, url in candidates:
        if url in verified_map:
            if apply_found(r, list(verified_map[url])): recovered += 1
            continue
        if url in fails or fetched >= VERIFY_CAP: continue
        fetched += 1
        found = None
        html = _fetch_page(url)
        pages = [html] if html else []
        if html:
            links = [h for h in HREF_RE.findall(html) if any(td in h.lower() for td in TICKET_DOMAINS)]
            if links:
                thtml = _fetch_page(urljoin(url, links[0]))
                if thtml: pages.append(thtml)
        for pg in pages:
            vname, vaddr = _addr_from_jsonld(pg)
            coords = _coords_from_page(pg)
            city = _locality_from_jsonld(pg)
            if coords or vaddr:
                lat, lng = coords if coords else ("", "")
                found = [vname, vaddr, str(lat), str(lng), city]
                break
        time.sleep(RESCUE_DELAY)
        if found:
            verified_map[url] = found
            if apply_found(r, found): recovered += 1
        else:
            fails.add(url)
    _save_json_map("verified_locations.json", verified_map)
    _save_json_map("verify_failures.json", sorted(fails))
    still = [r for r, _ in candidates if not (r.get("venue_address") or "").strip()
             or norm_text(r.get("venue_address")) == norm_text(r.get("venue"))]
    buf = io.StringIO()
    w = csv.writer(buf); w.writerow(["date", "event_name", "venue", "url"])
    for r in still: w.writerow([r.get("date"), r.get("event_name"), r.get("venue"), r.get("url")])
    requests.post(f"{SUPABASE_URL}/storage/v1/object/{IMPORTS_BUCKET}/needs_verification.csv",
                  headers={**SB, "Content-Type": "text/csv", "x-upsert": "true"},
                  data=buf.getvalue().encode())
    print(f"Verification: {recovered} addresses recovered ({fetched} fetched), {len(still)} held in needs_verification.csv.")

# ---------- domain reputation ----------
BLOCK_MIN_RECORDS = 20
BLOCK_BAD_RATIO = 0.5

def update_domain_stats(rows, quarantine, kept_ids=None):
    stats = _load_json_map("domain_stats_v2.json", {})
    def bump(dom, field):
        if not dom: return
        s = stats.setdefault(dom, {"total": 0, "fake": 0, "unpinned": 0})
        s["total"] += 1; s[field] = s.get(field, 0) + (1 if field != "total" else 0)
    for q in quarantine:
        try: rec = json.loads(q.get("raw_full") or "{}")
        except Exception: rec = {}
        dom = _domain(str(_get(rec, "url", "source_url", "link") or ""))
        bump(dom, "fake" if q["reason"].startswith(("suspected non-event", "blocked domain")) else "unpinned")
    for r in rows:
        dom = _domain(r.get("url") or "")
        in_dataset = kept_ids is None or id(r) in kept_ids
        pinned = bool((r.get("venue_lat") or "").strip())
        bump(dom, "unpinned" if (in_dataset and not pinned) else "total")
    blocked = sorted(d for d, s in stats.items()
                     if s["total"] >= BLOCK_MIN_RECORDS
                     and s.get("fake", 0) / s["total"] > BLOCK_BAD_RATIO)
    _save_json_map("domain_stats_v2.json", stats)
    _save_json_map("blocked_domains_v2.json", blocked)
    buf = io.StringIO()
    w = csv.writer(buf); w.writerow(["domain", "total_records", "fake_or_rumor", "unpinnable", "bad_ratio", "blocked"])
    for d, s in sorted(stats.items(), key=lambda kv: -((kv[1].get("fake",0)+kv[1].get("unpinned",0)) / max(kv[1]["total"],1))):
        bad = s.get("fake", 0) + s.get("unpinned", 0)
        w.writerow([d, s["total"], s.get("fake", 0), s.get("unpinned", 0),
                    f"{bad/max(s['total'],1):.2f}", "YES" if d in blocked else ""])
    requests.post(f"{SUPABASE_URL}/storage/v1/object/{IMPORTS_BUCKET}/domain_report.csv",
                  headers={**SB, "Content-Type": "text/csv", "x-upsert": "true"},
                  data=buf.getvalue().encode())
    if blocked: print(f"Blocked domains ({len(blocked)}): {', '.join(blocked[:10])}{'...' if len(blocked)>10 else ''}")
    print(f"Domain report: {len(stats)} domains tracked -> imports/domain_report.csv")

# ---------- steps ----------
def fetch_import():  # returns (rows, quarantine)
    r = requests.get(f"{SUPABASE_URL}/storage/v1/object/{IMPORTS_BUCKET}/{IMPORT_NAME}", headers=SB)
    if r.status_code == 404 or (r.status_code == 400 and b"not_found" in r.content.lower()):
        print("No import file found - refresh-only run."); return [], []
    r.raise_for_status()
    data = r.content
    try:
        data = gzip.decompress(data)
    except OSError:
        pass  # already plain text
    blocked = set(_load_json_map("blocked_domains_v2.json", []))
    rows, quarantine = parse_import_records(data.decode("utf-8", errors="replace").splitlines(), blocked)
    print(f"Import: {len(rows)} events parsed, {len(quarantine)} quarantined before rescue.")
    rescued, quarantine = rescue_by_url(quarantine)
    rows.extend(rescued)
    if quarantine:
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=["reason", "raw"], extrasaction="ignore"); w.writeheader(); w.writerows(quarantine)
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

def _dedup_name(s):
    """Name normalization for the dedup key ONLY. norm_text turns a straight
    apostrophe into a space but silently deletes a curly one, so "People's" and
    "People’s" produced different keys and both survived. Joining without
    spaces fixes that and also folds "SummerMaxxing" / "Summer Maxxing".
    Trailing generic words are stripped so "America's Block Party" and
    "America's Block Party Concert" collapse to one listing."""
    toks = norm_text(s).split()
    while toks and toks[-1] in {"concert", "concerts", "tickets", "show", "event", "live"}:
        toks.pop()
    return "".join(toks)

def dedup(existing, new):
    key = lambda r: (r.get("date","").strip(), r.get("start_time","").strip(), _dedup_name(r.get("event_name")))
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

FAILURES_NAME = "geocode_failures_v3.json"

def load_failures():
    r = requests.get(f"{SUPABASE_URL}/storage/v1/object/{IMPORTS_BUCKET}/{FAILURES_NAME}", headers=SB)
    if r.status_code != 200: return set()
    try: return set(json.loads(r.content))
    except Exception: return set()

def save_failures(keys):
    requests.post(f"{SUPABASE_URL}/storage/v1/object/{IMPORTS_BUCKET}/{FAILURES_NAME}",
                  headers={**SB, "Content-Type": "application/json", "x-upsert": "true"},
                  data=json.dumps(sorted(keys)))

def audit_existing(all_rows, cache):
    """Quality sweep of everything already in the dataset:
    - drop rows that read as hoax/rumor stories rather than events
    - drop rows with no venue and no address (unlocatable list noise)
    - clear coordinates that contradict the row's own address text when the
      cached venue-name entry (the coords' likely source) disagrees with it
    Cleared rows re-enter the normal geocode queue with their full address."""
    kept, dropped_fake, dropped_novenue, cleared = [], 0, 0, 0
    for r in all_rows:
        if looks_like_non_event(r.get("event_name",""), r.get("description","")):
            dropped_fake += 1; continue
        if not (r.get("venue") or "").strip() and not (r.get("venue_address") or "").strip():
            dropped_novenue += 1; continue
        addr = r.get("venue_address") or ""
        if (r.get("venue_lat") or "").strip() and addr.strip():
            bad = coords_contradict_text(r["venue_lat"], r["venue_lng"], addr)
            if not bad:
                entry = cache.get(venue_key(r.get("venue")))
                if entry:
                    try:
                        same = (abs(float(entry["lat"]) - float(r["venue_lat"])) < 0.02 and
                                abs(float(entry["lng"]) - float(r["venue_lng"])) < 0.02)
                    except (ValueError, TypeError, KeyError):
                        same = False
                    bad = same and not entry_matches_text(entry, addr)
            if bad:
                r["venue_lat"] = ""; r["venue_lng"] = ""
                r["venue_map_status"] = ""; r["region"] = ""
                cleared += 1
        kept.append(r)
    print(f"Audit: dropped {dropped_fake} suspected non-events, {dropped_novenue} venue-less rows, "
          f"cleared {cleared} inconsistent geocodes for re-geocoding.")
    return kept

def geocode(all_rows, cache, cmap):
    v2 = _load_json_map("geo_cache_v2.json", {})   # precise cache: venue|address -> entry

    def lookup(r):
        addr = (r.get("venue_address") or "").strip()
        if not addr: return None   # no address = unverifiable, never pin
        k_full = venue_key(r.get("venue"), addr)
        hit = v2.get(k_full) or cache.get(k_full)
        if hit and entry_matches_text(hit, addr): return hit
        hit = cache.get(venue_key(r.get("venue")))
        if hit and entry_matches_text(hit, addr): return hit
        return None

    hits = 0
    for r in all_rows:
        if (r.get("venue_lat") or "").strip() and (r.get("venue_lng") or "").strip(): continue
        h = lookup(r)
        if h: apply_coords(r, h); hits += 1

    uniq, no_addr = {}, 0
    for r in all_rows:
        if (r.get("venue_lat") or "").strip(): continue
        addr = (r.get("venue_address") or "").strip()
        if not addr or norm_text(addr) == norm_text(r.get("venue")):
            no_addr += 1; continue   # never geocode a bare venue name
        if (r.get("venue") or addr).strip():
            uniq.setdefault(venue_key(r.get("venue"), addr), r)

    known_failed = set() if date.today().weekday() == 0 else load_failures()
    skipped_failed = sum(1 for k in uniq if k in known_failed)
    todo = [(k, r) for k, r in uniq.items() if k not in known_failed][:DAILY_GEOCODE_LIMIT]
    print(f"Cache hits: {hits}. To geocode: {len(uniq)} (running {len(todo)}, "
          f"skipping {skipped_failed} known-failed, {no_addr} rows lack an address -> unmapped).")

    def liq(q):
        g = requests.get("https://us1.locationiq.com/v1/search",
                         params={"key": LOCATIONIQ_KEY, "q": q[:250], "format": "json",
                                 "limit": 1, "addressdetails": 1}, timeout=15)
        if g.status_code == 429:
            time.sleep(2.5)
            g = requests.get("https://us1.locationiq.com/v1/search",
                             params={"key": LOCATIONIQ_KEY, "q": q[:250], "format": "json",
                                     "limit": 1, "addressdetails": 1}, timeout=15)
        return g

    COARSE_TYPES = {"country", "state", "county", "region", "province", "island", "archipelago"}
    def entry_of(g):
        if g.status_code != 200 or not g.json(): return None
        top = g.json()[0]; a = top.get("address", {})
        if str(top.get("type", "")).lower() in COARSE_TYPES:
            return None   # a state/county centroid is never a venue location
        city = a.get("city") or a.get("town") or a.get("village") or a.get("suburb") or ""
        return {"lat": top["lat"], "lng": top["lon"], "city": city,
                "state": a.get("state",""), "country": a.get("country","")}, top, a

    geocoded, failed, rejected, writeback = 0, 0, 0, []
    new_failed = set()
    for k, r in todo:
        addr = (r.get("venue_address") or "").strip()
        venue = (r.get("venue") or "").strip()
        queries = []
        if venue and addr: queries.append(f"{venue}, {addr}")
        if addr: queries.append(addr)   # address alone: geocoders are best at this
        accepted, last_status, saw_reject = None, None, False
        try:
            for q in queries:
                g = liq(q); last_status = g.status_code
                res = entry_of(g)
                time.sleep(1.1)
                if not res: continue
                e, top, a = res
                if entry_matches_text(e, addr) and not coords_contradict_text(e["lat"], e["lng"], addr):
                    accepted = (e, top, a); break
                saw_reject = True
            if accepted:
                e, top, a = accepted
                v2[k] = e; cache[k] = e
                wb = {cmap["venue"]: venue, cmap["lat"]: float(top["lat"]), cmap["lng"]: float(top["lon"])}
                if cmap["addr"]: wb[cmap["addr"]] = addr
                if cmap["city"]: wb[cmap["city"]] = e["city"]
                if cmap["state"]: wb[cmap["state"]] = a.get("state","")
                if cmap["country"]: wb[cmap["country"]] = a.get("country","")
                writeback.append(wb); geocoded += 1
            elif saw_reject:
                rejected += 1; new_failed.add(k)
            else:
                if failed == 0:
                    print(f"  First geocode failure: HTTP {last_status}")
                failed += 1
                if last_status == 404: new_failed.add(k)
        except Exception as ex:
            if failed == 0:
                print(f"  First geocode failure: {ex}")
            failed += 1
            time.sleep(1.1)

    for r in all_rows:
        if (r.get("venue_lat") or "").strip(): continue
        h = lookup(r)
        if h: apply_coords(r, h)

    if new_failed or known_failed:
        save_failures(known_failed | new_failed)
    _save_json_map("geo_cache_v2.json", v2)
    seen_wb = {}
    for wb in writeback: seen_wb[wb[cmap["venue"]]] = wb
    writeback = list(seen_wb.values())
    for j in range(0, len(writeback), 200):
        w = requests.post(f"{SUPABASE_URL}/rest/v1/{CACHE_TABLE}?on_conflict={cmap['venue']}",
                          headers={**SB, "Content-Type": "application/json",
                                   "Prefer": "return=minimal,resolution=merge-duplicates"},
                          data=json.dumps(writeback[j:j+200]))
        if w.status_code >= 300: print(f"Cache write-back warning ({w.status_code}): {w.text[:200]}")
    print(f"Geocoded {geocoded} new (failed: {failed}, rejected as inconsistent: {rejected}).")
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
    body = {"message": f"daily pipeline: {len(all_rows)} events ({datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")}Z)",
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
    all_rows = audit_existing(all_rows, cache)
    verify_unpinnable(all_rows)
    geocoded, failed = geocode(all_rows, cache, cmap)
    # Must run AFTER geocode (needs venue coords) and BEFORE publish/push, so the
    # local wall clock is what lands in events.json and the CSV.
    localize_times(all_rows)
    # Recompute categories across the whole set so old rows heal too, then
    # recover artist-name-only concerts via MusicBrainz (cached, capped).
    recategorize_all(all_rows)
    enrich_artists(all_rows)
    upcoming = publish_json(all_rows)
    push_csv(all_rows, sha)
    update_domain_stats(new_rows, quarantine, {id(r) for r in all_rows})
    print("=" * 40)
    print(f"DONE. total={len(all_rows)} upcoming={upcoming} imported={len(new_rows)} "
          f"dupes_folded={dropped} quarantined={len(quarantine)} geocoded_new={geocoded} geocode_failed={failed}")

if __name__ == "__main__":
    main()
