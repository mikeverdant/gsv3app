#!/usr/bin/env python3
"""
eval_categories.py — PROVE THE PROMPT. WRITES NOTHING, ANYWHERE.

Reads the live events.json, classifies a sample, prints a table. It does not
touch the CSV, the Supabase tables, the cache, or the pipeline. Run it, read the
output, change the prompt, run it again. When the numbers are good, and only
then, the same PROMPT text moves into pipeline.py.

Two parts:
  GOLDEN  - hand-built cases with known right answers, including every case we
            argued through. Scored automatically. This is the regression net.
  SAMPLE  - real rows currently sitting in "Event", printed for eyeballing.
            No right answer exists for these yet; that is the point of looking.

Cost: about 2 cents.
"""

import json
import os
import re
import sys
import time
import requests

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY")
if not ANTHROPIC_KEY:
    sys.exit("Missing secret: ANTHROPIC_API_KEY (add it in repo Settings -> Secrets -> Actions)")

EVENTS_JSON = "https://hmluygfhvdegmealscky.supabase.co/storage/v1/object/public/events/events.json"
MODEL = "claude-haiku-4-5-20251001"
BATCH = 20
SAMPLE_N = int(os.environ.get("SAMPLE_N", "120"))

# Confidence below this becomes "Event" in the real pipeline. Applied by OUR
# code, never by the model. Cached confidence means this is retunable for free.
CONF_FLOOR = float(os.environ.get("CONF_FLOOR", "0.6"))

CATEGORIES = [
    "Music", "Comedy", "Theater", "Film", "Arts", "Dance", "Talks",
    "Food & Drink", "Markets", "Nightlife", "Community", "Family",
    "Sports", "Wellness", "Social",
]

SYSTEM = """You classify live events for a discovery app. People use it to decide what to go do.

Work through these steps for each event. Do not skip to the label.

STEP 1 - IS IT AN EVENT?
An event is something happening at a time and place that people gather for.
NOT events: gift cards, parking passes, camping passes, shuttles or transport to
an event, merchandise. Set is_event false for these.
Private, invite-only, members-only and sold-out things ARE events. Being hard to
get into is not the same as not existing. Never exclude them.

STEP 2 - STRIP THE VENUE'S ORDINARY BUSINESS
What a venue normally does is never part of the event and never a category.
A pub pours beer. A restaurant serves dinner. A brewery is open. A comedy club
usually books comedy. A theater usually books touring acts. None of that tells
you what THIS event is. A promoter or charity renting a comedy club for a
fundraiser is running a fundraiser, not comedy.
Food or drink is a category ONLY when it IS the event (a tasting, a beer
festival, a food festival) or is bundled into the price (a brunch with a show).
Food you could simply decline and still attend is the venue, not the event.

STEP 3 - NAME THE OFFERING
In a few words, what is the attendee going FOR? Not everything mentioned in the
text. The thing itself.

STEP 4 - PRIMARY IS WHAT SURVIVES CANCELLATION
If two things are genuinely bundled, the primary is the one that still happens
if the other is cancelled:
- Cancel the drag show: brunch is still served -> Food & Drink primary
- Cancel the dinner: the play still runs -> Theater primary
- Cancel the headliner: the gala still happens -> Community primary
- Cancel the band at the farmers market: the market still happens -> Markets primary
- Cancel the band at the pub: there is no event at all -> Music primary

STEP 5 - SECONDARY IS RARE. MOST EVENTS HAVE NONE.
Add a secondary ONLY if you can name a concrete element inside the event in a
few words: "a drag show during the brunch", "Cypress Hill headlining".
If you cannot name the element, there is no secondary. Leave both fields "".
The venue's name, the venue's usual programming, the neighbourhood, and the mood
are NEVER elements. Do not fill the field just because it exists.

DISTRACTORS - THE OFFERING DECIDES, MODIFIERS DESCRIBE
One word never flips a category. "A hilarious night with the Grateful Dead" is
Music: the offering is the band's set and "hilarious" is a modifier.
Descriptions are auto-generated and usually restate the name and venue with
filler like "promises an unforgettable experience". Ignore filler. The words
perform, live, show, experience, celebration mean nothing on their own.

PERFORMERS
Use your knowledge of real acts. A named band, DJ, singer, rapper, or orchestra
playing a set is Music. A named comedian is Comedy. A named touring production
is Theater.
A tribute or cover act is still Music: the offering is a music performance, even
though the performer is not the artist named.
A band being MENTIONED is not that band performing. "A DJ playing tributes to X,
Y and Z" is a DJ set, not those bands.

CONFIDENCE (0.0 to 1.0, how sure you are of the PRIMARY)
Be near-certain about WHAT THE PERFORMANCE IS. That is a single point of
failure: a wrong or missing label makes the event unfindable, so refusing costs
exactly as much as being wrong. If you can identify the performance type,
COMMIT. Do not hedge.
Be relaxed about ORDERING two bundled elements. Both get surfaced either way, so
a close call there is cheap.
Low confidence is for when you genuinely cannot tell what the event is, not for
when you are choosing between two reasonable answers.

CATEGORIES - pick exactly one primary. There is no "unknown" or "other" option.
Music        - a musical performance is the offering: concerts, gigs, DJ sets, tribute acts, orchestras, music festivals, music open mics
Comedy       - stand-up, improv, sketch, a comedian's set
Theater      - plays, musicals, opera, drag shows, burlesque, stage productions
Film         - screenings, movie nights, film festivals
Arts         - gallery shows, exhibitions, art walks, poetry, literary readings
Dance        - social dance nights, dance classes, ballet, dance performances
Talks        - lectures, panels, author talks, conferences, seminars, workshops, classes
Food & Drink - food or drink IS the event: tastings, beer and food festivals, bundled meals
Markets      - farmers markets, craft fairs, flea markets, vendor markets, swap meets
Nightlife    - club nights, raves, late-night parties where the scene itself is the draw
Community    - fundraisers, galas, parades, civic and cultural festivals, pride, neighbourhood events
Family       - programming aimed at children: storytimes, playgroups, kids' activities
Sports       - games, races, tournaments, matches, rodeos
Wellness     - yoga, meditation, sound baths, fitness
Social       - the offering is being among people or meeting them: trivia nights, meetups, game nights, speed dating, singles nights, mixers, social clubs

OUTPUT
A JSON array only. One object per input event, same order, no prose, no fences:
[{"i":1,"is_event":true,"offering":"the band's set","primary":"Music","secondary":"","secondary_element":"","confidence":0.95}]"""

# Few-shot examples deliberately CARRY their distractors, because clean examples
# do not teach a model to ignore noise; examples showing noise being ignored do.
EXAMPLES_USER = """Classify these events:

1. NAME: The Grateful Dead
   VENUE: Star Theater
   ABOUT: A hilarious and unforgettable night as the Grateful Dead perform live at the Star Theater. Food and drink available.

2. NAME: Drag Brunch
   VENUE: Lips SF
   ABOUT: Bottomless mimosas and a full brunch menu, with a drag show performed between courses. $45 includes brunch.

3. NAME: Boys & Girls Club Annual Fundraiser
   VENUE: Cobb's Comedy Club
   ABOUT: An evening supporting Boys & Girls Club of America programs for local kids. Silent auction and dinner.

4. NAME: Trivia Night
   VENUE: Fort Point Brewery
   ABOUT: Weekly pub trivia. Teams of up to six. Beer and food available at the bar.

5. NAME: Puppy Pool Party
   VENUE: Southeast Community Center
   ABOUT: Puppy Pool Party is an event at Southeast Community Center promising an unforgettable experience.

6. NAME: Winthrop R&B Festival - PARKING PASS
   VENUE: Winthrop Fairgrounds
   ABOUT: Parking pass for the Winthrop R&B Festival.

7. NAME: Saturday Farmers Market
   VENUE: Laurelhurst Park
   ABOUT: Local produce, bread and flowers every Saturday morning, with live music from the Cedar Ridge Band on the lawn.

8. NAME: 90s Night: A Tribute to Nirvana, Pearl Jam and Soundgarden
   VENUE: The Independent
   ABOUT: DJ Rick spins the grunge era all night long.

9. NAME: Cheekface
   VENUE: Kilby Court
   ABOUT: 

10. NAME: Sewing Club
   VENUE: Tesco Extra Bulwell
   ABOUT: Come learn, improve your skills, or just socialize while you sew."""

EXAMPLES_ASSISTANT = """[{"i":1,"is_event":true,"offering":"the band's set","primary":"Music","secondary":"","secondary_element":"","confidence":0.97},
{"i":2,"is_event":true,"offering":"a brunch with a drag show","primary":"Food & Drink","secondary":"Theater","secondary_element":"a drag show between courses","confidence":0.8},
{"i":3,"is_event":true,"offering":"a charity fundraiser","primary":"Community","secondary":"","secondary_element":"","confidence":0.95},
{"i":4,"is_event":true,"offering":"pub trivia","primary":"Social","secondary":"","secondary_element":"","confidence":0.93},
{"i":5,"is_event":true,"offering":"unclear","primary":"Social","secondary":"","secondary_element":"","confidence":0.2},
{"i":6,"is_event":false,"offering":"a parking pass","primary":"Community","secondary":"","secondary_element":"","confidence":0.0},
{"i":7,"is_event":true,"offering":"a farmers market","primary":"Markets","secondary":"Music","secondary_element":"the Cedar Ridge Band playing on the lawn","confidence":0.95},
{"i":8,"is_event":true,"offering":"a DJ set of 90s grunge","primary":"Music","secondary":"","secondary_element":"","confidence":0.92},
{"i":9,"is_event":true,"offering":"Cheekface playing a set","primary":"Music","secondary":"","secondary_element":"","confidence":0.9},
{"i":10,"is_event":true,"offering":"a sewing club meetup","primary":"Social","secondary":"","secondary_element":"","confidence":0.75}]"""

# ---------------------------------------------------------------------------
# GOLDEN SET: every case we reasoned through, plus the ones that burned us.
# (name, venue, description, expected_primary, expected_is_event)
# expected_primary None means "anything, but confidence must be BELOW the floor"
GOLDEN = [
    ("Vince Gill: 50 Years From Home", "Au-Rene Theater", "", "Music", True),
    ("Sara Bareilles", "Bill Graham Civic Auditorium", "An evening with Sara Bareilles.", "Music", True),
    ("Alter Bridge with Big Wreck", "Roseland Theater", "A concert featuring Alter Bridge alongside Big Wreck at the Roseland Theater in Portland.", "Music", True),
    ("Luenell", "Cobb's Comedy Club", "Experience the magic of Luenell at Cobb's Comedy Club.", "Comedy", True),
    ("Aida Rodriguez", "The Main Room", "Tonight at the Improv featuring Aida Rodriguez.", "Comedy", True),
    ("Boosie & Webbie", "Winspear Opera House", "Join Boosie & Webbie for a celebration.", "Music", True),
    ("Mean Girls the Musical", "Gerry Frank Amphitheater", "The hit Broadway musical comes to town.", "Theater", True),
    ("State Ballet of Georgia - Swan Lake", "London Coliseum", "", "Dance", True),
    ("Baby Storytime", "Park Hill Branch Library", "Stories and songs for babies and their caregivers.", "Family", True),
    ("Port Townsend Farmers Market", "Downtown Port Townsend", "Local organic produce, accompanied by live music.", "Markets", True),
    ("Chalant Matcha Pop-Up", "Paper Son Coffee", "A matcha pop-up at Paper Son Coffee.", "Food & Drink", True),
    ("Bucketlisters Wine Tasting Experience", "City Winery", "Experience a unique wine tasting.", "Food & Drink", True),
    ("Yoga in the Park", "Laurelhurst Park", "", "Wellness", True),
    ("UPTOWN POETRY SLAM", "Green Mill Jazz Club", "A poetry slam event taking place weekly.", "Arts", True),
    ("73rd Annual Robin Hood Festival", "Old Town Sherwood", "Featuring the selection of the Maid Marian and the International Archery Match.", "Community", True),
    ("Speed Dating Ages 30-45", "The Alembic", "Meet up to 12 singles in one night. Drinks available for purchase.", "Social", True),
    ("Silent Book Club", "Ruby Coffee", "Bring a book, read in company, chat after. Coffee and pastries for sale.", "Social", True),
    ("Rhapsody: The Music of Queen", "The Colonial Theatre", "A tribute to Queen's greatest hits.", "Music", True),
    ("AirOtic Soiree - Gift Card", "Secret Location", "Gift card for AirOtic Soiree.", None, False),
    ("Shuttle to Lil Wayne", "Bus Depot", "Round-trip shuttle service to the Lil Wayne concert.", None, False),
    ("Puppy Pool Party", "Southeast Community Center", "", None, True),
]


def call(batch):
    """One classification request. Returns {i: obj} or None on failure."""
    lines = []
    for i, (name, venue, desc) in enumerate(batch, 1):
        d = re.sub(r"\s+", " ", desc or "")[:220]
        lines.append(f"{i}. NAME: {name}\n   VENUE: {venue}\n   ABOUT: {d}")
    body = {
        "model": MODEL,
        "max_tokens": 4000,
        "system": SYSTEM,
        "messages": [
            {"role": "user", "content": EXAMPLES_USER},
            {"role": "assistant", "content": EXAMPLES_ASSISTANT},
            {"role": "user", "content": "Classify these events:\n\n" + "\n\n".join(lines)},
        ],
    }
    headers = {"content-type": "application/json", "x-api-key": ANTHROPIC_KEY,
               "anthropic-version": "2023-06-01"}
    for attempt in range(3):
        try:
            r = requests.post("https://api.anthropic.com/v1/messages",
                              headers=headers, json=body, timeout=120)
        except Exception as e:
            print(f"  (network error: {e}, retrying)")
            time.sleep(3 * (attempt + 1)); continue
        if r.status_code in (429, 500, 502, 503, 529):
            time.sleep(5 * (attempt + 1)); continue
        if r.status_code != 200:
            print(f"  API {r.status_code}: {r.text[:200]}")
            return None
        text = "".join(b.get("text", "") for b in r.json().get("content", [])
                       if b.get("type") == "text").strip()
        text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.I | re.M).strip()
        try:
            parsed = json.loads(text)
        except Exception:
            m = re.search(r"\[.*\]", text, re.S)
            if not m:
                print(f"  unparseable: {text[:200]}")
                return None
            try:
                parsed = json.loads(m.group(0))
            except Exception:
                return None
        out = {}
        for o in parsed if isinstance(parsed, list) else []:
            try:
                out[int(o.get("i"))] = o
            except Exception:
                continue
        return out
    return None


def label_of(o):
    """Apply OUR abstain rule. The model never returns 'Event'; we assign it."""
    if not o:
        return "(no answer)"
    if not o.get("is_event", True):
        return "(not an event)"
    p = str(o.get("primary") or "").strip()
    if p not in CATEGORIES:
        return "Event"            # unknown label -> never written through
    try:
        c = float(o.get("confidence", 0))
    except Exception:
        c = 0.0
    return "Event" if c < CONF_FLOOR else p


def run(rows):
    """rows: list of (name, venue, desc). Returns list of result objects."""
    res = []
    for s in range(0, len(rows), BATCH):
        chunk = rows[s:s + BATCH]
        got = call(chunk)
        for i in range(1, len(chunk) + 1):
            res.append((got or {}).get(i))
        print(f"  ...{min(s + BATCH, len(rows))}/{len(rows)}")
    return res


def main():
    print("=" * 78)
    print(f"EVAL — model={MODEL}  confidence floor={CONF_FLOOR}  (writes nothing)")
    print("=" * 78)

    # ---------------- GOLDEN ----------------
    print(f"\nGOLDEN SET ({len(GOLDEN)} cases with known answers)\n")
    results = run([(n, v, d) for n, v, d, _, _ in GOLDEN])
    passed = 0
    for (name, venue, desc, want, want_ev), o in zip(GOLDEN, results):
        got = label_of(o)
        conf = float(o.get("confidence", 0)) if o else 0.0
        if not want_ev:
            ok = (o is not None and not o.get("is_event", True))
        elif want is None:
            ok = (conf < CONF_FLOOR)          # must abstain
        else:
            ok = (got == want)
        passed += ok
        sec = (o or {}).get("secondary") or ""
        secel = (o or {}).get("secondary_element") or ""
        print(f"{'PASS' if ok else 'FAIL'}  {name[:36]:38} -> {got:13} "
              f"conf={conf:.2f}  want={want if want else ('NOT-EVENT' if not want_ev else 'ABSTAIN')}")
        print(f"      offering: {(o or {}).get('offering','?')}")
        if sec:
            print(f"      secondary: {sec}  <- {secel}")
    print(f"\nGOLDEN: {passed}/{len(GOLDEN)}")

    # ---------------- REAL SAMPLE ----------------
    print(f"\n{'=' * 78}\nREAL ROWS currently labeled 'Event' (sample of {SAMPLE_N})\n")
    try:
        data = requests.get(EVENTS_JSON, timeout=60).json()
    except Exception as e:
        print(f"could not load events.json: {e}")
        return
    seen, pool = set(), []
    for r in data:
        if str(r.get("category", "")).strip() != "Event":
            continue
        k = (str(r.get("event_name", "")).lower().strip(),
             str(r.get("venue", "")).lower().strip())
        if k in seen:
            continue
        seen.add(k)
        pool.append((str(r.get("event_name", "")), str(r.get("venue", "")),
                     str(r.get("description", ""))))
    step = max(1, len(pool) // SAMPLE_N)
    sample = pool[::step][:SAMPLE_N]          # spread across the file, not the first N
    print(f"(pool of {len(pool)} unique Event rows; sampling every {step})\n")

    out = run(sample)
    counts, rescued, abstained, notevent = {}, 0, 0, 0
    for (name, venue, desc), o in zip(sample, out):
        lab = label_of(o)
        conf = float(o.get("confidence", 0)) if o else 0.0
        counts[lab] = counts.get(lab, 0) + 1
        if lab == "Event":
            abstained += 1
        elif lab == "(not an event)":
            notevent += 1
        elif lab != "(no answer)":
            rescued += 1
        sec = (o or {}).get("secondary") or ""
        print(f"{lab:14} {conf:.2f}  {name[:40]:42} @ {venue[:20]:22}"
              f"{('  +' + sec) if sec else ''}")
        print(f"               offering: {(o or {}).get('offering','?')}")

    n = len(sample)
    print(f"\n{'=' * 78}\nSAMPLE RESULT on {n} rows that are 'Event' today:")
    print(f"  rescued into a real category : {rescued}  ({rescued / n * 100:.0f}%)")
    print(f"  still Event (low confidence) : {abstained}  ({abstained / n * 100:.0f}%)")
    print(f"  rejected as not-an-event     : {notevent}")
    print("\n  breakdown:")
    for k, v in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"    {k:16} {v}")
    print("\nNothing was written. Read the rows above and tell me what is wrong.")


if __name__ == "__main__":
    main()
