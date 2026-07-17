#!/usr/bin/env python3
"""
eval_categories.py — PROVE THE PROMPT. WRITES NOTHING, ANYWHERE.

v2 changes (all from reading v1's real output):
  - No real venue or act names anywhere in the prompt. Naming a venue teaches
    the model a lookup for that venue; naming an act teaches that act. Both
    suppress correct answers on live data. Principles only.
  - No enumerated venue types. Whatever gets listed is what gets checked, and
    the list can never cover every venue. One rule covers all of them.
  - "confidence" replaced by "basis". A self-rated probability is a vibe: v1
    never produced anything under 0.65, so the abstain floor could never fire.
    "What is your basis for this?" is a fact the model can answer honestly.
    OUR code abstains on basis == "guess".
  - The venue rule now explicitly governs the secondary field too. v1 leaked
    venue business into secondary (an outdoor concert got +Food & Drink).
  - Golden cases are HELD OUT: nothing in GOLDEN appears in the examples.

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
    sys.exit("Missing secret: ANTHROPIC_API_KEY (repo Settings -> Secrets -> Actions)")

EVENTS_JSON = "https://hmluygfhvdegmealscky.supabase.co/storage/v1/object/public/events/events.json"
MODEL = "claude-haiku-4-5-20251001"
BATCH = 20
SAMPLE_N = int(os.environ.get("SAMPLE_N", "120"))

# Which bases we accept. Anything else becomes "Event". Applied by OUR code.
# Retunable later off the cached basis without re-calling the API.
ACCEPT_BASIS = set(
    (os.environ.get("ACCEPT_BASIS") or "known_act,stated_in_text,inferred").split(",")
)

CATEGORIES = [
    "Music", "Comedy", "Theater", "Film", "Arts", "Dance", "Talks",
    "Food & Drink", "Markets", "Nightlife", "Community", "Family",
    "Sports", "Wellness", "Social",
]

SYSTEM = """You classify live events for a discovery app. People use it to decide what to go do.

Work the steps. Do not jump to a label.

STEP 1 - IS IT AN EVENT?
An event is something happening at a time and place that people gather for.
Set is_event false ONLY for things that are not events at all: gift cards,
parking passes, camping passes, transport to an event, merchandise.
Private, invite-only, members-only and sold-out things ARE events. Being hard to
get into is not the same as not existing. Never exclude them.

STEP 2 - THE VENUE IS NOT THE EVENT
What a venue routinely does tells you NOTHING about what any single event there
is. Any room can be rented by anyone for anything. A room that usually hosts one
kind of act will host a completely different kind next week, and a room with a
word in its name does not make that word the category. Never reason from what
the venue normally does, or from what its name suggests.
Judge only what the text says is happening at THIS event.

STEP 3 - THE VENUE'S ORDINARY SERVICES ARE NOT THE EVENT
Anything the venue sells or does anyway, that an attendee could simply decline
and still have the full experience they came for, is not part of the event and
never a category. If it is optional, it is the venue.
Food or drink is a category only when it IS the thing being offered, or when it
is bundled into the price so an attendee cannot decline it.

STEP 4 - NAME THE OFFERING
In a few words: what is the attendee going FOR? Not everything mentioned in the
text. The thing itself.

STEP 5 - PRIMARY IS WHAT SURVIVES CANCELLATION
When two things are genuinely bundled, ask: if one were cancelled, would the
other still happen?
- The one that still happens is the base, and the base is PRIMARY.
- If cancelling it leaves no event at all, it is PRIMARY.
Worked through: an added performance inside a larger paid experience does not
become the primary, because cancelling the performance leaves the experience
intact. But a performance that IS the event has nothing left when cancelled, so
it is primary.

STEP 6 - SECONDARY IS RARE. MOST EVENTS HAVE NONE.
Add a secondary ONLY if you can name a concrete element inside THIS event in a
few words. If you cannot name the element, there is no secondary: leave both
fields "".
Everything in STEP 2 and STEP 3 applies here too. The venue, its name, its usual
programming, its ordinary services, the neighbourhood, and the mood are never
elements, and never secondaries. Never fill the field just because it exists.

DISTRACTORS - THE OFFERING DECIDES, MODIFIERS DESCRIBE
One word never flips a category. An adjective about the mood of a performance
describes it; it does not change what the performance is.
Descriptions are auto-generated and usually restate the name and venue padded
with filler. Ignore filler. The words perform, live, show, experience and
celebration carry no category meaning on their own.

PERFORMERS AND WHO IS ACTUALLY PERFORMING
Use your real knowledge of who a named act is, and classify by what that act
actually does. Do not assume a name you do not recognise is a musician just
because the listing looks like a gig. Writers, speakers, preachers, drag
performers, dancers and comedians all tour and all play the same rooms as bands.
A tribute or covers act is still the category of the performance being given,
even though the performer is not the artist named.
An act being MENTIONED is not that act performing. A performance built around
other artists' work is the performance actually happening, not those artists.

BASIS - be honest about how you know
Report the basis for your primary:
  "known_act"      - you recognise the named act/production and know what it does
  "stated_in_text" - the text plainly says what kind of event it is
  "inferred"       - not stated, but the text gives real evidence you reasoned from
  "guess"          - you are pattern-matching. You do not actually know.
Use "guess" whenever you do not genuinely know, including when a name is
unfamiliar and you are reading the format rather than the facts. Guessing is
expected sometimes and reporting it honestly is correct and useful. Never label
a basis stronger than it truly is.

CATEGORIES - pick exactly one primary. There is no "unknown" option.
Music        - a musical performance is the offering: concerts, gigs, DJ sets, tribute acts, orchestras, music festivals, music open mics
Comedy       - stand-up, improv, sketch, a comedian's set
Theater      - plays, musicals, opera, drag, burlesque, stage productions
Film         - screenings, movie nights, film festivals
Arts         - gallery shows, exhibitions, art walks, installations, poetry, literary readings
Dance        - social dance nights, dance classes, ballet, dance performances
Talks        - lectures, panels, author talks, conferences, conventions, expos, seminars, workshops, classes
Food & Drink - food or drink IS the offering: tastings, food and drink festivals, bundled meals
Markets      - farmers markets, craft fairs, flea markets, vendor markets, swap meets
Nightlife    - club nights, raves, late-night parties where the scene itself is the draw
Community    - fundraisers, galas, parades, civic and cultural festivals, pride, neighbourhood events, fairs
Family       - programming aimed at children: storytimes, playgroups, kids' activities and camps
Sports       - games, races, tournaments, matches, leagues, rodeos
Wellness     - yoga, meditation, sound baths, fitness
Social       - the offering is being among people or meeting them: trivia, meetups, game nights, speed dating, singles nights, mixers, social clubs

OUTPUT
A JSON array only. One object per input event, same order, no prose, no fences:
[{"i":1,"is_event":true,"offering":"...","primary":"Music","secondary":"","secondary_element":"","basis":"known_act"}]"""

# Few-shot examples. Every venue and act is INVENTED and generic on purpose:
# a real name here becomes a lookup the model applies to live data, which would
# suppress the correct answer at that venue or for that act. The examples exist
# to demonstrate reasoning through distractors, not to teach any instance.
EXAMPLES_USER = """Classify these events:

1. NAME: The Hollow Pines
   VENUE: The Rusty Anchor
   ABOUT: A hilarious and unforgettable night as The Hollow Pines perform live. Full bar and kitchen open till late.

2. NAME: Sunday Drag Brunch
   VENUE: Marigold Room
   ABOUT: Bottomless mimosas and a full brunch menu, with a drag show performed between courses. $45 includes brunch.

3. NAME: Northside Youth Trust Annual Benefit
   VENUE: The Chuckle Hut Comedy Club
   ABOUT: An evening supporting Northside Youth Trust's programs for local kids. Silent auction and dinner.

4. NAME: Tuesday Quiz Night
   VENUE: Ironworks Brewing Co
   ABOUT: Weekly pub quiz. Teams of up to six. Beer and food available at the bar.

5. NAME: Otter Splash Bash
   VENUE: Meadowbrook Community Center
   ABOUT: Otter Splash Bash is an event at Meadowbrook Community Center promising an unforgettable experience.

6. NAME: Riverbend Blues Festival - PARKING PASS
   VENUE: Riverbend Fairgrounds
   ABOUT: Parking pass for the Riverbend Blues Festival.

7. NAME: Saturday Growers Market
   VENUE: Elmwood Park
   ABOUT: Local produce, bread and flowers every Saturday morning, with live music from a local band on the lawn.

8. NAME: Decades Night: A Tribute to the Grunge Era
   VENUE: The Lantern Hall
   ABOUT: A DJ spins nineties classics all night long.

9. NAME: Marisol Vance
   VENUE: The Foundry Room
   ABOUT: An evening with Marisol Vance, who will share stories from her life and her years in ministry.

10. NAME: Fernhill Sewing Circle
   VENUE: Brightway Superstore
   ABOUT: Come learn, improve your skills, or just socialize while you sew."""

EXAMPLES_ASSISTANT = """[{"i":1,"is_event":true,"offering":"the band's set","primary":"Music","secondary":"","secondary_element":"","basis":"inferred"},
{"i":2,"is_event":true,"offering":"a brunch with a drag show","primary":"Food & Drink","secondary":"Theater","secondary_element":"a drag show between courses","basis":"stated_in_text"},
{"i":3,"is_event":true,"offering":"a charity benefit","primary":"Community","secondary":"","secondary_element":"","basis":"stated_in_text"},
{"i":4,"is_event":true,"offering":"a pub quiz","primary":"Social","secondary":"","secondary_element":"","basis":"stated_in_text"},
{"i":5,"is_event":true,"offering":"unclear","primary":"Social","secondary":"","secondary_element":"","basis":"guess"},
{"i":6,"is_event":false,"offering":"a parking pass","primary":"Community","secondary":"","secondary_element":"","basis":"stated_in_text"},
{"i":7,"is_event":true,"offering":"a growers market","primary":"Markets","secondary":"Music","secondary_element":"a local band playing on the lawn","basis":"stated_in_text"},
{"i":8,"is_event":true,"offering":"a DJ set of nineties music","primary":"Music","secondary":"","secondary_element":"","basis":"stated_in_text"},
{"i":9,"is_event":true,"offering":"a storytelling and ministry talk","primary":"Talks","secondary":"","secondary_element":"","basis":"stated_in_text"},
{"i":10,"is_event":true,"offering":"a sewing social club","primary":"Social","secondary":"","secondary_element":"","basis":"stated_in_text"}]"""

# ---------------------------------------------------------------------------
# GOLDEN: real cases, HELD OUT (nothing here appears in the examples).
# (name, venue, description, expected_primary, expected_is_event)
#   expected_primary None + is_event True  -> must abstain (basis "guess")
#   expected_is_event False                -> must be rejected as not-an-event
GOLDEN = [
    # bare artist names at rooms whose names suggest something else
    ("Vince Gill: 50 Years From Home", "Au-Rene Theater", "", "Music", True),
    ("Sara Bareilles", "Bill Graham Civic Auditorium", "An evening with Sara Bareilles.", "Music", True),
    ("Boosie & Webbie", "Winspear Opera House", "Join Boosie & Webbie for a celebration.", "Music", True),
    ("Alter Bridge with Big Wreck", "Roseland Theater", "A concert featuring Alter Bridge alongside Big Wreck at the Roseland Theater in Portland.", "Music", True),
    # a non-musician touring a theatre: v1 called this Music at 0.85
    ("Dante Gebel - Dallas", "Majestic Theatre - Dallas", "Dante Gebel live in Dallas.", "Talks", True),
    # comedians in a room whose name says comedy (must NOT be suppressed by the venue rule)
    ("Luenell", "Cobb's Comedy Club", "Experience the magic of Luenell at Cobb's Comedy Club.", "Comedy", True),
    ("Marc Maron Tickets", "Chevalier Theatre", "", "Comedy", True),
    # a charity renting a comedy room
    ("Boys & Girls Club Annual Fundraiser", "Cobb's Comedy Club", "An evening supporting Boys & Girls Club of America programs for local kids.", "Community", True),
    # stage
    ("Mean Girls the Musical", "Gerry Frank Amphitheater", "The hit Broadway musical comes to town.", "Theater", True),
    ("State Ballet of Georgia - Swan Lake", "London Coliseum", "", "Dance", True),
    ("Crystal Methyd", "Hal's", "An evening with Crystal Methyd.", "Theater", True),
    # container vs element
    ("Port Townsend Farmers Market", "Downtown Port Townsend", "Local organic produce, accompanied by live music.", "Markets", True),
    # venue services must be stripped, in BOTH fields
    ("How to See Like a Photographer", "Times Ten Cellars", "A lecture on photographic seeing. Wine available for purchase.", "Talks", True),
    ("Cornhole League", "Benny Boy Brewing", "A weekly cornhole league. Beer and food available at the bar.", "Sports", True),
    ("First Fridays at Five", "Persip Park", "A free outdoor community concert series. Food trucks on site.", "Music", True),
    # food IS the offering
    ("Bucketlisters Wine Tasting Experience", "City Winery", "Experience a unique wine tasting.", "Food & Drink", True),
    ("Chalant Matcha Pop-Up", "Paper Son Coffee", "A matcha pop-up at Paper Son Coffee.", "Food & Drink", True),
    # tribute act
    ("Rhapsody: The Music of Queen", "The Colonial Theatre", "A tribute to Queen's greatest hits.", "Music", True),
    # social
    ("Speed Dating Ages 30-45", "The Alembic", "Meet up to 12 singles in one night. Drinks available for purchase.", "Social", True),
    # family / wellness / arts
    ("Baby Storytime", "Park Hill Branch Library", "Stories and songs for babies and their caregivers.", "Family", True),
    ("Yoga in the Park", "Laurelhurst Park", "", "Wellness", True),
    ("UPTOWN POETRY SLAM", "Green Mill Jazz Club", "A poetry slam event taking place weekly.", "Arts", True),
    # convention/expo now has a home
    ("Godzillafest 2026", "Holiday Inn San Francisco", "A Godzilla fan convention with guests, panels and dealers.", "Talks", True),
    # not events
    ("AirOtic Soiree - Gift Card", "Secret Location", "Gift card for AirOtic Soiree.", None, False),
    ("Shuttle to Lil Wayne", "Bus Depot", "Round-trip shuttle service to the Lil Wayne concert.", None, False),
    # must abstain: no information exists
    ("Puppy Pool Party", "Southeast Community Center", "", None, True),
    ("Chanpan", "Polaris Hall", "", None, True),
]


def call(batch):
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
    """OUR abstain rule. The model never returns 'Event'; we assign it."""
    if not o:
        return "(no answer)"
    if not o.get("is_event", True):
        return "(not an event)"
    p = str(o.get("primary") or "").strip()
    if p not in CATEGORIES:
        return "Event"
    if str(o.get("basis") or "").strip() not in ACCEPT_BASIS:
        return "Event"
    return p


def run(rows):
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
    print(f"EVAL v2 — model={MODEL}")
    print(f"accepted bases={sorted(ACCEPT_BASIS)}  (anything else -> Event)")
    print("writes nothing, anywhere")
    print("=" * 78)

    print(f"\nGOLDEN SET ({len(GOLDEN)} held-out cases with known answers)\n")
    results = run([(n, v, d) for n, v, d, _, _ in GOLDEN])
    passed = 0
    for (name, venue, desc, want, want_ev), o in zip(GOLDEN, results):
        got = label_of(o)
        basis = (o or {}).get("basis", "?")
        if not want_ev:
            ok = (o is not None and not o.get("is_event", True))
            wanted = "NOT-EVENT"
        elif want is None:
            ok = (got == "Event")
            wanted = "ABSTAIN"
        else:
            ok = (got == want)
            wanted = want
        passed += ok
        sec = (o or {}).get("secondary") or ""
        print(f"{'PASS' if ok else 'FAIL'}  {name[:34]:36} -> {got:13} "
              f"basis={basis:14} want={wanted}")
        print(f"      offering: {(o or {}).get('offering','?')}"
              f"{('   secondary: ' + sec + ' <- ' + ((o or {}).get('secondary_element') or '')) if sec else ''}")
    print(f"\nGOLDEN: {passed}/{len(GOLDEN)}")

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
    sample = pool[::step][:SAMPLE_N]
    print(f"(pool of {len(pool)} unique Event rows; sampling every {step})\n")

    out = run(sample)
    counts, bases = {}, {}
    rescued = abstained = notevent = 0
    for (name, venue, desc), o in zip(sample, out):
        lab = label_of(o)
        b = (o or {}).get("basis", "?")
        counts[lab] = counts.get(lab, 0) + 1
        bases[b] = bases.get(b, 0) + 1
        if lab == "Event":
            abstained += 1
        elif lab == "(not an event)":
            notevent += 1
        elif lab != "(no answer)":
            rescued += 1
        sec = (o or {}).get("secondary") or ""
        print(f"{lab:14} {b:14} {name[:38]:40} @ {venue[:18]:20}"
              f"{('  +' + sec) if sec else ''}")
        print(f"               offering: {(o or {}).get('offering','?')}")

    n = len(sample)
    print(f"\n{'=' * 78}\nSAMPLE RESULT on {n} rows that are 'Event' today:")
    print(f"  rescued into a real category : {rescued}  ({rescued / n * 100:.0f}%)")
    print(f"  still Event (honest guess)   : {abstained}  ({abstained / n * 100:.0f}%)")
    print(f"  rejected as not-an-event     : {notevent}")
    print("\n  category breakdown:")
    for k, v in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"    {k:16} {v}")
    print("\n  basis breakdown (is it calibrated? 'guess' should not be 0):")
    for k, v in sorted(bases.items(), key=lambda x: -x[1]):
        print(f"    {k:16} {v}")
    print("\nNothing was written. Read the rows above and tell me what is wrong.")


if __name__ == "__main__":
    main()
