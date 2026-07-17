import { supabase } from "~/store/supabase";

export type Event = {
  featured: string;
  date: string;
  start_time: string;
  event_name: string;
  venue: string;
  venue_address: string;
  venue_lat: string;
  venue_lng: string;
  venue_notes: string;
  venue_map_status: string;
  region: string;
  url: string;
  category: string;
  price: string;
  description: string;
};

export type MockEvent = {
  id: string;
  title: string;
  venue: string;
  date: string;
  time: string;
  category: string;
  price: string;
  tags: string[];
  gradientFrom: string;
  gradientTo: string;
};

export const MOCK_EVENTS: MockEvent[] = [];

const JSON_URL =
  "https://hmluygfhvdegmealscky.supabase.co/storage/v1/object/public/events/events.json";

const EVENT_FIELDS: (keyof Event)[] = [
  "featured", "date", "start_time", "event_name", "venue", "venue_address",
  "venue_lat", "venue_lng", "venue_notes", "venue_map_status", "region", "url", "category",
  "price", "description",
];

function rowToEvent(row: Record<string, unknown>): Event {
  const event = {} as Event;
  for (const field of EVENT_FIELDS) {
    event[field] = (row[field] ?? "").toString().trim();
  }
  return event;
}

// Normalizes a name for dedup comparison: unicode-fold (curly quotes become
// straight), lowercase, strip everything but letters and digits. "America's
// Block Party" and "America’s Block Party" produce the same key.
function dedupeKeyPart(s: string): string {
  return s
    .normalize("NFKD")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function dedupeEvents(events: Event[]): Event[] {
  const seen = new Set<string>();
  const result: Event[] = [];
  for (const e of events) {
    const key = `${dedupeKeyPart(e.event_name)}|${e.date}|${dedupeKeyPart(e.venue)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(e);
  }
  return result;
}

function parseEventsJSON(data: unknown): Event[] {
  if (!Array.isArray(data)) return [];
  return dedupeEvents(
    data
      .filter((row) => row && typeof row === "object")
      .map((row) => rowToEvent(row as Record<string, unknown>))
  );
}

export type CategoryFamily = {
  key: string;
  label: string;
  categories: string[];
  dark: string;
  light: string;
};

export const CATEGORY_FAMILIES: CategoryFamily[] = [
  { key: "music",     label: "Music & Nightlife",  categories: ["Music", "Nightlife", "Dance"],    dark: "#7B5EA7", light: "#5B3E87" },
  { key: "stage",     label: "Stage & Screen",     categories: ["Comedy", "Theater", "Film"],      dark: "#B8860B", light: "#8B6000" },
  { key: "arts",      label: "Arts & Ideas",       categories: ["Arts", "Talks"],                  dark: "#4A7C8E", light: "#2A5C6E" },
  { key: "food",      label: "Food & Drink",       categories: ["Food & Drink"],                   dark: "#8B5E3C", light: "#6B3E1C" },
  { key: "community", label: "Community & Markets", categories: ["Markets", "Community", "Family"], dark: "#4A7A4A", light: "#2A5A2A" },
  { key: "active",    label: "Active & Wellness",   categories: ["Sports", "Wellness"],             dark: "#3A5F8A", light: "#1A3F6A" },
];

const FALLBACK_COLOR = { dark: "#2a2a2a", light: "#CCCCCC" };

function buildColorMap(mode: "dark" | "light"): Record<string, string> {
  const map: Record<string, string> = {};
  for (const fam of CATEGORY_FAMILIES) {
    for (const cat of fam.categories) map[cat] = fam[mode];
  }
  map["Event"] = FALLBACK_COLOR[mode];
  return map;
}

export const CATEGORY_COLORS_DARK: Record<string, string> = buildColorMap("dark");
export const CATEGORY_COLORS_LIGHT: Record<string, string> = buildColorMap("light");

export function getCategoryColor(category: string, isDark: boolean): string {
  const map = isDark ? CATEGORY_COLORS_DARK : CATEGORY_COLORS_LIGHT;
  return map[category] ?? map["Event"];
}

export function getCategoryFamily(category: string): CategoryFamily | null {
  return CATEGORY_FAMILIES.find((f) => f.categories.includes(category)) ?? null;
}

function hexToRgb(hex: string): [number, number, number] | null {
  const h = (hex || "").replace("#", "").trim();
  if (h.length === 3) {
    const r = parseInt(h[0] + h[0], 16), g = parseInt(h[1] + h[1], 16), b = parseInt(h[2] + h[2], 16);
    return [r, g, b].some(isNaN) ? null : [r, g, b];
  }
  if (h.length === 6 || h.length === 8) {
    const r = parseInt(h.slice(0, 2), 16), g = parseInt(h.slice(2, 4), 16), b = parseInt(h.slice(4, 6), 16);
    return [r, g, b].some(isNaN) ? null : [r, g, b];
  }
  return null;
}

function channelToHex(n: number): string {
  return Math.max(0, Math.min(255, Math.round(n))).toString(16).padStart(2, "0");
}

// Brightens a hex color by scaling each channel toward full intensity. Used to
// give category icons / borders / glow a vivid (not muted, not neon) accent.
function brighten(hex: string, f: number): string {
  const rgb = hexToRgb(hex);
  if (!rgb) return hex;
  const out = rgb.map((c) => Math.max(0, Math.min(255, Math.round(c + (255 - c) * (f * 0.35) + c * f))));
  return "#" + out.map(channelToHex).join("");
}

// The vivid accent for a category — brighter than the base color, used for
// icons, card borders, and the soft glow. In dark mode it lifts the muted base
// off the near-black background; in light mode the base already reads, so it's
// returned mostly as-is (a hair brighter).
export function getCategoryAccent(category: string, isDark: boolean): string {
  const base = getCategoryColor(category, isDark);
  return isDark ? brighten(base, 0.5) : brighten(base, 0.08);
}

export function getCategoryCardBg(cardColor: string, category: string, isDark: boolean): string {
  const base = hexToRgb(cardColor);
  const tint = hexToRgb(getCategoryColor(category, isDark));
  if (!base || !tint) return cardColor;
  // Slightly stronger tint than before (was .10/.06) so the category color
  // reads on the card without hurting text contrast.
  const ratio = isDark ? 0.16 : 0.10;
  const mixed = base.map((b, i) => b + (tint[i] - b) * ratio);
  return "#" + mixed.map(channelToHex).join("");
}

// Category inference — mirrors the fixed pipeline logic: word-boundary
// matching (never substrings, so "improve" is not improv and "matcha" is not
// match), scored across fields (name 5 / venue 2 / description 1) instead of
// first-match-wins, and no generic verbs like "perform" or "live" that let
// Music swallow everything. Only runs when a row has no stored category
// (owned/scanned events); the pipeline's stored value always wins.
const CATEGORY_KEYWORDS: Record<string, string[]> = {
  Music: ["music", "concert", "band", "jazz", "blues", "bluegrass", "acoustic", "folk", "hip-hop", "hiphop", "rap", "rock", "punk", "metal", "indie", "reggae", "soul", "orchestra", "choir", "symphony", "recital", "vinyl", "album", "singer", "songwriter", "dj", "edm", "tribute", "country music"],
  Comedy: ["comedy", "improv", "standup", "stand-up", "comedian", "sketch comedy"],
  Theater: ["theater", "theatre", "musical", "opera", "ballet", "broadway", "cabaret", "burlesque", "drag show", "spoken word", "storytelling"],
  Film: ["film", "cinema", "screening", "movie", "documentary", "matinee"],
  Arts: ["art", "arts", "gallery", "exhibit", "exhibition", "museum", "poetry", "literary", "author", "photography", "mural", "sculpture"],
  Dance: ["salsa", "bachata", "swing dance", "tango", "lindy", "ballroom", "milonga", "line dancing", "dance class"],
  Talks: ["lecture", "panel", "book club", "keynote", "seminar", "symposium", "author talk"],
  "Food & Drink": ["food", "brunch", "dinner", "tasting", "beer", "wine", "cocktail", "whiskey", "brewery", "winery", "culinary", "chef", "restaurant", "bar"],
  Markets: ["market", "farmers", "fair", "craft fair", "vendor", "artisan", "bazaar", "flea", "night market"],
  Nightlife: ["nightlife", "nightclub", "rave", "techno", "house music", "dance party", "after party", "late night", "lounge"],
  Community: ["fundraiser", "benefit", "nonprofit", "volunteer", "civic", "heritage", "meetup", "meet-up", "festival", "parade", "rally", "pride", "block party"],
  Family: ["family", "kids", "children", "toddler", "baby", "storytime", "playgroup"],
  Sports: ["basketball", "football", "soccer", "baseball", "hockey", "tournament", "marathon", "5k", "wrestling", "boxing", "golf", "tennis", "pickleball"],
  Wellness: ["yoga", "meditation", "wellness", "mindfulness", "sound bath", "breathwork", "pilates", "tai chi"],
};

// Definitional terms: words that prove a category on their own because they
// have no second meaning. Split by field, because the same word carries very
// different weight depending on where it appears. Mirrors pipeline.py exactly.
//
// NAME: broad. "Music in the Garden" is music; "Blues Jam" is music.
const CATEGORY_DEFINITIONAL: Record<string, string[]> = {
  Music: ["concert", "concerts", "live music", "music", "symphony", "orchestra", "philharmonic", "recital", "choir", "quartet", "jazz", "blues", "bluegrass", "reggae", "hip-hop", "punk rock", "heavy metal", "indie rock", "edm", "dj set", "songwriter", "tribute to"],
  Comedy: ["comedy", "comedian", "comedians", "standup", "stand-up", "improv"],
  Theater: ["musical", "opera", "cabaret", "burlesque", "pantomime"],
  Film: ["screening", "documentary", "movie", "film festival"],
  Arts: ["exhibition", "exhibit", "art show", "art fair", "poetry"],
  Dance: ["salsa", "bachata", "tango", "milonga", "swing dance", "line dancing", "ballet", "ballroom"],
  Talks: ["lecture", "keynote", "seminar", "symposium", "book club", "author talk"],
  "Food & Drink": ["food truck", "tasting", "brewery", "distillery", "winery", "ribfest", "food festival", "food fest", "bbq"],
  Markets: ["farmers market", "flea market", "craft fair", "swap meet", "bazaar", "vendor market", "market"],
  Nightlife: ["nightclub", "dj set", "rave", "club night", "after party", "techno", "house music"],
  Community: ["fundraiser", "parade", "street fair"],
  Family: ["storytime", "story time"],
  Sports: ["rodeo", "marathon", "5k", "10k", "fun run", "regatta"],
  Wellness: ["yoga", "meditation", "sound bath", "breathwork", "pilates", "tai chi"],
};

// DESCRIPTION: narrow. Nearly every blurb says "music" somewhere, which is how
// a French street market got tagged Music. Only words that state what the
// event IS are allowed to prove a category here.
const CATEGORY_DEFINITIONAL_DESC: Record<string, string[]> = {
  Music: ["concert", "concerts", "live music performance"],
  Comedy: ["comedians", "comedy show", "stand-up comedy", "standup comedy"],
  Film: ["screening", "film screening"],
  Theater: ["stage production"],
  Markets: ["farmers market", "flea market"],
  Family: ["storytime", "story time", "playgroup", "toddler", "toddlers"],
};

// VENUE: tiniest of all. Most venues host anything, but a room with "Comedy"
// in its name hosts comedy and a cinema shows films.
const CATEGORY_DEFINITIONAL_VENUE: Record<string, string[]> = {
  Comedy: ["comedy"],
  Film: ["cinema", "cineplex", "movie theater", "drive-in"],
  Nightlife: ["nightclub"],
  Music: ["music hall", "concert hall", "jazz club"],
  Wellness: ["yoga studio", "wellness center"],
};

// Generic venue words that predict nothing: a Theater hosts rock, comedy and
// ballet; an Auditorium hosts all that plus graduations. Their hits are ignored
// so a venue name can never decide a category by itself.
const VENUE_STOPWORD_RE =
  /^(theater|theatre|auditorium|amphitheater|amphitheatre|arena|coliseum|stadium|hall|center|centre|pavilion|plaza|park|room|stage|garden|casino|civic|memorial|field|bowl|dome|complex|venue|space|studio|studios)$/i;

const rx = (kw: string) => new RegExp(`\\b${kw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");
const buildMap = (src: Record<string, string[]>): Record<string, RegExp[]> =>
  Object.fromEntries(Object.entries(src).map(([c, ks]) => [c, ks.map(rx)]));

const CATEGORY_PATTERNS: [string, RegExp[]][] = Object.entries(CATEGORY_KEYWORDS).map(
  ([category, keywords]) => [category, keywords.map(rx)]
);
const DEF_NAME_PATTERNS = buildMap(CATEGORY_DEFINITIONAL);
const DEF_DESC_PATTERNS = buildMap(CATEGORY_DEFINITIONAL_DESC);
const DEF_VENUE_PATTERNS = buildMap(CATEGORY_DEFINITIONAL_VENUE);

// Merges overlapping match spans and returns the distinct matched text, so two
// keywords covering the same words ("music" and "live music" on one phrase)
// count as the one piece of evidence they are.
function distinctMatches(text: string, spans: [number, number][]): string[] {
  const merged: [number, number][] = [];
  for (const [s, e] of [...spans].sort((a, b) => a[0] - b[0])) {
    const last = merged[merged.length - 1];
    if (last && s < last[1]) last[1] = Math.max(last[1], e);
    else merged.push([s, e]);
  }
  return merged.map(([s, e]) => text.slice(s, e).trim());
}

export function inferCategory(event: Event): string {
  if (event.category?.trim()) return event.category.trim();
  const n = (event.event_name || "").toLowerCase();
  const v = (event.venue || "").toLowerCase();
  const d = (event.description || "").toLowerCase();
  let best = "Event";
  let bestScore = 0;
  let secondScore = 0;

  for (const [category, patterns] of CATEGORY_PATTERNS) {
    let score = 0;
    const spans: Record<"n" | "v" | "d", [number, number][]> = { n: [], v: [], d: [] };
    for (const p of patterns) {
      const mn = p.exec(n);
      let mv = p.exec(v);
      const md = p.exec(d);
      if (mv && VENUE_STOPWORD_RE.test(mv[0])) mv = null;
      if (mn) { score += 5; spans.n.push([mn.index, mn.index + mn[0].length]); }
      if (mv) { score += 2; spans.v.push([mv.index, mv.index + mv[0].length]); }
      if (md) { score += 1; spans.d.push([md.index, md.index + md[0].length]); }
    }
    // Count independent evidence: distinct matched text, deduped across fields
    // (descriptions echo venue names, and that is one fact, not two).
    const seen = new Set<string>([
      ...distinctMatches(n, spans.n),
      ...distinctMatches(v, spans.v),
      ...distinctMatches(d, spans.d),
    ]);
    const distinct = seen.size;

    let definitional = false;
    if ((DEF_NAME_PATTERNS[category] ?? []).some((p) => p.test(n))) {
      definitional = true; score = Math.max(score, 5);
    }
    if ((DEF_VENUE_PATTERNS[category] ?? []).some((p) => p.test(v))) {
      definitional = true; score = Math.max(score, 2);
    }
    if ((DEF_DESC_PATTERNS[category] ?? []).some((p) => p.test(d))) {
      definitional = true; score = Math.max(score, 1);
    }
    // One ambiguous signal never decides.
    if (!definitional && distinct < 2) continue;

    if (score > bestScore) {
      secondScore = bestScore;
      bestScore = score;
      best = category;
    } else if (score > secondScore) {
      secondScore = score;
    }
  }
  // Two categories equally likely -> honest refusal, same as geocoding.
  if (bestScore > 0 && bestScore === secondScore) return "Event";
  return best;
}

export function getAvailableRegions(events: Event[]): string[] {
  const set = new Set<string>();
  for (const e of events) {
    const r = e.region?.trim();
    if (r) set.add(r);
  }
  return Array.from(set).sort();
}

export function filterByRegion(events: Event[], region: string | null): Event[] {
  if (!region) return events;
  return events.filter((e) => e.region?.trim().toLowerCase() === region.trim().toLowerCase());
}

export function filterByFamily(events: Event[], familyKey: string | null): Event[] {
  if (!familyKey) return events;
  const fam = CATEGORY_FAMILIES.find((f) => f.key === familyKey);
  if (!fam) return events;
  const cats = new Set(fam.categories);
  return events.filter((e) => cats.has(inferCategory(e)));
}

const OWNED_FIELDS =
  "featured, date, start_time, event_name, venue, venue_address, venue_lat, venue_lng, venue_notes, venue_map_status, region, url, category";

async function fetchOwnedEvents(): Promise<Event[]> {
  try {
    const { data, error } = await supabase.from("events").select(OWNED_FIELDS);
    if (error) throw error;
    return (data ?? []).map((r: any) => ({
      featured: r.featured ?? "",
      date: r.date ?? "",
      start_time: r.start_time ?? "",
      event_name: r.event_name ?? "",
      venue: r.venue ?? "",
      venue_address: r.venue_address ?? "",
      venue_lat: r.venue_lat != null ? String(r.venue_lat) : "",
      venue_lng: r.venue_lng != null ? String(r.venue_lng) : "",
      venue_notes: r.venue_notes ?? "",
      venue_map_status: r.venue_map_status ?? "",
      region: r.region ?? "",
      url: r.url ?? "",
      category: r.category ?? "",
      price: r.price ?? "",
      description: r.description ?? "",
    }));
  } catch {
    return [];
  }
}

async function fetchBulkEvents(): Promise<Event[]> {
  const res = await fetch(JSON_URL);
  const data = await res.json();
  return parseEventsJSON(data);
}

let bulkCache: Event[] | null = null;
let bulkCacheTime: number | null = null;
const BULK_TTL = 1000 * 60 * 10;

let ownedCache: Event[] | null = null;
let ownedCacheTime: number | null = null;
const OWNED_TTL = 1000 * 30;

export function invalidateEventsCache(): void {
  bulkCache = null; bulkCacheTime = null;
  ownedCache = null; ownedCacheTime = null;
}

export async function fetchEvents(): Promise<Event[]> {
  const now = Date.now();

  if (!bulkCache || !bulkCacheTime || now - bulkCacheTime >= BULK_TTL) {
    try {
      bulkCache = await fetchBulkEvents();
      bulkCacheTime = now;
    } catch {
      bulkCache = bulkCache ?? [];
    }
  }

  if (!ownedCache || !ownedCacheTime || now - ownedCacheTime >= OWNED_TTL) {
    ownedCache = await fetchOwnedEvents();
    ownedCacheTime = now;
  }

  return dedupeEvents([...(ownedCache ?? []), ...(bulkCache ?? [])]);
}

export function isEventPast(event: Event): boolean {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const eventDate = new Date(event.date + "T00:00:00");
  return eventDate < today;
}

export function filterUpcoming(events: Event[]): Event[] {
  return events.filter((e) => !isEventPast(e));
}

export function groupByDate(events: Event[]): { date: string; events: Event[] }[] {
  const map = new Map<string, Event[]>();
  for (const e of events) {
    if (!map.has(e.date)) map.set(e.date, []);
    map.get(e.date)!.push(e);
  }
  // Sort the date groups chronologically (soonest first), and sort the events
  // inside each group by start time so the list reads top-to-bottom in order.
  const toMinutes = (t: string): number => {
    const m = (t || "").trim().match(/^(\d{1,2})(?::(\d{2}))?\s*([AaPp][Mm])?$/);
    if (!m) return 24 * 60; // no/!invalid time → end of day
    let hh = parseInt(m[1], 10);
    const mm = m[2] ? parseInt(m[2], 10) : 0;
    const ap = (m[3] || "").toLowerCase();
    if (ap === "pm" && hh < 12) hh += 12;
    if (ap === "am" && hh === 12) hh = 0;
    if (hh > 23 || mm > 59) return 24 * 60;
    return hh * 60 + mm;
  };
  return Array.from(map.entries())
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([date, evs]) => ({
      date,
      events: [...evs].sort((a, b) => toMinutes(a.start_time) - toMinutes(b.start_time)),
    }));
}

export function formatDate(dateStr: string): string {
  const date = new Date(dateStr + "T00:00:00");
  return date.toLocaleDateString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
}

export function getDistance(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 3958.8;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLng = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLng / 2) *
      Math.sin(dLng / 2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

export function sortByDistance(events: Event[], userLat: number, userLng: number): Event[] {
  return [...events].sort((a, b) => {
    const latA = parseFloat(a.venue_lat);
    const lngA = parseFloat(a.venue_lng);
    const latB = parseFloat(b.venue_lat);
    const lngB = parseFloat(b.venue_lng);
    if (isNaN(latA) || isNaN(lngA)) return 1;
    if (isNaN(latB) || isNaN(lngB)) return -1;
    return getDistance(userLat, userLng, latA, lngA) - getDistance(userLat, userLng, latB, lngB);
  });
}

export const SEARCH_RADIUS_MILES = 25;

export function filterByRadius(events: Event[], userLat: number, userLng: number): Event[] {
  return events.filter((e) => {
    const lat = parseFloat(e.venue_lat);
    const lng = parseFloat(e.venue_lng);
    if (isNaN(lat) || isNaN(lng)) return false;
    return getDistance(userLat, userLng, lat, lng) <= SEARCH_RADIUS_MILES;
  });
}

export async function geocodeCity(city: string, apiKey: string): Promise<{ lat: number; lng: number; label: string } | null> {
  const url = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(city)}&key=${apiKey}`;
  const res = await fetch(url);
  const data = await res.json();
  if (data.status === "OK" && data.results.length > 0) {
    const result = data.results[0];
    return {
      lat: result.geometry.location.lat,
      lng: result.geometry.location.lng,
      label: result.formatted_address,
    };
  }
  return null;
}

export const FEATURED_MIN = 5;

const AUTO_FILL_CATEGORIES = new Set<string>([
  "Music", "Comedy", "Theater", "Film", "Dance", "Nightlife", "Arts", "Food & Drink", "Sports",
]);

type FeaturedRow = {
  event_url: string;
  tier: string;
  manual_priority: number | null;
  active: boolean;
};

function isEventComplete(e: Event): boolean {
  if (!e.event_name?.trim()) return false;
  if (!e.date?.trim()) return false;
  if (!e.url?.trim()) return false;
  if (isNaN(parseFloat(e.venue_lat)) || isNaN(parseFloat(e.venue_lng))) return false;
  return true;
}

function chronological(a: Event, b: Event): number {
  if (a.date !== b.date) return a.date < b.date ? -1 : 1;
  return (a.start_time || "").localeCompare(b.start_time || "");
}

function pickAutoFill(
  pool: Event[],
  needed: number,
  alreadyChosenUrls: Set<string>
): Event[] {
  const eligible = pool
    .filter((e) => isEventComplete(e))
    .filter((e) => !alreadyChosenUrls.has(e.url))
    .filter((e) => AUTO_FILL_CATEGORIES.has(inferCategory(e)))
    .sort(chronological);

  const chosen: Event[] = [];
  const perCategory: Record<string, number> = {};

  for (const e of eligible) {
    if (chosen.length >= needed) break;
    const cat = inferCategory(e);
    if ((perCategory[cat] ?? 0) >= 2) continue;
    perCategory[cat] = (perCategory[cat] ?? 0) + 1;
    chosen.push(e);
  }

  if (chosen.length < needed) {
    for (const e of eligible) {
      if (chosen.length >= needed) break;
      if (chosen.some((c) => c.url === e.url)) continue;
      chosen.push(e);
    }
  }

  return chosen;
}

export async function getFeaturedEvents(
  allUpcoming: Event[],
  userLat: number | null,
  userLng: number | null
): Promise<Event[]> {
  let rows: FeaturedRow[] = [];
  try {
    const { data, error } = await supabase
      .from("featured_events")
      .select("event_url, tier, manual_priority, active")
      .eq("active", true);
    if (error) throw error;
    rows = (data ?? []) as FeaturedRow[];
  } catch {
    rows = [];
  }

  const byUrl = new Map<string, Event>();
  for (const e of allUpcoming) {
    if (e.url?.trim() && isEventComplete(e)) byUrl.set(e.url.trim(), e);
  }

  const manual: { e: Event; priority: number }[] = [];
  const paid: Event[] = [];
  const chosenUrls = new Set<string>();

  for (const row of rows) {
    if (!row.event_url?.trim()) continue;
    const match = byUrl.get(row.event_url.trim());
    if (!match) continue;
    if (chosenUrls.has(match.url)) continue;
    if (row.tier === "manual") {
      manual.push({ e: match, priority: row.manual_priority ?? 9999 });
      chosenUrls.add(match.url);
    } else if (row.tier === "paid") {
      paid.push(match);
      chosenUrls.add(match.url);
    }
  }

  manual.sort((a, b) => a.priority - b.priority);
  const manualEvents = manual.map((m) => m.e);

  const haveLocation = userLat !== null && userLng !== null;

  let regionManual = manualEvents;
  let regionPaid = paid;
  let autoFill: Event[] = [];

  if (haveLocation) {
    regionManual = filterByRadius(manualEvents, userLat!, userLng!);
    regionPaid = filterByRadius(paid, userLat!, userLng!);

    const chosenSoFar = new Set<string>([...regionManual, ...regionPaid].map((e) => e.url));
    const needed = FEATURED_MIN - chosenSoFar.size;

    if (needed > 0) {
      const nearbyPool = filterByRadius(allUpcoming, userLat!, userLng!);
      autoFill = pickAutoFill(nearbyPool, needed, chosenSoFar);
    }
  }

  const combined: Event[] = [];
  const seen = new Set<string>();
  for (const e of [...regionManual, ...regionPaid, ...autoFill]) {
    if (seen.has(e.url)) continue;
    seen.add(e.url);
    combined.push(e);
  }

  return combined.sort(chronological);
}
