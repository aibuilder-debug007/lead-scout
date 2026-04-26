import os
import csv
import io
import re
import httpx
import asyncio
import logging
from bs4 import BeautifulSoup
from fastapi import FastAPI, Form, Request, Cookie
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import anthropic
import secrets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
templates = Jinja2Templates(directory="templates")

BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
GOOGLE_PLACES_KEY = os.environ.get("GOOGLE_PLACES_KEY")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "leadscout123")
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

VALID_TOKENS: set[str] = set()
RESULTS_CACHE: dict[str, dict] = {}  # session -> {location, leads}

CHAIN_KEYWORDS = [
    "mcdonald", "starbucks", "subway", "dunkin", "burger king", "wendy's", "taco bell",
    "chick-fil-a", "domino's", "pizza hut", "kfc", "popeyes", "sonic drive", "dairy queen",
    "panera", "chipotle", "panda express", "five guys", "shake shack", "in-n-out",
    "walmart", "target", "costco", "home depot", "lowe's", "cvs pharmacy", "walgreens",
    "rite aid", "anytime fitness", "planet fitness", "la fitness", "gold's gym",
    "orangetheory", "crunch fitness", "great clips", "supercuts", "sport clips",
    "fantastic sams", "hair cuttery", "jiffy lube", "midas", "meineke", "firestone",
    "pep boys", "autozone", "h&r block", "jackson hewitt", "7-eleven", "circle k",
    "marriott", "hilton", "holiday inn", "best western", "sheraton", "hyatt",
    "applebee's", "denny's", "ihop", "olive garden", "red lobster", "chili's",
    "outback", "buffalo wild wings", "jersey mike", "jimmy john", "potbelly",
]

BLOCKED_DOMAINS = [
    "yelp.com", "yellowpages.com", "tripadvisor.com", "google.com", "facebook.com",
    "bbb.org", "mapquest.com", "foursquare.com", "angieslist.com", "thumbtack.com",
    "homeadvisor.com", "houzz.com", "bark.com", "nextdoor.com", "citysearch.com",
    "manta.com", "superpages.com", "whitepages.com", "dexknows.com", "findlaw.com",
    "avvo.com", "zocdoc.com", "healthgrades.com", "vitals.com", "lawyers.com",
    "chamberofcommerce.com", "instagram.com", "twitter.com", "linkedin.com",
    "bing.com", "yahoo.com", "wikimedia", "wikipedia.org"
]

BUSINESS_TYPES = [
    # High-LTV verticals where AI booking / review recovery / website upgrades convert
    "dentist", "chiropractor", "med spa", "physical therapy",
    "hair salon", "nail salon", "barbershop", "spa",
    "yoga studio", "pilates studio", "personal trainer",
    "auto detailing", "pet grooming", "tutoring",
    "tax preparer", "small accountant",
    # Removed: real estate (already use CRMs), law firm (slow + regulated),
    # plumber/electrician (too busy, blue collar), restaurant chains, big gyms
]


def check_auth(session: str | None) -> bool:
    return session in VALID_TOKENS if session else False


async def get_place_detail(place_id: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=10) as h:
            r = await h.get(
                "https://maps.googleapis.com/maps/api/place/details/json",
                params={
                    "place_id": place_id,
                    "fields": "website,formatted_phone_number,opening_hours,formatted_address",
                    "key": GOOGLE_PLACES_KEY
                }
            )
            return r.json().get("result", {})
    except Exception:
        return {}


async def places_search(query: str, location: str) -> list[dict]:
    businesses = []

    # Google Places
    if GOOGLE_PLACES_KEY:
        try:
            async with httpx.AsyncClient(timeout=10) as h:
                r = await h.get(
                    "https://maps.googleapis.com/maps/api/place/textsearch/json",
                    params={"query": f"{query} in {location}", "key": GOOGLE_PLACES_KEY}
                )
                data = r.json()
                logger.info(f"Places status for '{query} in {location}': {data.get('status')} — {len(data.get('results', []))} results")

                if data.get("status") == "OK" and data.get("results"):
                    added = 0
                    for place in data["results"]:
                        if added >= 3:
                            break
                        name_lower = place.get("name", "").lower()
                        review_count = place.get("user_ratings_total", 0) or 0
                        rating = place.get("rating", 0) or 0

                        # ── HARD SKIPS ────────────────────────────────
                        if any(chain in name_lower for chain in CHAIN_KEYWORDS):
                            logger.info(f"SKIP chain: {place.get('name')}")
                            continue
                        # Already too established to need help
                        if review_count > 100:
                            logger.info(f"SKIP too established ({review_count} reviews): {place.get('name')}")
                            continue
                        # Already crushing it -- no pain to solve
                        if rating >= 4.7 and review_count >= 25:
                            logger.info(f"SKIP already great ({rating}/{review_count}): {place.get('name')}")
                            continue

                        place_id = place.get("place_id")
                        detail = await get_place_detail(place_id)
                        website = detail.get("website", "")

                        # ── WEAKNESS GATE -- must have AT LEAST ONE real signal ──
                        weaknesses: list[str] = []
                        if not website:
                            weaknesses.append("No website at all")
                        if rating and rating <= 4.2:
                            weaknesses.append(f"Mediocre rating: {rating}")
                        if 0 < review_count < 20:
                            weaknesses.append(f"Low review count: only {review_count}")
                        if not detail.get("formatted_phone_number") and not website:
                            weaknesses.append("No phone listed on Google")

                        if not weaknesses:
                            logger.info(f"SKIP no weaknesses ({rating}/{review_count}, has site): {place.get('name')}")
                            continue

                        logger.info(f"KEEP {place.get('name')} -- weaknesses: {weaknesses}")
                        businesses.append({
                            "name": place.get("name", ""),
                            "address": detail.get("formatted_address") or place.get("formatted_address", location),
                            "website": website,
                            "phone": detail.get("formatted_phone_number", ""),
                            "rating": str(rating) if rating else "",
                            "review_count": review_count,
                            "weaknesses": weaknesses,
                        })
                        added += 1
                    return businesses
                else:
                    logger.warning(f"Places API error: {data.get('status')} — {data.get('error_message', '')}")
        except Exception as e:
            logger.error(f"Places exception: {e}")

    # Brave fallback
    try:
        async with httpx.AsyncClient(timeout=10) as h:
            r = await h.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers={"Accept": "application/json", "X-Subscription-Token": BRAVE_API_KEY},
                params={"q": f'"{query}" "{location}"', "count": 10}
            )
            results = r.json().get("web", {}).get("results", [])

        loc_words = [w for w in location.lower().replace(",", "").split() if len(w) > 2]
        for res in results:
            url = res.get("url", "").lower()
            if any(d in url for d in BLOCKED_DOMAINS):
                continue
            combined = (url + res.get("title", "") + res.get("description", "")).lower()
            if not any(w in combined for w in loc_words):
                continue
            businesses.append({
                "name": res.get("title", "").split(" - ")[0].split(" | ")[0].strip(),
                "address": location,
                "website": res.get("url", ""),
                "phone": "",
                "rating": "",
            })
            if len(businesses) >= 2:
                break
    except Exception as e:
        logger.error(f"Brave fallback exception: {e}")

    return businesses


async def scrape_site_deep(url: str) -> tuple[str, str, list[str]]:
    """Crawl homepage + every relevant link found in the actual nav.
    Returns (text, contact, pages_seen) so Claude knows exactly what was checked."""
    if not url:
        return "", "", []

    base = url.rstrip("/")
    all_text = ""
    all_emails: set[str] = set()
    all_phones_norm: dict[str, str] = {}   # digits -> first-seen format
    pages_seen: list[str] = []

    EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    PHONE_RE = re.compile(r"\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}")
    JUNK_EMAIL = ("@example.", "@email.", "@your", "@domain.", "wix.com", "wixsite",
                  "godaddy", "sentry", "@2x", "@1x", "noreply", "no-reply",
                  "@png", "@jpg", "u003e", "wixstatic")

    def absorb(html: str, label: str):
        nonlocal all_text
        soup = BeautifulSoup(html, "html.parser")
        # Strip ONLY script/style -- nav/header/footer often hold phone+address+hours
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        all_text += f"\n\n[PAGE {label}]\n{text[:2500]}"
        for e in EMAIL_RE.findall(html):
            if not any(j in e.lower() for j in JUNK_EMAIL):
                all_emails.add(e)
        for p in PHONE_RE.findall(html):
            digits = re.sub(r"\D", "", p)
            if len(digits) == 10 and digits not in all_phones_norm:
                all_phones_norm[digits] = p
        return soup

    PRIORITY = ("menu", "food", "drink", "reservation", "booking", "book",
                "order", "contact", "about", "team", "service", "price",
                "rate", "hours", "location", "appointment", "schedule")

    async with httpx.AsyncClient(timeout=10, follow_redirects=True,
                                 headers={"User-Agent": "Mozilla/5.0"}) as h:
        # 1) Homepage -- discover real nav links
        try:
            r = await h.get(url)
            if r.status_code == 200:
                pages_seen.append("/")
                soup = absorb(r.text, "/")
                links: set[str] = set()
                for a in soup.find_all("a", href=True):
                    href = a["href"].strip()
                    if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
                        continue
                    if href.startswith("/"):
                        links.add(href.split("?")[0].split("#")[0])
                    elif href.lower().startswith(base.lower()):
                        rel = href[len(base):].split("?")[0].split("#")[0]
                        if rel: links.add(rel)
                # Pick priority links only
                priority_links = [
                    l for l in links
                    if any(kw in l.lower() for kw in PRIORITY) and len(l) < 80
                ][:6]
                for rel in priority_links:
                    page_url = base + rel
                    try:
                        rr = await h.get(page_url)
                        if rr.status_code == 200:
                            pages_seen.append(rel)
                            absorb(rr.text, rel)
                    except Exception:
                        continue
        except Exception:
            pass

    contact_parts: list[str] = []
    if all_emails:
        contact_parts.append(f"Email: {sorted(all_emails)[0]}")
    if all_phones_norm:
        contact_parts.append(f"Phone: {next(iter(all_phones_norm.values()))}")

    return all_text[:9000], " | ".join(contact_parts), pages_seen


async def find_owner_personal_info(owner_name: str, business_name: str, location: str) -> dict:
    """Search for owner's verified LinkedIn and direct phone number."""
    result = {"linkedin": "", "owner_phone": ""}
    if not owner_name or owner_name == "Not found" or not BRAVE_API_KEY:
        return result

    # LinkedIn — search then verify with Claude that it's actually this owner
    try:
        async with httpx.AsyncClient(timeout=10) as h:
            r = await h.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers={"Accept": "application/json", "X-Subscription-Token": BRAVE_API_KEY},
                params={"q": f'"{owner_name}" "{business_name}" linkedin.com/in', "count": 5}
            )
            li_results = r.json().get("web", {}).get("results", [])
        for res in li_results:
            url = res.get("url", "")
            if "linkedin.com/in/" not in url:
                continue
            snippet = res.get("title", "") + " " + res.get("description", "")
            msg = claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=10,
                messages=[{"role": "user", "content": (
                    f"Does this LinkedIn snippet confirm that '{owner_name}' owns or runs '{business_name}'? "
                    f"Answer only Yes or No.\n\n{snippet}"
                )}]
            )
            if "yes" in msg.content[0].text.lower():
                result["linkedin"] = url
                break
    except Exception:
        pass

    # Owner direct phone — search Brave for their name + business + phone
    try:
        for query in [
            f'"{owner_name}" "{business_name}" phone',
            f'"{owner_name}" "{location}" phone',
        ]:
            async with httpx.AsyncClient(timeout=10) as h:
                r = await h.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    headers={"Accept": "application/json", "X-Subscription-Token": BRAVE_API_KEY},
                    params={"q": query, "count": 5}
                )
                snippets = " ".join(
                    res.get("description", "") + " " + res.get("title", "")
                    for res in r.json().get("web", {}).get("results", [])
                )
            phones = re.findall(r"\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}", snippets)
            if phones:
                result["owner_phone"] = phones[0]
                break
    except Exception:
        pass

    return result


async def find_owner(business_name: str, location: str, site_text: str) -> str:
    """Return an owner name ONLY if it literally appears in the source text.
    Anything else is a hallucination -- return 'Not found' instead."""

    def name_present(candidate: str, source: str) -> bool:
        """Verify that the proposed name actually exists in the source text."""
        if not candidate or candidate.lower() == "not found":
            return False
        if len(candidate) > 60 or len(candidate) < 4:
            return False
        # Require BOTH first and last name tokens to appear in source
        tokens = [t for t in candidate.split() if len(t) >= 2]
        if len(tokens) < 2:
            return False
        return all(t.lower() in source.lower() for t in tokens[:2])

    # 1) Try website text
    if site_text:
        msg = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=60,
            messages=[{"role": "user", "content": (
                "Extract the owner/founder/president full name from this website text. "
                "Reply with ONLY the exact name as written in the text, or 'Not found' if no clear "
                "owner is named. Do NOT guess. Do NOT use names that are just mentioned (e.g. customers, "
                "staff bios) -- it must explicitly state owner/founder/president.\n\n"
                f"{site_text[:4000]}"
            )}]
        )
        candidate = msg.content[0].text.strip().strip('"\'.,')
        if name_present(candidate, site_text):
            return candidate

    # 2) Brave fallback -- and the name must appear verbatim in a snippet that also references the business
    if BRAVE_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=10) as h:
                r = await h.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    headers={"Accept": "application/json", "X-Subscription-Token": BRAVE_API_KEY},
                    params={"q": f'"{business_name}" {location} owner OR founder OR president', "count": 4}
                )
                results = r.json().get("web", {}).get("results", [])
            snippets = " ".join(res.get("description", "") + " " + res.get("title", "") for res in results)
            if not snippets.strip():
                return "Not found"
            msg = claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=60,
                messages=[{"role": "user", "content": (
                    f"Extract the owner/founder name of '{business_name}' from this text. "
                    "Only return a name if the text explicitly attaches them as owner/founder/president "
                    "of THIS business. Otherwise reply 'Not found'. Reply with just the name or 'Not found'.\n\n"
                    f"{snippets[:1500]}"
                )}]
            )
            candidate = msg.content[0].text.strip().strip('"\'.,')
            # Verify the name actually appears in a snippet alongside the business name
            biz_lower = business_name.lower()
            for res in results:
                snippet = (res.get("description", "") + " " + res.get("title", "")).lower()
                if biz_lower in snippet and name_present(candidate, snippet):
                    return candidate
        except Exception:
            pass
    return "Not found"


def analyze_lead(name: str, website: str, address: str, phone: str, rating: str,
                 review_count: int, site_text: str, pages_seen: list[str],
                 weaknesses: list[str]) -> dict:
    pages_str = ", ".join(pages_seen) if pages_seen else "(no website scraped)"
    signals_str = "; ".join(weaknesses) if weaknesses else "(none auto-detected)"
    msg = claude.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=600,
        extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
        system=[{
            "type": "text",
            "text": (
                "You are a sales analyst for a small AI agency.\n\n"
                "HARD RULES -- BREAK ANY ONE AND THE LEAD IS DISCARDED:\n"
                "1. The ISSUE must be backed by EVIDENCE -- a direct quote from the scraped content "
                "OR a hard fact (rating number, review count, 'no website' when website field is empty).\n"
                "2. NEVER claim 'no menu' / 'no booking' / 'no contact form' / 'no pricing' UNLESS you can "
                "see the relevant page was crawled and verified empty. The PAGES CRAWLED list tells you what was checked. "
                "If the menu/booking page wasn't in the pages crawled, you do NOT have evidence for that claim -- "
                "say 'NO CLEAR ISSUE' instead of inventing one.\n"
                "3. If you cannot find a real, evidence-backed issue, output exactly:\n"
                "   ISSUE: NO CLEAR ISSUE\n"
                "   EVIDENCE: insufficient data\n"
                "   SOLUTION: skip\n"
                "   PITCH: skip\n"
                "4. NEVER make up specific stats (e.g. '20-30% increase in reservations'). Use general language.\n\n"
                "Output format (EXACT, no extra text):\n"
                "ISSUE: [one specific problem OR 'NO CLEAR ISSUE']\n"
                "EVIDENCE: [direct quote from content OR hard fact like 'rating 3.1 / 12 reviews']\n"
                "SOLUTION: [short product name to sell]\n"
                "PITCH: [2-3 sentences, dollar-focused, no fake stats]"
            ),
            "cache_control": {"type": "ephemeral"}
        }],
        messages=[{"role": "user", "content": f"""Business: {name}
Address: {address}
Google Phone: {phone or '(none)'}
Google Rating: {rating or 'Not listed'} ({review_count or 'unknown'} reviews)
Website: {website if website else 'NONE -- no website found'}
PAGES CRAWLED: {pages_str}
PRE-DETECTED WEAKNESS SIGNALS (use these as the basis of the pitch): {signals_str}
Website Content: {site_text[:6000] if site_text else ('(failed to scrape -- treat as no evidence)' if website else 'N/A')}"""}]
    )
    text = msg.content[0].text
    result = {"issue": "", "evidence": "", "what_they_need": "", "why_they_need_it": ""}
    issue_match    = re.search(r"ISSUE:\s*(.+?)(?:\n|(?=EVIDENCE:))",      text, re.IGNORECASE)
    evidence_match = re.search(r"EVIDENCE:\s*(.+?)(?:\n|(?=SOLUTION:))",   text, re.IGNORECASE)
    sol_match      = re.search(r"SOLUTION:\s*(.+?)(?:\n|(?=PITCH:))",      text, re.IGNORECASE)
    pitch_match    = re.search(r"PITCH:\s*(.+)",                           text, re.DOTALL | re.IGNORECASE)
    if issue_match:    result["issue"]            = issue_match.group(1).strip()
    if evidence_match: result["evidence"]         = evidence_match.group(1).strip()
    if sol_match:      result["what_they_need"]   = sol_match.group(1).strip()
    if pitch_match:    result["why_they_need_it"] = pitch_match.group(1).strip()

    # Reject ungrounded leads: if Claude said NO CLEAR ISSUE, blank out the lead
    issue_lower = result["issue"].lower()
    if "no clear issue" in issue_lower or "insufficient data" in issue_lower:
        return {"issue": "", "evidence": "", "what_they_need": "", "why_they_need_it": ""}

    # If Claude claimed "no menu" but a menu page WAS crawled, that's a hallucination -- reject
    site_lower = site_text.lower() if site_text else ""
    crawled_menu  = any("menu" in p.lower() for p in pages_seen) or "menu" in site_lower[:3000]
    crawled_book  = any(kw in p.lower() for p in pages_seen for kw in ["book","reservation","appointment"]) \
                    or any(w in site_lower for w in ["reservation","book a table","book now","schedule"])
    if "no menu" in issue_lower and crawled_menu:
        logger.warning(f"REJECTED hallucinated 'no menu' issue for {name!r} -- menu page was crawled")
        return {"issue": "", "evidence": "", "what_they_need": "", "why_they_need_it": ""}
    if any(p in issue_lower for p in ["no online booking","no booking","no reservation"]) and crawled_book:
        logger.warning(f"REJECTED hallucinated 'no booking' issue for {name!r}")
        return {"issue": "", "evidence": "", "what_they_need": "", "why_they_need_it": ""}

    if not result["issue"]:
        logger.warning(f"Parse failure for {name!r}: {text[:150]}")
    return result


async def find_website_via_search(business_name: str, location: str) -> str:
    """Search Brave for a business website when Google Places doesn't return one."""
    try:
        async with httpx.AsyncClient(timeout=10) as h:
            r = await h.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers={"Accept": "application/json", "X-Subscription-Token": BRAVE_API_KEY},
                params={"q": f'"{business_name}" {location} official website', "count": 5}
            )
            results = r.json().get("web", {}).get("results", [])
        for res in results:
            url = res.get("url", "")
            if url and not any(d in url.lower() for d in BLOCKED_DOMAINS):
                return url
    except Exception:
        pass
    return ""


async def verify_lead_data(lead: dict) -> dict:
    """Cross-check each lead field against live sources. Returns per-field pass/fail."""
    checks = {"website": None, "phone": None, "owner": None, "business": None}

    # 1. Website — actually loads with a real response
    url = lead.get("url", "")
    if url and url != "No website":
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as h:
                r = await h.get(url)
                checks["website"] = r.status_code < 400
        except Exception:
            checks["website"] = False

    # 2. Phone — Brave search confirms number is associated with this business name
    contact = lead.get("contact_info", "")
    phone_match = re.search(r"\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}", contact)
    if phone_match and BRAVE_API_KEY:
        phone = phone_match.group()
        try:
            async with httpx.AsyncClient(timeout=8) as h:
                r = await h.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    headers={"Accept": "application/json", "X-Subscription-Token": BRAVE_API_KEY},
                    params={"q": f'"{phone}" "{lead["name"]}"', "count": 3}
                )
            checks["phone"] = len(r.json().get("web", {}).get("results", [])) > 0
        except Exception:
            checks["phone"] = False

    # 3. Owner — Brave search confirms name is tied to this business
    owner = lead.get("owner_name", "Not found")
    if owner and owner != "Not found" and BRAVE_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=8) as h:
                r = await h.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    headers={"Accept": "application/json", "X-Subscription-Token": BRAVE_API_KEY},
                    params={"q": f'"{owner}" "{lead["name"]}"', "count": 3}
                )
            checks["owner"] = len(r.json().get("web", {}).get("results", [])) > 0
        except Exception:
            checks["owner"] = False

    # 4. Business exists — Google Places confirms the business name is real
    if GOOGLE_PLACES_KEY:
        try:
            async with httpx.AsyncClient(timeout=8) as h:
                r = await h.get(
                    "https://maps.googleapis.com/maps/api/place/textsearch/json",
                    params={"query": lead["name"], "key": GOOGLE_PLACES_KEY}
                )
            results = r.json().get("results", [])
            if results:
                top_name = results[0].get("name", "").lower()
                lead_name = lead["name"].lower()
                checks["business"] = (
                    lead_name in top_name or top_name in lead_name
                    or any(w in top_name for w in lead_name.split() if len(w) > 3)
                )
            else:
                checks["business"] = False
        except Exception:
            checks["business"] = False

    verified = sum(1 for v in checks.values() if v is True)
    failed = sum(1 for v in checks.values() if v is False)
    if verified >= 3:
        label = "High Confidence"
    elif verified >= 1:
        label = "Partial"
    else:
        label = "Unverified"

    return {"checks": checks, "score": verified, "failed": failed, "label": label}


async def process_business(biz: dict, location: str) -> dict:
    name = biz.get("name", "")
    website = biz.get("website", "")
    phone = biz.get("phone", "")
    address = biz.get("address", location)
    rating = biz.get("rating", "")
    review_count = biz.get("review_count", 0)
    weaknesses = biz.get("weaknesses", [])

    # If Google Places didn't return a website, try to find it via search
    if not website and BRAVE_API_KEY:
        website = await find_website_via_search(name, location)

    # Deep scrape: follows real navigation links (e.g. /menu, /reservations)
    site_text, scraped_contact, pages_seen = await scrape_site_deep(website)

    # Run analysis and owner search in parallel
    analysis_task = asyncio.get_event_loop().run_in_executor(
        None, analyze_lead, name, website, address, phone, rating, review_count, site_text, pages_seen, weaknesses
    )
    owner_task = find_owner(name, location, site_text)
    analysis, owner = await asyncio.gather(analysis_task, owner_task)

    # Skip leads with no real issue -- don't show bullshit pitches
    if not analysis["issue"]:
        return None

    owner_info = await find_owner_personal_info(owner, name, location)

    # Build contact_info, deduping phones by digit-stripped value
    seen_phone_digits: set[str] = set()
    contact_parts: list[str] = []

    def add_phone(p: str):
        if not p: return
        digits = re.sub(r"\D", "", p)
        if len(digits) >= 10 and digits not in seen_phone_digits:
            seen_phone_digits.add(digits)
            contact_parts.append(f"Phone: {p}")

    add_phone(phone)
    if scraped_contact:
        # Walk through "Email: x | Phone: y" and add each piece, deduping phones
        for piece in scraped_contact.split(" | "):
            piece = piece.strip()
            if piece.startswith("Phone:"):
                add_phone(piece.split(":", 1)[1].strip())
            elif piece:
                contact_parts.append(piece)
    if address and address != location:
        contact_parts.append(f"Address: {address}")

    lead = {
        "name": name,
        "url": website or "No website",
        "rating": rating,
        "review_count": review_count,
        "issue": analysis["issue"],
        "evidence": analysis.get("evidence", ""),
        "what_they_need": analysis["what_they_need"],
        "why_they_need_it": analysis["why_they_need_it"],
        "contact_info": " | ".join(contact_parts) if contact_parts else "Not found",
        "pages_crawled": pages_seen,
        "weakness_signals": weaknesses,
        "owner_name": owner,
        "owner_linkedin": owner_info.get("linkedin", ""),
        "owner_phone": owner_info.get("owner_phone", ""),
    }

    lead["verification"] = await verify_lead_data(lead)
    return lead


async def scout_leads(location: str) -> list[dict]:
    all_leads = []
    seen: set[str] = set()
    for biz_type in BUSINESS_TYPES:
        businesses = await places_search(biz_type, location)
        unique = [b for b in businesses if b["name"].lower().strip() not in seen]
        for b in unique:
            seen.add(b["name"].lower().strip())
        tasks = [process_business(biz, location) for biz in unique]
        results = await asyncio.gather(*tasks)
        # Drop leads where Claude couldn't find evidence-backed issues
        all_leads.extend([r for r in results if r is not None])
    return all_leads


@app.get("/debug-places")
async def debug_places(session: str | None = Cookie(default=None)):
    if not check_auth(session):
        return RedirectResponse(url="/login", status_code=303)
    try:
        async with httpx.AsyncClient(timeout=10) as h:
            r = await h.get(
                "https://maps.googleapis.com/maps/api/place/textsearch/json",
                params={"query": "restaurant in Newark NJ", "key": GOOGLE_PLACES_KEY}
            )
            return r.json()
    except Exception as e:
        return {"error": str(e)}


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": False})


@app.post("/login", response_class=HTMLResponse)
async def login(request: Request, password: str = Form(...)):
    if password == APP_PASSWORD:
        token = secrets.token_hex(32)
        VALID_TOKENS.add(token)
        response = RedirectResponse(url="/", status_code=303)
        response.set_cookie("session", token, httponly=True, max_age=86400 * 7)
        return response
    return templates.TemplateResponse("login.html", {"request": request, "error": True})


@app.get("/logout")
async def logout(session: str | None = Cookie(default=None)):
    VALID_TOKENS.discard(session)
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie("session")
    return response


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, session: str | None = Cookie(default=None)):
    if not check_auth(session):
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("index.html", {"request": request, "leads": None})


@app.post("/search", response_class=HTMLResponse)
async def search(request: Request, location: str = Form(...), session: str | None = Cookie(default=None)):
    if not check_auth(session):
        return RedirectResponse(url="/login", status_code=303)
    leads = await scout_leads(location)
    RESULTS_CACHE[session] = {"location": location, "leads": leads}
    high_confidence = sum(1 for l in leads if l.get("verification", {}).get("label") == "High Confidence")
    return templates.TemplateResponse("index.html", {"request": request, "leads": leads, "location": location, "high_confidence": high_confidence})


@app.post("/download")
async def download(location: str = Form(...), session: str | None = Cookie(default=None)):
    if not check_auth(session):
        return RedirectResponse(url="/login", status_code=303)
    cached = RESULTS_CACHE.get(session)
    if cached and cached.get("location") == location:
        leads = cached["leads"]
    else:
        leads = await scout_leads(location)
    output = io.StringIO()
    fieldnames = ["name", "url", "rating", "review_count", "issue", "what_they_need", "why_they_need_it", "contact_info", "owner_name", "owner_linkedin", "owner_phone", "confidence", "website_verified", "phone_verified", "owner_verified", "business_verified"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for lead in leads:
        v = lead.get("verification", {})
        checks = v.get("checks", {})
        def bstr(val): return "Yes" if val is True else ("No" if val is False else "N/A")
        writer.writerow({
            "name": lead["name"],
            "url": lead["url"],
            "rating": lead.get("rating", ""),
            "review_count": lead.get("review_count", ""),
            "issue": lead.get("issue", ""),
            "what_they_need": lead["what_they_need"],
            "why_they_need_it": lead["why_they_need_it"],
            "contact_info": lead["contact_info"],
            "owner_name": lead["owner_name"],
            "owner_linkedin": lead.get("owner_linkedin", ""),
            "owner_phone": lead.get("owner_phone", ""),
            "confidence": v.get("label", ""),
            "website_verified": bstr(checks.get("website")),
            "phone_verified": bstr(checks.get("phone")),
            "owner_verified": bstr(checks.get("owner")),
            "business_verified": bstr(checks.get("business")),
        })
    output.seek(0)
    safe_loc = re.sub(r"[^\w\-]", "_", location)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=leads_{safe_loc}.csv"},
    )
