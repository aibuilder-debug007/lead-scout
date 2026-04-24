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
    "restaurant", "hair salon", "auto repair", "gym", "dentist",
    "plumber", "real estate agent", "law firm", "pet grooming", "landscaping",
    "electrician", "clothing store", "yoga studio", "accountant", "cleaning service"
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
                    for place in data["results"][:3]:
                        place_id = place.get("place_id")
                        detail = await get_place_detail(place_id)
                        businesses.append({
                            "name": place.get("name", ""),
                            "address": detail.get("formatted_address") or place.get("formatted_address", location),
                            "website": detail.get("website", ""),
                            "phone": detail.get("formatted_phone_number", ""),
                            "rating": str(place.get("rating", "")),
                        })
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


async def scrape_site_deep(url: str) -> tuple[str, str]:
    """Scrape homepage + contact/about pages for maximum info."""
    if not url:
        return "", ""

    all_text = ""
    all_emails = []
    all_phones = []

    pages_to_try = [url, url.rstrip("/") + "/contact", url.rstrip("/") + "/about", url.rstrip("/") + "/about-us"]

    async with httpx.AsyncClient(timeout=10, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as h:
        for page_url in pages_to_try:
            try:
                r = await h.get(page_url)
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.text, "html.parser")
                for tag in soup(["script", "style", "nav", "header", "footer"]):
                    tag.decompose()
                text = soup.get_text(separator=" ", strip=True)
                all_text += " " + text[:2000]

                emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", r.text)
                phones = re.findall(r"\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}", r.text)
                all_emails.extend(emails)
                all_phones.extend(phones)
            except Exception:
                continue

    contact_parts = []
    if all_emails:
        contact_parts.append(f"Email: {all_emails[0]}")
    if all_phones:
        contact_parts.append(f"Phone: {all_phones[0]}")

    return all_text[:5000], " | ".join(contact_parts)


async def find_owner(business_name: str, location: str, site_text: str) -> str:
    # First try to extract from website content
    if site_text:
        msg = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            messages=[{"role": "user", "content": f"Find the owner, founder, or president name from this business website text. Reply with just the full name or 'Not found'.\n\n{site_text[:3000]}"}]
        )
        result = msg.content[0].text.strip()
        if result != "Not found" and len(result) < 60:
            return result

    # Fallback: search the web
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
            max_tokens=80,
            messages=[{"role": "user", "content": f"Extract the owner or founder name of '{business_name}' from this text. Reply with just the full name or 'Not found'.\n\n{snippets[:1000]}"}]
        )
        return msg.content[0].text.strip()
    except Exception:
        return "Not found"


def analyze_lead(name: str, website: str, address: str, phone: str, rating: str, site_text: str) -> dict:
    msg = claude.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=500,
        extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
        system=[{
            "type": "text",
            "text": (
                "You are an AI services sales analyst. Analyze local businesses and identify exactly what AI service would help them most.\n\n"
                "Reply in EXACTLY this format:\n"
                "WHAT_THEY_NEED: [specific AI service — e.g. 'AI chatbot for customer inquiries', 'automated review response system', 'AI appointment booking']\n"
                "WHY_THEY_NEED_IT: [2-3 sentences — cite specific observed facts: missing features, low rating, no online booking, outdated site, no contact form, etc.]"
            ),
            "cache_control": {"type": "ephemeral"}
        }],
        messages=[{"role": "user", "content": f"""Business Name: {name}
Address: {address}
Phone: {phone}
Google Rating: {rating or 'Not listed'}
Website: {website if website else 'None found'}
Website Content: {site_text[:4000] if site_text else ('(site exists but could not be scraped)' if website else 'N/A — no website')}"""}]
    )
    text = msg.content[0].text
    result = {"what_they_need": "", "why_they_need_it": ""}
    what_match = re.search(r"WHAT_THEY_NEED:\s*(.+?)(?:\n|(?=WHY_THEY_NEED_IT:))", text, re.IGNORECASE)
    why_match = re.search(r"WHY_THEY_NEED_IT:\s*(.+)", text, re.DOTALL | re.IGNORECASE)
    if what_match:
        result["what_they_need"] = what_match.group(1).strip()
    if why_match:
        result["why_they_need_it"] = why_match.group(1).strip()
    if not result["what_they_need"] or not result["why_they_need_it"]:
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

    # If Google Places didn't return a website, try to find it via search
    if not website and BRAVE_API_KEY:
        website = await find_website_via_search(name, location)

    # Deep scrape homepage + contact + about pages
    site_text, scraped_contact = await scrape_site_deep(website)

    # Run analysis and owner search in parallel
    analysis_task = asyncio.get_event_loop().run_in_executor(
        None, analyze_lead, name, website, address, phone, rating, site_text
    )
    owner_task = find_owner(name, location, site_text)
    analysis, owner = await asyncio.gather(analysis_task, owner_task)

    contact_parts = []
    if phone:
        contact_parts.append(f"Phone: {phone}")
    if scraped_contact:
        contact_parts.append(scraped_contact)
    if address and address != location:
        contact_parts.append(f"Address: {address}")

    lead = {
        "name": name,
        "url": website or "No website",
        "rating": rating,
        "what_they_need": analysis["what_they_need"],
        "why_they_need_it": analysis["why_they_need_it"],
        "contact_info": " | ".join(contact_parts) if contact_parts else "Not found",
        "owner_name": owner,
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
        all_leads.extend(results)
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
    fieldnames = ["name", "url", "rating", "what_they_need", "why_they_need_it", "contact_info", "owner_name", "confidence", "website_verified", "phone_verified", "owner_verified", "business_verified"]
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
            "what_they_need": lead["what_they_need"],
            "why_they_need_it": lead["why_they_need_it"],
            "contact_info": lead["contact_info"],
            "owner_name": lead["owner_name"],
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
