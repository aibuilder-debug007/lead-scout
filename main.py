import os
import csv
import io
import re
import httpx
import asyncio
from bs4 import BeautifulSoup
from fastapi import FastAPI, Form, Request, Cookie
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import anthropic
import secrets

app = FastAPI()
templates = Jinja2Templates(directory="templates")

BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
GOOGLE_PLACES_KEY = os.environ.get("GOOGLE_PLACES_KEY")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "leadscout123")
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

VALID_TOKENS: set[str] = set()

def check_auth(session: str | None) -> bool:
    return session in VALID_TOKENS if session else False

BUSINESS_TYPES = [
    "restaurant", "hair salon", "auto repair", "gym", "dentist",
    "plumber", "real estate agent", "law firm", "pet grooming", "landscaping",
    "electrician", "clothing store", "yoga studio", "accountant", "cleaning service"
]


async def places_search(query: str, location: str) -> list[dict]:
    """Search Google Places for businesses and return verified data."""
    try:
        async with httpx.AsyncClient(timeout=10) as h:
            # Text search to find businesses
            r = await h.get(
                "https://maps.googleapis.com/maps/api/place/textsearch/json",
                params={"query": f"{query} in {location}", "key": GOOGLE_PLACES_KEY}
            )
            r.raise_for_status()
            results = r.json().get("results", [])

        businesses = []
        for place in results[:3]:
            place_id = place.get("place_id")
            # Get detailed info for each place
            detail = await get_place_detail(place_id)
            businesses.append({
                "name": place.get("name", ""),
                "address": place.get("formatted_address", ""),
                "place_id": place_id,
                "website": detail.get("website", ""),
                "phone": detail.get("formatted_phone_number", ""),
                "rating": place.get("rating", ""),
                "types": place.get("types", []),
            })
        return businesses
    except Exception as e:
        return []


async def get_place_detail(place_id: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=10) as h:
            r = await h.get(
                "https://maps.googleapis.com/maps/api/place/details/json",
                params={
                    "place_id": place_id,
                    "fields": "website,formatted_phone_number,opening_hours",
                    "key": GOOGLE_PLACES_KEY
                }
            )
            r.raise_for_status()
            return r.json().get("result", {})
    except Exception:
        return {}


async def scrape_site(url: str) -> tuple[str, str]:
    if not url:
        return "", ""
    try:
        async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as h:
            r = await h.get(url)
            soup = BeautifulSoup(r.text, "html.parser")
            for tag in soup(["script", "style", "nav", "footer"]):
                tag.decompose()
            text = soup.get_text(separator=" ", strip=True)
            emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", r.text)
            contact = ""
            if emails:
                contact = "Email: " + emails[0]
            return text[:3000], contact.strip()
    except Exception:
        return "", ""


async def find_owner(business_name: str, location: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=10) as h:
            r = await h.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers={"Accept": "application/json", "X-Subscription-Token": BRAVE_API_KEY},
                params={"q": f'"{business_name}" {location} owner OR founder OR "owned by"', "count": 3}
            )
            results = r.json().get("web", {}).get("results", [])
        snippets = " ".join(res.get("description", "") for res in results)
        if not snippets:
            return "Not found"
        msg = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=60,
            messages=[{"role": "user", "content": f"Extract the owner or founder name from this text. Reply with just the name or 'Not found'.\n\n{snippets[:500]}"}]
        )
        return msg.content[0].text.strip()
    except Exception:
        return "Not found"


def analyze_lead(name: str, website: str, address: str, phone: str, rating: str, site_text: str) -> dict:
    has_website = "yes" if website else "no"
    msg = claude.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=400,
        messages=[{"role": "user", "content": f"""Analyze this local business as a potential AI/digital services lead.

Business: {name}
Address: {address}
Phone: {phone}
Google Rating: {rating}
Has Website: {has_website}
Website: {website}
Website content: {site_text[:2000]}

Reply in EXACTLY this format:
WHAT_THEY_NEED: [specific service, e.g. AI chatbot, automated booking, website build, email automation, review management]
WHY_THEY_NEED_IT: [1-2 sentences — be specific about their gap based on rating, no website, outdated site, etc.]
"""}]
    )
    text = msg.content[0].text
    result = {"what_they_need": "", "why_they_need_it": ""}
    for line in text.strip().split("\n"):
        if line.startswith("WHAT_THEY_NEED:"):
            result["what_they_need"] = line.replace("WHAT_THEY_NEED:", "").strip()
        elif line.startswith("WHY_THEY_NEED_IT:"):
            result["why_they_need_it"] = line.replace("WHY_THEY_NEED_IT:", "").strip()
    return result


async def process_business(biz: dict, location: str) -> dict:
    website = biz.get("website", "")
    phone = biz.get("phone", "")
    name = biz.get("name", "")
    address = biz.get("address", "")
    rating = str(biz.get("rating", "No rating"))

    site_text, email = await scrape_site(website)
    analysis = analyze_lead(name, website, address, phone, rating, site_text)
    owner = await find_owner(name, location)

    contact_parts = []
    if phone:
        contact_parts.append(f"Phone: {phone}")
    if email:
        contact_parts.append(email)
    if address:
        contact_parts.append(f"Address: {address}")

    return {
        "name": name,
        "url": website or "No website",
        "what_they_need": analysis["what_they_need"],
        "why_they_need_it": analysis["why_they_need_it"],
        "contact_info": " | ".join(contact_parts) if contact_parts else "Not found",
        "owner_name": owner,
    }


async def scout_leads(location: str) -> list[dict]:
    all_leads = []
    for biz_type in BUSINESS_TYPES:
        businesses = await places_search(biz_type, location)
        tasks = [process_business(biz, location) for biz in businesses]
        results = await asyncio.gather(*tasks)
        all_leads.extend(results)
    return all_leads


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
    return templates.TemplateResponse("index.html", {"request": request, "leads": leads, "location": location})


@app.post("/download")
async def download(location: str = Form(...), session: str | None = Cookie(default=None)):
    if not check_auth(session):
        return RedirectResponse(url="/login", status_code=303)
    leads = await scout_leads(location)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["name", "url", "what_they_need", "why_they_need_it", "contact_info", "owner_name"])
    writer.writeheader()
    writer.writerows(leads)
    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=leads_{location.replace(' ', '_')}.csv"},
    )
