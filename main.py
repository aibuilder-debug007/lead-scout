import os
import csv
import io
import httpx
import asyncio
from bs4 import BeautifulSoup
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import anthropic

app = FastAPI()
templates = Jinja2Templates(directory="templates")

BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


async def search_businesses(query: str, count: int = 10) -> list[dict]:
    url = "https://api.search.brave.com/res/v1/web/search"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    }
    params = {"q": query, "count": count, "result_filter": "web"}

    async with httpx.AsyncClient(timeout=15) as client_http:
        resp = await client_http.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

    results = []
    for item in data.get("web", {}).get("results", []):
        results.append({"title": item.get("title", ""), "url": item.get("url", ""), "description": item.get("description", "")})
    return results


async def scrape_website(url: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client_http:
            resp = await client_http.get(url)
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            text = soup.get_text(separator=" ", strip=True)
            return text[:3000]
    except Exception:
        return ""


def analyze_business(business_name: str, url: str, description: str, website_text: str) -> dict:
    prompt = f"""You are an AI sales analyst. Analyze this business and recommend an AI service they could benefit from.

Business: {business_name}
Website: {url}
Description: {description}
Website content: {website_text}

Respond in this exact format:
BUSINESS_TYPE: [what kind of business this is]
PAIN_POINT: [their biggest operational pain point in 1 sentence]
AI_SERVICE: [specific AI service they need, e.g. "AI chatbot for customer inquiries", "automated booking assistant", "AI email follow-up system"]
PITCH: [2-3 sentence personalized outreach message you would send them]
"""

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}],
    )
    text = message.content[0].text

    result = {"business_type": "", "pain_point": "", "ai_service": "", "pitch": ""}
    for line in text.strip().split("\n"):
        if line.startswith("BUSINESS_TYPE:"):
            result["business_type"] = line.replace("BUSINESS_TYPE:", "").strip()
        elif line.startswith("PAIN_POINT:"):
            result["pain_point"] = line.replace("PAIN_POINT:", "").strip()
        elif line.startswith("AI_SERVICE:"):
            result["ai_service"] = line.replace("AI_SERVICE:", "").strip()
        elif line.startswith("PITCH:"):
            result["pitch"] = line.replace("PITCH:", "").strip()
    return result


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "leads": None})


@app.post("/search", response_class=HTMLResponse)
async def search(request: Request, niche: str = Form(...), location: str = Form(...), count: int = Form(10)):
    query = f"{niche} {location}"
    businesses = await search_businesses(query, count)

    leads = []
    for biz in businesses:
        website_text = await scrape_website(biz["url"])
        analysis = analyze_business(biz["title"], biz["url"], biz["description"], website_text)
        leads.append({
            "name": biz["title"],
            "url": biz["url"],
            "business_type": analysis["business_type"],
            "pain_point": analysis["pain_point"],
            "ai_service": analysis["ai_service"],
            "pitch": analysis["pitch"],
        })

    return templates.TemplateResponse("index.html", {"request": request, "leads": leads, "query": query})


@app.post("/download")
async def download(niche: str = Form(...), location: str = Form(...), count: int = Form(10)):
    query = f"{niche} {location}"
    businesses = await search_businesses(query, count)

    leads = []
    for biz in businesses:
        website_text = await scrape_website(biz["url"])
        analysis = analyze_business(biz["title"], biz["url"], biz["description"], website_text)
        leads.append({
            "Name": biz["title"],
            "Website": biz["url"],
            "Business Type": analysis["business_type"],
            "Pain Point": analysis["pain_point"],
            "Recommended AI Service": analysis["ai_service"],
            "Pitch": analysis["pitch"],
        })

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["Name", "Website", "Business Type", "Pain Point", "Recommended AI Service", "Pitch"])
    writer.writeheader()
    writer.writerows(leads)
    output.seek(0)

    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=leads_{query.replace(' ', '_')}.csv"},
    )
