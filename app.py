import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st
from io import BytesIO

# ---------- Helpers ----------

def parse_mongolian_date(raw: str) -> datetime:
    # Parses Mongolian dates (today, yesterday) or ISO strings
    now = datetime.now()
    text = raw.strip()
    if "Өнөөдөр" in text:
        time_part = text.replace("Өнөөдөр", "").strip()
        return datetime.strptime(f"{now:%Y-%m-%d} {time_part}", "%Y-%m-%d %H:%M")
    if "Өчигдөр" in text:
        time_part = text.replace("Өчигдөр", "").strip()
        yesterday = now - timedelta(days=1)
        return datetime.strptime(f"{yesterday:%Y-%m-%d} {time_part}", "%Y-%m-%d %H:%M")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return now


def get_last_page(base_url: str) -> int:
    # Determine number of pages
    resp = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "html.parser")
    nums = [int(m.group(1)) for a in soup.find_all('a', href=True)
            for m in [re.search(r'[?&]page=(\d+)', a['href'])] if m]
    return max(nums) if nums else 1


def scrape_detail_page(url: str) -> dict | None:
    # Scrape details for a single ad
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        title_el = soup.select_one("#ad-title")
        price_meta = soup.select_one('meta[itemprop="price"]')
        sku_el = soup.select_one('span[itemprop="sku"]')
        loc_el = soup.select_one("span[itemprop='address']") or soup.select_one("#show-post-render-app a[href] span")
        date_span = soup.find(lambda tag: tag.name == "span" and "Нийтэлсэн:" in tag.text)

        date_val = None
        if date_span:
            raw_date = date_span.get_text(strip=True).replace("Нийтэлсэн:", "").strip()
            date_val = parse_mongolian_date(raw_date)

        props = {}
        for li in soup.select("ul.chars-column > li"):
            k_el = li.select_one(".key-chars")
            v_el = li.select_one(".value-chars")
            if k_el and v_el:
                key = k_el.get_text(strip=True).rstrip(":")
                val = v_el.get_text(strip=True)
                props[key] = val

        return {
            "Title": title_el.get_text(strip=True) if title_el else None,
            "Price": price_meta["content"] if price_meta else None,
            "Ad_ID": sku_el.get_text(strip=True) if sku_el else None,
            "Location": loc_el.get_text(strip=True) if loc_el else None,
            "Date": date_val,
            "URL": url,
            **props
        }
    except Exception as e:
        st.warning(f"Failed to scrape {url}: {e}")
        return None


def scrape_category(base_url: str, max_workers: int) -> pd.DataFrame:
    # Scrape all ads in a category with progress bar
    pages = get_last_page(base_url)
    links = []
    for p in range(1, pages + 1):
        url_page = f"{base_url}?page={p}"
        resp = requests.get(url_page, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        anchors = soup.select("a.mask[href^='/adv/']")
        links += [f"https://www.unegui.mn{a['href']}" for a in anchors]

    total = len(links)
    st.info(f"Found {total} ads to scrape.")
    progress = st.progress(0)
    data_list = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(scrape_detail_page, u) for u in links]
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            if res:
                data_list.append(res)
            progress.progress(min(i / total, 1.0))

    return pd.DataFrame(data_list)


# ---------- Categories ----------

categories = {
    "apartments": {"label": "Орон сууц зарна", "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/oron-suuts-zarna/", "default_output": "unegui_apartments.xlsx"},
    "land":       {"label": "Газар зарна",      "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/gazar/",      "default_output": "unegui_land.xlsx"},
    "commercial": {"label": "Худалдаа үйлчилгээний талбай", "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/hudaldaa-jlchilgeenij-talbaj-zarna/", "default_output": "unegui_commercial.xlsx"},
    "houses":     {"label": "АОС, хаус, зуслан",   "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/a-o-s-hauszuslan/",    "default_output": "unegui_houses.xlsx"},
    "factory_warehouse": {"label": "Үйлдвэр, агуулах, объект", "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/obekt/", "default_output": "unegui_factory_warehouse.xlsx"},
    "ger_fenced": {"label": "Хашаа байшин, гэр", "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/hashaa-bajshin/", "default_output": "unegui_ger_fenced.xlsx"},
    "office":     {"label": "Ажлын байр, оффис", "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/azhlyin-bajroffis-zarna/", "default_output": "unegui_office.xlsx"},
    "garage_storage": {"label": "Гараж, склад, контейнер", "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/garazhskladkont-r/", "default_output": "unegui_garage_storage.xlsx"}
}

# ---------- Streamlit App ----------

st.title("🏘️ Unegui.mn Web Scraper")
st.markdown("Unegui.mn дэх үл хөдлөх хөрөнгийн зар")

# Date mode
mode = st.radio("Өдөр :", ["All Dates", "Custom Range"], index=1)
if mode == "Custom Range":
    start_date = st.date_input("Start date", value=datetime.now().date() - timedelta(days=7))
    end_date = st.date_input("End date", value=datetime.now().date())

# Category selection
selected = st.multiselect(
    "Choose categories to scrape:",
    options=list(categories.keys()),
    format_func=lambda k: categories[k]["label"],
    default=[]
)

max_workers = st.slider("Max parallel scraping threads", 1, 50, 20)
output_path = st.text_input("Output folder path", value="./")

if st.button("Start Scraping") and selected:
    for key in selected:
        info = categories[key]
        st.subheader(info["label"])
        df = scrape_category(info["url"], max_workers)
        if mode == "Custom Range":
            df = df[df["Date"].between(
                datetime.combine(start_date, datetime.min.time()),
                datetime.combine(end_date, datetime.max.time())
            )]
            st.success(f"Scraped {len(df)} ads between {start_date} and {end_date}.")
        else:
            st.success(f"Scraped {len(df)} ads (all dates).")

        st.dataframe(df)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = "all" if mode == "All Dates" else f"{start_date}_{end_date}"
        default_name = info["default_output"].replace(".xlsx", f"_{suffix}_{ts}.xlsx")
        filename = st.text_input("Filename", value=default_name, key=key)
        full_path = output_path.rstrip("/\\") + "/" + filename
        try:
            df.to_excel(full_path, index=False)
            st.info(f"Saved to {full_path}")
        except Exception as e:
            st.error(f"Saving error: {e}")
        buf = BytesIO(); df.to_excel(buf, index=False); buf.seek(0)
        st.download_button("Download Excel", data=buf, file_name=filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
