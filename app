import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st
from io import BytesIO

# ---------- Helpers ----------

def parse_mongolian_date(raw: str) -> str:
    now = datetime.now()
    raw = raw.strip()
    if "Өнөөдөр" in raw:
        time_part = raw.replace("Өнөөдөр", "").strip()
        return f"{now:%Y-%m-%d} {time_part}"
    elif "Өчигдөр" in raw:
        time_part = raw.replace("Өчигдөр", "").strip()
        yesterday = now - timedelta(days=1)
        return f"{yesterday:%Y-%m-%d} {time_part}"
    return raw

def get_last_page(base_url: str) -> int:
    response = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    page_nums = {
        int(m.group(1)) for a in soup.find_all('a', href=True)
        if (m := re.search(r'[?&]page=(\d+)', a['href']))
    }
    return max(page_nums) if page_nums else 1

def scrape_detail_page(url: str) -> dict | None:
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        title_el = soup.select_one("#ad-title")
        price_meta = soup.select_one('meta[itemprop="price"]')
        sku_el = soup.select_one('span[itemprop="sku"]')
        loc_el = soup.select_one("span[itemprop='address']") or soup.select_one("#show-post-render-app a[href] span")
        date_el = soup.find(lambda tag: tag.name == "span" and "Нийтэлсэн:" in tag.text)

        props = {}
        for li in soup.select("ul.chars-column > li"):
            key = li.select_one(".key-chars")
            val = li.select_one(".value-chars")
            if key and val:
                k = key.get_text(strip=True).rstrip(":")
                props[k] = val.get_text(strip=True)

        return {
            "Title": title_el.get_text(strip=True) if title_el else None,
            "Price": price_meta["content"] if price_meta else None,
            "Ad_ID": sku_el.get_text(strip=True) if sku_el else None,
            "Location": loc_el.get_text(strip=True) if loc_el else None,
            "Date": parse_mongolian_date(date_el.get_text(strip=True).replace("Нийтэлсэн:", "").strip()) if date_el else None,
            "URL": url,
            **props
        }
    except Exception as e:
        st.warning(f"Failed to scrape {url}: {e}")
        return None

def scrape_category(base_url: str, max_workers: int) -> pd.DataFrame:
    total_pages = get_last_page(base_url)
    links = []
    for page in range(1, total_pages + 1):
        page_url = f"{base_url}?page={page}"
        resp = requests.get(page_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        anchors = soup.select("a.mask[href^='/adv/']")
        links += [f"https://www.unegui.mn{a['href']}" for a in anchors]

    st.info(f"Found {len(links)} ads.")
    total_links = len(links)
    progress = st.progress(0)
    scraped = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(scrape_detail_page, url) for url in links]
        for i, future in enumerate(as_completed(futures), start=1):
            data = future.result()
            if data:
                scraped.append(data)
            progress.progress(min(i / total_links, 1.0))

    return pd.DataFrame(scraped)


# ---------- Categories ----------

categories = {
    "apartments": {
        "label": "Орон сууц зарна",
        "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/oron-suuts-zarna/",
        "default_output": "unegui_apartments.xlsx"
    },
    "land": {
        "label": "Газар зарна",
        "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/gazar/",
        "default_output": "unegui_land.xlsx"
    },
    "commercial": {
        "label": "Худалдаа үйлчилгээний талбай",
        "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/hudaldaa-jlchilgeenij-talbaj-zarna/",
        "default_output": "unegui_commercial.xlsx"
    },
    "houses": {
        "label": "АОС, хаус, зуслан",
        "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/a-o-s-hauszuslan/",
        "default_output": "unegui_houses.xlsx"
    },
    "factory_warehouse": {
        "label": "Үйлдвэр, агуулах, объект",
        "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/obekt/",
        "default_output": "unegui_factory_warehouse.xlsx"
    },
    "ger_fenced": {
        "label": "Хашаа байшин, гэр",
        "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/hashaa-bajshin/",
        "default_output": "unegui_ger_fenced.xlsx"
    },
    "office": {
        "label": "Ажлын байр, оффис",
        "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/azhlyin-bajroffis-zarna/",
        "default_output": "unegui_office.xlsx"
    },
    "garage_storage": {
        "label": "Гараж, склад, контейнер",
        "url": "https://www.unegui.mn/l-hdlh/l-hdlh-zarna/garazhskladkont-r/",
        "default_output": "unegui_garage_storage.xlsx"
    }
}

# ---------- Streamlit App ----------

st.title("🏘️ Unegui.mn Real Estate Scraper")
st.markdown("Scrape real estate listings from **Unegui.mn** by category and export them to Excel.")

selected = st.multiselect(
    "Choose categories to scrape:",
    options=list(categories.keys()),
    format_func=lambda k: categories[k]["label"],
    default=[]
)

max_workers = st.slider("Max parallel scraping threads", 1, 50, 20)
output_path = st.text_input("Output folder path (leave as './' for current directory)", value="./")

if st.button("Start Scraping") and selected:
    for key in selected:
        cat = categories[key]
        st.subheader(f"📂 {cat['label']}")
        df = scrape_category(cat["url"], max_workers)
        st.success(f"✅ Scraped {len(df)} ads.")

        st.dataframe(df)

        filename = st.text_input(f"Save as filename for {cat['label']}", value=cat["default_output"], key=key)
        full_path = output_path.rstrip("/\\") + "/" + filename

        # Save locally
        try:
            df.to_excel(full_path, index=False)
            st.info(f"Saved to: `{full_path}`")
        except Exception as e:
            st.error(f"Saving error: {e}")

        # Download
        buf = BytesIO()
        df.to_excel(buf, index=False)
        buf.seek(0)
        st.download_button(
            label="📥 Download Excel",
            data=buf,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
