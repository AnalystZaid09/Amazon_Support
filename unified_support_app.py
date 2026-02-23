import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import zipfile
import pypdf
import pdfplumber
import warnings
from datetime import datetime
from openpyxl.styles import Font, Alignment

warnings.filterwarnings('ignore', category=FutureWarning)

# ==========================================
# PAGE CONFIGURATION
# ==========================================
st.set_page_config(
    page_title="Amazon Support Unified Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================
# PROFESSIONAL STYLING
# ==========================================
st.markdown("""
<style>
    .main { background-color: #F8FAFC; }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #f0f2f6;
        border-radius: 4px 4px 0px 0px;
        padding: 8px 16px;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background-color: #ffffff;
        border-bottom: 2px solid #ff4b4b;
    }
    .metric-container {
        background: white;
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #E5E7EB;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    div[data-testid="stMetricValue"] {
        font-size: 24px;
        font-weight: 700;
        color: #1F2937;
    }
    div[data-testid="stMetricLabel"] {
        font-size: 14px;
        color: #6B7280;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# UTILITY FUNCTIONS
# ==========================================

def normalize_text(val):
    if pd.isna(val): return None
    return str(val).strip()

def normalize_sku(val):
    if pd.isna(val): return None
    val = str(val).strip()
    if val.endswith(".0"): val = val[:-2]
    return val

@st.cache_data
def convert_to_excel(df, sheet_name="Report"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

def format_currency(val):
    if pd.isna(val): return "₹ 0.00"
    return f"₹ {val:,.2f}"

# ==========================================
# ADVERTISEMENT EXTRACTION LOGIC
# ==========================================

def clean_campaign_name_final(name_list):
    full_name = " ".join(name_list).strip()
    noise_patterns = [
        r"\(?Exclusive\)?", r"Total amount billed.*INR", r"Total adjustments.*INR",
        r"Total amount tax included.*INR", r"Portfolio name.*?:", r"Page \d+ of \d+",
        r"Amazon Seller Services.*", r"8th Floor, Brigade GateWay.*", r"Trade Center, No 26/1.*",
        r"Dr Raj Kumar Road.*", r"Malleshwaram.*", r"Bangalore, Karnataka.*",
        r"Summary of Portfolio Charges.*", r"Campaign\s+Campaign Type\s+Clicks.*"
    ]
    for pattern in noise_patterns:
        full_name = re.sub(pattern, "", full_name, flags=re.IGNORECASE)
    return full_name.replace("  ", " ").strip(" :,\"")

def get_total_amount_from_bottom(pdf_obj):
    full_text = ""
    try:
        if hasattr(pdf_obj, 'pages'): # pypdf or pdfplumber
            for page in pdf_obj.pages:
                text = page.extract_text()
                if text: full_text += text + "\n"
    except Exception:
        pass
    
    flat = full_text.replace("\n", " ").replace("\r", " ").replace(",", "").lower()
    patterns = [
        r"total\s*amount\s*\(tax\s*included\)\s*([\d,]+\.\d{2})",
        r"total\s*tax\s*included.*?([\d,]+\.\d{2})",
        r"total\s*amount\s*\(tax\s*included\)\s*inr\s*([\d,]+\.\d{2})",
        r"total\s*amount.*?tax\s*included.*?([\d,]+\.\d{2})",
        r"total.*?tax\s*included.*?inr\s*([\d,]+\.\d{2})",
        r"total\s*amount.*?([\d,]+\.\d{2})"
    ]
    for pattern in patterns:
        match = re.search(pattern, flat, re.IGNORECASE)
        if match: return float(match.group(1))
    return 0.0

def process_invoice(pdf_file):
    pdf_bytes = pdf_file.read()
    pdf_file.seek(0)
    
    try:
        # Try pypdf
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        final_total = get_total_amount_from_bottom(reader)
        first_page_text = (reader.pages[0].extract_text() or "").replace('\n', ' ')
        inv_num = re.search(r"Invoice Number\s*[:\s]*(\S+)", first_page_text)
        inv_date = re.search(r"Invoice Date\s*[:\s]*(\d{2}-\d{2}-\d{4})", first_page_text)
        meta = {
            "num": inv_num.group(1).strip() if inv_num else "N/A",
            "date": inv_date.group(1).strip() if inv_date else "N/A",
            "total": float(final_total)
        }
        
        rows = []
        name_accum = []
        is_table = False
        for page in reader.pages:
            text = page.extract_text()
            if not text: continue
            lines = text.split('\n')
            for line in lines:
                line = line.strip()
                if "Campaign" in line and "Clicks" in line:
                    is_table = True
                    name_accum = []
                    continue
                if not is_table: continue
                metric_match = re.search(r"(SPONSORED\s+(?:PRODUCTS|BRANDS|DISPLAY))\s+(-?\d+)\s+(-?[\d,.]+)(?:\s*INR)?\s+(-?[\d,.]+)(?:\s*INR)?", line, re.IGNORECASE)
                if metric_match:
                    name_part = line[:metric_match.start()].strip()
                    if name_part: name_accum.append(name_part)
                    rows.append({
                        "Campaign": clean_campaign_name_final(name_accum),
                        "Campaign Type": metric_match.group(1),
                        "Clicks": int(metric_match.group(2)),
                        "Amount": float(metric_match.group(4).replace(',', '')),
                        "Invoice Number": meta["num"],
                        "Brand": None
                    })
                    name_accum = []
                else:
                    if any(k in line for k in ["FROM", "Trade Center", "Invoice Number", "Summary"]):
                        name_accum = []
                        continue
                    name_accum.append(line)
        
        if rows: return rows
    except:
        pass
        
    # Fallback to pdfplumber
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            final_total = get_total_amount_from_bottom(pdf)
            rows = []
            for page in pdf.pages:
                table = page.extract_table()
                if not table: continue
                name_accum = []
                for row in table:
                    clean_row = [str(cell).strip() if cell else "" for cell in row]
                    row_str = " ".join(clean_row)
                    metric_match = re.search(r"(SPONSORED\s+(?:PRODUCTS|BRANDS|DISPLAY))\s+(-?\d+)\s+(-?[\d,.]+)(?:\s*INR)?\s+(-?[\d,.]+)(?:\s*INR)?", row_str, re.IGNORECASE)
                    if metric_match:
                        possible_name = row_str[:metric_match.start()].strip()
                        if possible_name: name_accum.append(possible_name)
                        rows.append({
                            "Campaign": clean_campaign_name_final(name_accum),
                            "Campaign Type": metric_match.group(1).upper(),
                            "Clicks": int(metric_match.group(2)),
                            "Amount": float(metric_match.group(4).replace(',', '')),
                            "Invoice Number": "N/A",
                            "Brand": None
                        })
                        name_accum = []
                    else:
                        if any(k in row_str.upper() for k in ["CAMPAIGN", "CLICKS", "FROM", "TRADE CENTER", "INVOICE NUMBER", "SUMMARY"]):
                            name_accum = []
                            continue
                        if any(c for c in clean_row if c): name_accum.append(row_str)
            return rows
    except:
        return []

# ==========================================
# NCEMI HELPERS
# ==========================================

def fill_sku_from_report(payment_order, report_df):
    # Detect order and sku columns dynamically based on previous scripts
    order_col = report_df.columns[4] if len(report_df.columns) > 4 else report_df.columns[0]
    sku_col = report_df.columns[13] if len(report_df.columns) > 13 else report_df.columns[1]
    
    report_df[order_col] = report_df[order_col].apply(normalize_sku)
    report_df[sku_col] = report_df[sku_col].apply(normalize_sku)
    
    lookup = report_df.dropna(subset=[order_col]).drop_duplicates(order_col).set_index(order_col)[sku_col].to_dict()
    
    mask = payment_order["Sku"].isna()
    payment_order.loc[mask, "Sku"] = payment_order.loc[mask, "order id"].map(lookup)
    return payment_order

# ==========================================
# DYSON HELPERS
# ==========================================

def get_dyson_available_months(zip_files):
    """Scan ZIP files and return unique months found in Invoice Date column"""
    month_names = {
        1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
        7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"
    }
    found_months = set()
    try:
        for zip_file in zip_files:
            with zipfile.ZipFile(zip_file) as z:
                csv_files = [name for name in z.namelist() if name.endswith('.csv')]
                for csv_name in csv_files:
                    with z.open(csv_name) as f:
                        temp_df = pd.read_csv(f, usecols=["Invoice Date"])
                        dates = pd.to_datetime(temp_df["Invoice Date"], errors='coerce')
                        for m in dates.dt.month.dropna().unique():
                            found_months.add(month_names[int(m)])
            zip_file.seek(0)  # Reset file pointer so it can be read again
    except Exception:
        pass
    ordered = ["January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]
    return [m for m in ordered if m in found_months]


def process_dyson_data(zip_files, pm_file, promo_file, past_months):
    """Process B2B/B2C Dyson data and calculate support"""
    try:
        # ---------- READ FILES ----------
        all_dfs = []
        for zip_file in zip_files:
            with zipfile.ZipFile(zip_file) as z:
                csv_files = [name for name in z.namelist() if name.endswith('.csv')]
                for csv_name in csv_files:
                    with z.open(csv_name) as f:
                        temp_df = pd.read_csv(f)
                        all_dfs.append(temp_df)

        df = pd.concat(all_dfs, ignore_index=True)

        # Clean and prepare data
        df["Sku"] = df["Sku"].astype(str).str.strip()
        df["Asin"] = df["Asin"].astype(str).str.strip()
        PM = pd.read_excel(pm_file)
        Promo = pd.read_excel(promo_file)
        PM["Amazon Sku Name"] = PM["Amazon Sku Name"].astype(str).str.strip()
        PM["ASIN"] = PM["ASIN"].astype(str).str.strip()
        Promo["ASIN"] = Promo["ASIN"].astype(str).str.strip()

        # ---------- STEP 1: BRAND MAP via SKU ----------
        brand_map_sku = PM.groupby("Amazon Sku Name", as_index=True)["Brand"].first()
        df["Brand"] = df["Sku"].map(brand_map_sku)

        # Move Brand column after Sku
        cols = list(df.columns)
        if "Sku" in cols and "Brand" in cols:
            sku_idx = cols.index("Sku")
            cols.remove("Brand")
            cols.insert(sku_idx + 1, "Brand")
            df = df[cols]

        # ---------- STEP 2: REMOVE SELECTED PAST-MONTH REFUNDS ----------
        month_map = {
            "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
            "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12
        }

        df["Invoice Date"] = pd.to_datetime(df["Invoice Date"], errors='coerce')

        if past_months:
            selected_month_nums = [month_map[m] for m in past_months]
            past_month_refund_mask = (
                (df["Invoice Date"].dt.month.isin(selected_month_nums)) &
                (df["Transaction Type"].astype(str).str.strip().str.upper() == "REFUND")
            )
            df = df[~past_month_refund_mask].copy()

        # ---------- DYSON ONLY ----------
        dyson_df = df[df["Brand"].notna() & (df["Brand"].astype(str).str.strip().str.upper() == "DYSON")].copy()

        # ---------- ORDER STATUS ----------
        cancel_orders = set(
            dyson_df[dyson_df["Transaction Type"].astype(str).str.strip().str.upper() == "CANCEL"]["Order Id"]
        )

        dyson_df["Order Status"] = np.where(
            dyson_df["Order Id"].isin(cancel_orders),
            "Cancel",
            dyson_df["Transaction Type"]
        )

        # Move Order Status after Order Id
        cols = list(dyson_df.columns)
        order_idx = cols.index("Order Id")
        cols.remove("Order Status")
        cols.insert(order_idx + 1, "Order Status")
        dyson_df = dyson_df[cols]

        # ---------- PROCESSED DATA (BEFORE PIVOT) ----------
        processed_df = dyson_df.copy()

        # ---------- PIVOT ----------
        pivot = pd.pivot_table(
            dyson_df,
            index="Asin",
            columns="Order Status",
            values="Quantity",
            aggfunc="sum",
            fill_value=0,
            margins=False
        ).reset_index()

        # ---------- NET SALE ----------
        pivot["Net Sale / Actual Shipment"] = (
            pivot.get("Shipment", 0) - pivot.get("Refund", 0)
        )

        # ---------- PROMO MAP ----------
        pivot["SKU CODE"] = pivot["Asin"].map(Promo.groupby("ASIN")["SKU Code"].first())
        pivot["SSP"] = pivot["Asin"].map(Promo.groupby("ASIN")["SSP"].first())
        pivot["Cons Promo"] = pivot["Asin"].map(Promo.groupby("ASIN")["Cons Promo"].first())
        pivot["Margin %"] = pivot["Asin"].map(Promo.groupby("ASIN")["Margin"].first()) * 100

        # ---------- SUPPORT ----------
        pivot["Support"] = (
            (pivot["SSP"] - pivot["Cons Promo"])
            * (1 - pivot["Margin %"] / 100)
        )

        pivot["SUPPORT AS PER NET SALE"] = (
            pivot["Support"].fillna(0)
            * pivot["Net Sale / Actual Shipment"].fillna(0)
        )

        # ---------- BASE AMOUNT (EXCLUDING 18% GST) ----------
        pivot["Base Amount"] = (pivot["SUPPORT AS PER NET SALE"] / 1.18).round(2)

        # ---------- CLEAN NUMERIC ----------
        pivot.replace("", np.nan, inplace=True)

        exclude_cols = ["Asin", "SKU CODE"]
        numeric_cols = [col for col in pivot.columns if col not in exclude_cols]

        for col in numeric_cols:
            pivot[col] = pd.to_numeric(pivot[col], errors="coerce").fillna(0)

        # ---------- GRAND TOTAL ----------
        grand_total = {}
        for col in pivot.columns:
            if col == "Asin":
                grand_total[col] = "Grand Total"
            elif col == "SKU CODE":
                grand_total[col] = ""
            elif col in numeric_cols:
                grand_total[col] = pivot[col].sum()
            else:
                grand_total[col] = 0

        pivot = pd.concat([pivot, pd.DataFrame([grand_total])], ignore_index=True)

        # Convert SKU CODE to string to avoid Arrow serialization issues
        pivot["SKU CODE"] = pivot["SKU CODE"].astype(str)

        return pivot, processed_df

    except Exception as e:
        st.error(f"Error processing Dyson data: {str(e)}")
        return None, None


def convert_dyson_df_to_csv(df):
    """Convert dataframe to CSV for Dyson downloads"""
    return df.to_csv(index=False).encode('utf-8')

# ==========================================
# SIDEBAR - GLOBAL UPLOADS
# ==========================================
st.sidebar.title("📤 Data Upload Center")

st.sidebar.subheader("💎 Essential Master Data")
pm_file = st.sidebar.file_uploader("Purchase Master (PM)", type=["xlsx", "xls"], key="pm_global")
portfolio_file = st.sidebar.file_uploader("Portfolio Report (Ads Mapping)", type=["xlsx", "xls"], key="portfolio_global")

st.sidebar.markdown("---")

st.sidebar.subheader("📊 Support Report Files")
coupon_file = st.sidebar.file_uploader("Coupon Orders (TXT)", type=["txt"], key="coupon_up")
exchange_file = st.sidebar.file_uploader("Exchange Data (Excel)", type=["xlsx", "xls"], key="exchange_up")
freebies_file = st.sidebar.file_uploader("Freebies Orders (TXT)", type=["txt"], key="freebies_up")
ncemi_payment_file = st.sidebar.file_uploader("NCEMI Payment (CSV)", type=["csv"], key="ncemi_pay_up")
ncemi_support_files = st.sidebar.file_uploader("NCEMI B2B/B2C Files", type=["csv", "zip"], accept_multiple_files=True, key="ncemi_sup_up")
adv_files = st.sidebar.file_uploader("Advertisement Invoices (PDF)", type=["pdf"], accept_multiple_files=True, key="adv_up")
rev_log_file = st.sidebar.file_uploader("Replacement Logistic (CSV)", type=["csv"], key="rev_log_up")

st.sidebar.markdown("---")

st.sidebar.subheader("🏭 Bergner Support")
bergner_orders_file = st.sidebar.file_uploader("Bergner Orders (Excel)", type=["xlsx", "xls"], key="bergner_orders_up")
bergner_support_file = st.sidebar.file_uploader("Bergner Support File (Excel)", type=["xlsx", "xls"], key="bergner_sup_up")

st.sidebar.subheader("🧮 Dyson Support")
dyson_b2b_zips = st.sidebar.file_uploader("Dyson B2B Report (ZIP)", type=["zip"], accept_multiple_files=True, key="dyson_b2b_up")
dyson_b2c_zips = st.sidebar.file_uploader("Dyson B2C Report (ZIP)", type=["zip"], accept_multiple_files=True, key="dyson_b2c_up")
dyson_promo_file = st.sidebar.file_uploader("Dyson Promo (Excel)", type=["xlsx", "xls"], key="dyson_promo_up")
dyson_invoice_file = st.sidebar.file_uploader("Dyson Invoice (Excel)", type=["xlsx", "xls"], key="dyson_invoice_up")
dyson_invoice_promo_file = st.sidebar.file_uploader("Dyson Invoice Promo CN (Excel)", type=["xlsx", "xls"], key="dyson_inv_promo_up")

st.sidebar.subheader("📦 Tramontina Support")
tramontina_orders_file = st.sidebar.file_uploader("Tramontina Orders (Excel)", type=["xlsx", "xls"], key="tramo_orders_up")
tramontina_bau_file = st.sidebar.file_uploader("Tramontina BAU Offer (Excel)", type=["xlsx", "xls"], key="tramo_bau_up")

st.sidebar.markdown("---")

st.sidebar.subheader("🏭 Bergner Secondary")
bergner_sec_orders_file = st.sidebar.file_uploader("Bergner Sec Orders (TXT)", type=["txt", "tsv", "csv"], key="bergner_sec_orders_up")
bergner_sec_file = st.sidebar.file_uploader("Bergner Sec Support File (Excel)", type=["xlsx", "xls"], key="bergner_sec_up")

st.sidebar.subheader("📦 Tramontina Secondary")
tramontina_sec_orders_file = st.sidebar.file_uploader("Tramontina Sec Orders (TXT)", type=["txt", "tsv", "csv"], key="tramo_sec_orders_up")
tramontina_sec_file = st.sidebar.file_uploader("Tramontina Sec Support File (Excel)", type=["xlsx", "xls"], key="tramo_sec_up")

# ==========================================
# DATA LOADING & INITIAL MAPPING
# ==========================================
brand_mapping = {}
if pm_file:
    pm_df = pd.read_excel(pm_file)
    pm_df["ASIN"] = pm_df["ASIN"].astype(str)
    brand_mapping = pm_df.drop_duplicates("ASIN").set_index("ASIN")["Brand"].to_dict()

# ==========================================
# MAIN TABS INITIALIZATION
# ==========================================
st.title("🚀 Amazon Support Unified Dashboard")

# ==========================================
# FILE FORMAT INSTRUCTIONS
# ==========================================
with st.expander("📖 File Format Instructions — Click to see expected headers for all files", expanded=False):
    st.markdown("### Required Column Headers for Each File")
    st.markdown("Make sure your files have the following column headers (case-sensitive) before uploading.")

    st.markdown("---")
    st.markdown("#### 💎 Purchase Master (PM) — Excel (.xlsx)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `ASIN` | Amazon Standard Identification Number |
| `Brand` | Brand name |
| `Amazon Sku Name` | SKU name (used for Dyson mapping) |
    """)

    st.markdown("---")
    st.markdown("#### 🏷️ Coupon Orders — TXT (Tab-separated)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `asin` | Product ASIN |
| `product-name` | Product name |
| `item-status` | Order status (Cancelled rows excluded) |
| `promotion-ids` | Promotion identifiers (filtered for PLM) |
| `item-promotion-discount` | Discount amount |
| `quantity` | Order quantity |
| `purchase-date` | Purchase date |
| `ship-postal-code` | Shipping postal code |
    """)

    st.markdown("---")
    st.markdown("#### 🔄 Exchange Data — Excel (.xlsx)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `brand` | Brand name |
| `order_day` | Order date |
| `seller funding` | Seller funding amount |
| `liquidator funding` | Liquidator funding amount |
| `order_id` | Order identifier |
| `buyback_category` | Exchange category |
| `forward_flag_status` | Status flag |
| `total_discount_claimed` | Total discount |
    """)

    st.markdown("---")
    st.markdown("#### 🎁 Freebies Orders — TXT (Tab-separated)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `asin` | Product ASIN |
| `product-name` | Product name |
| `item-status` | Order status (Cancelled rows excluded) |
| `promotion-ids` | Promotion identifiers (filtered for BOGO) |
| `item-price` | Item price |
| `quantity` | Order quantity |
    """)

    st.markdown("---")
    st.markdown("#### 💳 NCEMI Payment — CSV")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `type` | Transaction type (Transfer / Service Fee) |
| `Sku` | Product SKU |
| `total` | Total amount |
| `other transaction fees` | Transaction fees |
| `other` | Other fees |
    """)

    st.markdown("#### 💳 NCEMI B2B/B2C — CSV or ZIP (containing CSV)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `Sku` | Product SKU |
| `Asin` | Product ASIN |
    """)

    st.markdown("---")
    st.markdown("#### 📢 Advertisement Invoices — PDF")
    st.markdown("PDF invoices from Amazon Ads. No specific column headers needed — data is extracted automatically.")

    st.markdown("#### 📢 Portfolio Report (Ads Mapping) — Excel (.xlsx)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| Any column containing `campaign` or `portfolio` | Campaign name for mapping |
| Any column containing `brand` | Brand name |
    """)

    st.markdown("---")
    st.markdown("#### 🔄 Replacement Logistic — CSV (header at row 13)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `type` | Transaction type (filtered for 'Order') |
| `Sku` | Product SKU |
| `product sales` | Product sales amount (filtered for 0) |
| `quantity` | Quantity |
| `total` | Total amount |
    """)

    st.markdown("---")
    st.markdown("#### 🏭 Bergner Orders — Excel (.xlsx)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `asin` | Product ASIN |
| `product-name` | Product name |
| `item-status` | Order status (Cancelled rows excluded) |
| `item-price` | Item price |
| `quantity` | Order quantity |
    """)

    st.markdown("#### 🏭 Bergner Support File — Excel (.xlsx)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `ASIN` | Amazon ASIN |
| `P/L` | Profit/Loss per unit |
| `Support Approved` | Approved support quantity |
    """)

    st.markdown("---")
    st.markdown("#### 🧮 Dyson B2B/B2C Report — ZIP (containing CSV)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `Sku` | Product SKU |
| `Asin` | Product ASIN |
| `Quantity` | Order quantity |
| `Transaction Type` | Shipment / Refund / Cancel |
| `Order Id` | Order identifier |
| `Invoice Date` | Invoice date (used for month filtering) |
    """)

    st.markdown("#### 🧮 Dyson Promo — Excel (.xlsx)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `ASIN` | Amazon ASIN |
| `SKU Code` | SKU code |
| `SSP` | Standard Selling Price |
| `Cons Promo` | Consumer Promo price |
| `Margin` | Margin (decimal, e.g. 0.10 for 10%) |
    """)

    st.markdown("#### 🧮 Dyson Invoice — Excel (.xlsx)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `Material_Cd` | Material code |
| `Qty` | Quantity |
| `Total_Val` | Total value |
    """)

    st.markdown("#### 🧮 Dyson Invoice Promo CN — Excel (.xlsx)")
    st.markdown("""
| Column Position | Description |
|--------|-------------|
| Column D (4th column) | Lookup key (Material code) |
| Column L (12th column) | Consumer Promo value to return |
    """)

    st.markdown("---")
    st.markdown("#### 📦 Tramontina Orders — Excel (.xlsx)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `asin` | Product ASIN |
| `product-name` or `product_name` | Product name |
| `item-status` | Order status (Cancelled rows excluded) |
| `item-price` | Item price |
| `quantity` | Order quantity |
    """)

    st.markdown("#### 📦 Tramontina BAU Offer — Excel (.xlsx, 3 sheets)")
    st.markdown("""
| Sheet Name | Key Columns |
|--------|-------------|
| `Amazon BAU Price` | `ASIN`, `P/L` |
| `Freebie` | `ASIN`, `P/L` |
| `Coupon` | `ASIN`, `P/L` |
    """)

    st.markdown("---")
    st.markdown("#### 🏭 Bergner Secondary Orders — TXT (Tab-separated)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `asin` | Product ASIN |
| `quantity` | Order quantity |
| `item-status` | Order status (Cancelled rows excluded) |
    """)

    st.markdown("#### 🏭 Bergner Secondary Support — Excel (.xlsx, header at row 3)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `ASIN` | Amazon ASIN |
| `P/L` | Profit/Loss per unit |
    """)

    st.markdown("---")
    st.markdown("#### 📦 Tramontina Secondary Orders — TXT (Tab-separated)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `asin` | Product ASIN |
| `quantity` | Order quantity |
| `item-status` | Order status (Cancelled rows excluded) |
    """)

    st.markdown("#### 📦 Tramontina Secondary Support — Excel (.xlsx)")
    st.markdown("""
| Column | Description |
|--------|-------------|
| `ASIN` | Amazon ASIN |
| `P/l` | Profit/Loss per unit |
    """)

if not (pm_file or coupon_file or exchange_file or freebies_file or ncemi_payment_file or adv_files or rev_log_file or bergner_orders_file or dyson_b2b_zips or dyson_b2c_zips or dyson_invoice_file or tramontina_orders_file or bergner_sec_orders_file or tramontina_sec_orders_file):
    st.info("👋 Welcome! Please upload your data files in the sidebar to generate reports.")
    st.markdown("""
    ### 📂 Expected Files:
    - **Product Master (PM)**: Excel with `ASIN` and `Brand` columns.
    - **Coupon/Freebies**: Tab-separated TXT order reports.
    - **Exchange**: Excel with `brand` and `seller funding`.
    - **NCEMI**: Payment CSV + B2B/B2C order reports for SKU mapping.
    - **Advertisement**: PDF Invoices and Portfolio Excel for campaign mapping.
    - **Bergner**: Orders Excel + Bergner Support Excel.
    - **Dyson**: B2B/B2C ZIP reports + Dyson Promo Excel.
    - **Tramontina**: Orders Excel + BAU Offer Excel (3 sheets).
    - **Bergner Secondary**: Orders TXT + Bergner Support Excel.
    - **Tramontina Secondary**: Orders TXT + Tramontina Support Excel.
    """)
    st.stop()

tabs = st.tabs(["🏠 Combined Summary", "🏷️ Coupon", "🔄 Exchange", "🎁 Freebies", "💳 NCEMI", "📢 Advertisement", "🔄 Replacement Logistic", "🏭 Bergner", "🧮 Dyson", "📦 Tramontina", "🏭 Bergner Secondary", "📦 Tramontina Secondary"])

combined_results = []

# ==========================================
# TAB 1: COMBINED SUMMARY (PLACEHOLDER FOR NOW)
# ==========================================
with tabs[0]:
    st.header("🏠 Brand-wise Combined Support Report")
    # Will be populated after other tabs process their data

# ==========================================
# TAB 2: COUPON
# ==========================================
with tabs[1]:
    st.header("🏷️ Coupon Report Analysis")
    if coupon_file and pm_file:
        c_df = pd.read_csv(coupon_file, sep="\t", dtype={"ship-postal-code": str})
        c_df = c_df[c_df["product-name"] != "-"]
        c_df = c_df[~c_df["item-status"].str.strip().str.lower().eq("cancelled")]
        c_df["asin"] = c_df["asin"].astype(str)
        c_df["Brand"] = c_df["asin"].map(brand_mapping)
        c_df = c_df[c_df["promotion-ids"].astype(str).str.contains("PLM", case=False, na=False)]
        c_df["item-promotion-discount"] = pd.to_numeric(c_df["item-promotion-discount"], errors="coerce").abs()
        
        st.success(f"✅ Final data after filtering (PLM promotions only): {len(c_df)} records")
        
        # Sub-tabs for Coupon
        c_tab1, c_tab2, c_tab3 = st.tabs(["📋 Master Report", "🔍 Brand Filtered Report", "📊 Pivot Table Report"])
        
        with c_tab1:
            st.subheader("Master Report")
            st.write(f"**Total Records:** {len(c_df)}")
            if not c_df.empty:
                st.write(f"**Date Range:** {c_df['purchase-date'].min()} to {c_df['purchase-date'].max()}")
                st.dataframe(c_df, use_container_width=True, height=400)
                st.download_button("📥 Download Master Report", convert_to_excel(c_df, 'Master Report'), "coupon_master_report.xlsx")

        with c_tab2:
            st.subheader("Brand Filtered Report")
            brands = sorted(c_df[c_df['Brand'].notna()]['Brand'].unique().tolist())
            selected_brands_c = st.multiselect("Select Brand(s)", options=brands, default=brands, key="c_brand_sel")
            
            if selected_brands_c:
                filtered_c = c_df[c_df['Brand'].isin(selected_brands_c)].copy()
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Orders", len(filtered_c))
                col2.metric("Total Discount", format_currency(filtered_c["item-promotion-discount"].sum()))
                col3.metric("Avg Discount", format_currency(filtered_c["item-promotion-discount"].mean()))
                
                st.dataframe(filtered_c, use_container_width=True, height=400)
                st.download_button("📥 Download Filtered Report", convert_to_excel(filtered_c, 'Filtered Report'), "coupon_filtered_report.xlsx")
            else:
                st.warning("Please select at least one brand.")

        with c_tab3:
            st.subheader("Pivot Table Report - Discount by Brand")
            c_pivot = c_df.groupby("Brand")["item-promotion-discount"].sum().reset_index()
            c_pivot.columns = ["Brand", "Coupon Discount"]
            c_pivot = c_pivot.sort_values(by="Coupon Discount", ascending=False)
            
            # Grand Total
            grand_total_c = c_pivot["Coupon Discount"].sum()
            summary_c = pd.DataFrame({"Brand": ["Grand Total"], "Coupon Discount": [grand_total_c]})
            c_pivot_display = pd.concat([c_pivot, summary_c], ignore_index=True)
            
            st.dataframe(
                c_pivot_display.style.format({"Coupon Discount": format_currency})
                .background_gradient(subset=["Coupon Discount"], cmap="YlOrRd"),
                use_container_width=True
            )
            st.download_button("📥 Download Pivot Table", convert_to_excel(c_pivot_display, 'Pivot Table'), "coupon_pivot_table.xlsx")
            
            st.bar_chart(c_pivot.set_index("Brand")["Coupon Discount"])
            combined_results.append(c_pivot)
    else:
        st.warning("Please upload both Order TXT and PM file for Coupon analysis.")

# ==========================================
# TAB 3: EXCHANGE
# ==========================================
with tabs[2]:
    st.header("🔄 Exchange Report Analysis")
    if exchange_file:
        e_df = pd.read_excel(exchange_file)
        e_df["Date"] = pd.to_datetime(e_df["order_day"], format="mixed", errors="coerce")
        e_df["Month"] = e_df["Date"].dt.strftime("%b-%y")
        e_df["brand_norm"] = e_df["brand"].apply(lambda x: str(x).strip().title())
        
        st.success(f"✅ Exchange data loaded successfully! Total records: {len(e_df)}")
        
        # Sub-tabs for Exchange
        e_tab1, e_tab2, e_tab3 = st.tabs(["📈 Pivot Table (All Data)", "📋 Pivot Table (Month-wise)", "📊 Additional Analysis"])
        
        with e_tab1:
            st.subheader("Brand-wise Seller Funding (All Data)")
            e_pivot_all = e_df.groupby("brand_norm")["seller funding"].sum().reset_index()
            e_pivot_all.columns = ["Brand", "Total Seller Funding"]
            
            # Grand Total
            e_summary_all = pd.DataFrame({"Brand": ["Grand Total"], "Total Seller Funding": [e_pivot_all["Total Seller Funding"].sum()]})
            e_pivot_all_display = pd.concat([e_pivot_all, e_summary_all], ignore_index=True)
            
            st.dataframe(
                e_pivot_all_display.style.format({"Total Seller Funding": format_currency}),
                use_container_width=True
            )
            st.download_button("📥 Download Pivot Table (All Data)", convert_to_excel(e_pivot_all_display, "Exchange All Data"), "exchange_pivot_all.xlsx")

        with e_tab2:
            months = sorted(e_df["Month"].dropna().unique())
            sel_month = st.selectbox("Select Month", options=months, key="e_month_sel")
            st.subheader(f"Brand-wise Seller Funding ({sel_month})")
            
            e_filtered_month = e_df[e_df["Month"] == sel_month].copy()
            e_pivot_month = e_filtered_month.groupby("brand_norm")["seller funding"].sum().reset_index()
            e_pivot_month.columns = ["Brand", "Total Seller Funding"]
            
            # Grand Total
            e_summary_month = pd.DataFrame({"Brand": ["Grand Total"], "Total Seller Funding": [e_pivot_month["Total Seller Funding"].sum()]})
            e_pivot_month_display = pd.concat([e_pivot_month, e_summary_month], ignore_index=True)
            
            st.dataframe(
                e_pivot_month_display.style.format({"Total Seller Funding": format_currency}),
                use_container_width=True
            )
            st.download_button(f"📥 Download Pivot Table ({sel_month})", convert_to_excel(e_pivot_month_display, f"Exchange {sel_month}"), f"exchange_pivot_{sel_month}.xlsx")

        with e_tab3:
            st.subheader("Additional Insights")
            sub_tab_a, sub_tab_b, sub_tab_c = st.tabs(["Category Analysis", "Status Distribution", "Funding Breakdown"])
            
            with sub_tab_a:
                cat_summary = e_df.groupby("buyback_category").agg({
                    "order_id": "count",
                    "total_discount_claimed": "sum",
                    "seller funding": "sum"
                }).reset_index()
                st.dataframe(cat_summary, use_container_width=True)
                
            with sub_tab_b:
                status_summary = e_df["forward_flag_status"].value_counts().reset_index()
                st.dataframe(status_summary, use_container_width=True)
                
            with sub_tab_c:
                col1, col2 = st.columns(2)
                col1.metric("Total Liquidator Funding", format_currency(e_df['liquidator funding'].sum()))
                col2.metric("Total Seller Funding", format_currency(e_df['seller funding'].sum()))
                
                funding_brand = e_df.groupby("brand_norm").agg({
                    "liquidator funding": "sum",
                    "seller funding": "sum"
                }).reset_index()
                st.dataframe(funding_brand, use_container_width=True)

        # For Combined Summary
        e_pivot_final = e_df.groupby("brand_norm")["seller funding"].sum().reset_index()
        e_pivot_final.columns = ["Brand", "Exchange Funding"]
        combined_results.append(e_pivot_final)
    else:
        st.warning("Please upload Exchange Excel file.")

# ==========================================
# TAB 4: FREEBIES
# ==========================================
with tabs[3]:
    st.header("🎁 Freebies (BOGO) Analysis")
    if freebies_file and pm_file:
        f_df = pd.read_csv(freebies_file, sep="\t", dtype={"ship-postal-code": str})
        f_df = f_df[f_df["product-name"] != "-"]
        f_df = f_df[~f_df["item-status"].str.strip().str.lower().eq("cancelled")]
        f_df["asin"] = f_df["asin"].astype(str)
        f_df["Brand"] = f_df["asin"].map(brand_mapping)
        f_df = f_df[f_df["promotion-ids"].astype(str).str.contains("buy", case=False, na=False)]
        f_df["item-promotion-discount"] = pd.to_numeric(f_df["item-promotion-discount"], errors="coerce").abs()
        
        st.success(f"✅ Final data after filtering (Buy 1 Get 1 promotions only): {len(f_df)} records")
        
        # Sub-tabs for Freebies
        f_tab1, f_tab2, f_tab3 = st.tabs(["📋 Master Report", "🔍 Brand Filtered Report", "📊 Pivot Table Report"])
        
        with f_tab1:
            st.subheader("Master Report - Support Freebies (BOGO)")
            st.write(f"**Total Records:** {len(f_df)}")
            if not f_df.empty:
                st.write(f"**Date Range:** {f_df['purchase-date'].min()} to {f_df['purchase-date'].max()}")
                st.dataframe(f_df, use_container_width=True, height=400)
                st.download_button("📥 Download Master Report", convert_to_excel(f_df, 'Freebies Master'), "freebies_master_report.xlsx")

        with f_tab2:
            st.subheader("Brand Filtered Report - Support Freebies")
            brands_f = sorted(f_df[f_df['Brand'].notna()]['Brand'].unique().tolist())
            selected_brands_f = st.multiselect("Select Brand(s)", options=brands_f, default=brands_f, key="f_brand_sel")
            
            if selected_brands_f:
                filtered_f = f_df[f_df['Brand'].isin(selected_brands_f)].copy()
                col1, col2, col3, col4 = st.columns(4)
                total_f_discount = filtered_f["item-promotion-discount"].sum()
                col1.metric("Total Orders", len(filtered_f))
                col2.metric("Total Discount", format_currency(total_f_discount))
                col3.metric("Base Amount (excl. GST)", format_currency(total_f_discount / 1.18))
                col4.metric("Avg Discount", format_currency(filtered_f["item-promotion-discount"].mean()))
                
                st.dataframe(filtered_f, use_container_width=True, height=400)
                st.download_button("📥 Download Filtered Report", convert_to_excel(filtered_f, 'Freebies Filtered'), "freebies_filtered_report.xlsx")
            else:
                st.warning("Please select at least one brand.")

        with f_tab3:
            st.subheader("Pivot Table Report - Support Freebies by Brand")
            f_pivot = f_df.groupby("Brand")["item-promotion-discount"].sum().reset_index()
            f_pivot.columns = ["Brand", "Freebies Discount"]
            f_pivot["Base Amount"] = f_pivot["Freebies Discount"] / 1.18
            
            # Grand Total
            f_summary = pd.DataFrame({
                "Brand": ["Grand Total"], 
                "Freebies Discount": [f_pivot["Freebies Discount"].sum()],
                "Base Amount": [f_pivot["Base Amount"].sum()]
            })
            f_pivot_display = pd.concat([f_pivot, f_summary], ignore_index=True)
            
            st.dataframe(
                f_pivot_display.style.format({"Freebies Discount": format_currency, "Base Amount": format_currency})
                .background_gradient(subset=["Freebies Discount"], cmap="YlOrRd"),
                use_container_width=True
            )
            st.download_button("📥 Download Pivot Table", convert_to_excel(f_pivot_display, 'Freebies Pivot'), "freebies_pivot_table.xlsx")
            
            col_a, col_b = st.columns(2)
            col_a.write("**Total Discount (with GST)**")
            col_a.bar_chart(f_pivot.set_index("Brand")["Freebies Discount"])
            col_b.write("**Base Amount (excl. GST)**")
            col_b.bar_chart(f_pivot.set_index("Brand")["Base Amount"])
            
            combined_results.append(f_pivot[["Brand", "Freebies Discount"]])
    else:
        st.warning("Please upload both Order TXT and PM file for Freebies analysis.")

# ==========================================
# TAB 5: NCEMI
# ==========================================
with tabs[4]:
    st.header("💳 NCEMI Promotion Analysis")
    if ncemi_payment_file and pm_file:
        p_df = pd.read_csv(ncemi_payment_file, skiprows=11)
        n_df = p_df[p_df["type"] == "Order"].copy()
        
        for col in ["product sales", "total"]:
            n_df[col] = pd.to_numeric(n_df[col].astype(str).str.replace(",",""), errors="coerce")
            
        n_df = n_df[n_df["product sales"] == 0]
        n_df["Sku"] = n_df["Sku"].apply(normalize_sku)
        n_df["order id"] = n_df["order id"].apply(normalize_sku)
        
        if ncemi_support_files:
            for f in ncemi_support_files:
                try:
                    df_rep = pd.read_csv(f) if f.name.endswith(".csv") else None
                    if f.name.endswith(".zip"):
                        with zipfile.ZipFile(f) as z:
                            csv_name = [name for name in z.namelist() if name.endswith(".csv")][0]
                            with z.open(csv_name) as zf:
                                df_rep = pd.read_csv(zf)
                    if df_rep is not None:
                        n_df = fill_sku_from_report(n_df, df_rep)
                except Exception: pass

        pm_full = pd.read_excel(pm_file)
        # Assuming columns based on ncemi script: 2-SKU, 4-Manager, 6-Brand, 3-Vendor SKU, 7-Product Name
        sku_key = pm_full.columns[2]
        pm_unique = pm_full.drop_duplicates(sku_key)
        
        n_df["Brand"] = n_df["Sku"].map(pm_unique.set_index(sku_key)[pm_full.columns[6]])
        n_df["Brand Manager"] = n_df["Sku"].map(pm_unique.set_index(sku_key)[pm_full.columns[4]])
        n_df["Vendor SKU"] = n_df["Sku"].map(pm_unique.set_index(sku_key)[pm_full.columns[3]])
        n_df["Product Name"] = n_df["Sku"].map(pm_unique.set_index(sku_key)[pm_full.columns[7]])
        
        st.success(f"✅ Processed {len(n_df)} NCEMI records. {n_df['Sku'].notna().sum()} SKUs filled.")
        
        # Sub-tabs for NCEMI
        n_tab1, n_tab2, n_tab3, n_tab4 = st.tabs(["📈 Brand Analysis", "👥 Brand Manager Analysis", "💰 Service Fees", "📋 Raw Data"])
        
        with n_tab1:
            st.subheader("Brand-wise Summary")
            n_pivot_brand = n_df.groupby("Brand")["total"].sum().reset_index()
            n_pivot_brand["total"] = n_pivot_brand["total"].abs()
            
            grand_total_n = n_pivot_brand["total"].sum()
            summary_n = pd.DataFrame({"Brand": ["Grand Total"], "total": [grand_total_n]})
            n_pivot_brand_display = pd.concat([n_pivot_brand, summary_n], ignore_index=True)
            
            st.dataframe(n_pivot_brand_display.style.format({"total": format_currency}), use_container_width=True)
            st.download_button("📥 Download Brand Analysis", n_pivot_brand_display.to_csv(index=False).encode(), "ncemi_brand_analysis.csv")
            combined_results.append(n_pivot_brand.rename(columns={"total": "NCEMI Funding"}))

        with n_tab2:
            st.subheader("Brand Manager-wise Summary")
            n_pivot_mgr = n_df.groupby("Brand Manager")["total"].sum().reset_index()
            n_pivot_mgr["total"] = n_pivot_mgr["total"].abs()
            
            summary_mgr = pd.DataFrame({"Brand Manager": ["Grand Total"], "total": [n_pivot_mgr["total"].sum()]})
            n_pivot_mgr_display = pd.concat([n_pivot_mgr, summary_mgr], ignore_index=True)
            st.dataframe(n_pivot_mgr_display.style.format({"total": format_currency}), use_container_width=True)
            st.download_button("📥 Download Manager Analysis", n_pivot_mgr_display.to_csv(index=False).encode(), "ncemi_manager_analysis.csv")

        with n_tab3:
            st.subheader("Service Fees Breakdown")
            sf_df = p_df[p_df["type"] == "Service Fee"].copy()
            for col in ["other transaction fees", "other", "total"]:
                sf_df[col] = pd.to_numeric(sf_df[col].astype(str).str.replace(",",""), errors="coerce")
            
            summary_sf = sf_df[["other transaction fees", "other", "total"]].sum()
            c1, c2, c3 = st.columns(3)
            c1.metric("Transaction Fees", format_currency(summary_sf["other transaction fees"]))
            c2.metric("Other Fees", format_currency(summary_sf["other"]))
            c3.metric("Total Service Fees", format_currency(summary_sf["total"]))
            
            st.dataframe(sf_df, use_container_width=True)
            st.download_button("📥 Download Service Fees", sf_df.to_csv(index=False).encode(), "ncemi_service_fees.csv")

        with n_tab4:
            st.subheader("Raw Data with Mapping")
            st.dataframe(n_df, use_container_width=True)
            st.download_button("📥 Download Raw Data", n_df.to_csv(index=False).encode(), "ncemi_raw_data.csv")
    else:
        st.warning("Please upload NCEMI Payment CSV and PM file.")

# ==========================================
# TAB 6: ADVERTISEMENT
# ==========================================
with tabs[5]:
    st.header("📢 Advertisement Expense Analysis")
    if adv_files:
        all_adv_rows = []
        for f in adv_files:
            rows = process_invoice(f)
            all_adv_rows.extend(rows)
        
        a_df = pd.DataFrame(all_adv_rows)
        
        if not a_df.empty:
            a_df["With GST Amount (18%)"] = a_df["Amount"] * 1.18
            
            if portfolio_file:
                port_df = pd.read_excel(portfolio_file)
                # Clean column names
                port_df.columns = port_df.columns.astype(str).str.strip().str.replace("\n", " ")
                
                c_col = [c for c in port_df.columns if "campaign" in c.lower() or "portfolio" in c.lower()]
                b_col = [c for c in port_df.columns if "brand" in c.lower()]
                
                if c_col and b_col:
                    port_map = port_df.drop_duplicates(c_col[0]).set_index(c_col[0])[b_col[0]].to_dict()
                    a_df["Brand"] = a_df["Campaign"].map(port_map)
                    st.success(f"✅ Mapping complete! {a_df['Brand'].notna().sum()} campaigns matched.")
            
            # Sub-tabs for Advertisement
            a_tab1, a_tab2, a_tab3 = st.tabs(["📋 Master Report", "🔍 Brand Filtered Report", "📊 Pivot Table Report"])
            
            with a_tab1:
                st.subheader("Master Report - All Invoices")
                st.write(f"**Total Records:** {len(a_df)}")
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Clicks", f"{a_df['Clicks'].sum():,}")
                col2.metric("Total Amount", format_currency(a_df['Amount'].sum()))
                col3.metric("Total With GST", format_currency(a_df['With GST Amount (18%)'].sum()))
                
                st.dataframe(a_df, use_container_width=True, height=400)
                st.download_button("📥 Download Master Report", convert_to_excel(a_df, 'Ads Master'), "ads_master_report.xlsx")

            with a_tab2:
                st.subheader("Brand Filtered Report")
                if "Brand" in a_df.columns:
                    brands_a = sorted(a_df["Brand"].dropna().unique().tolist())
                    sel_brands_a = st.multiselect("Select Brand(s)", options=brands_a, default=brands_a, key="a_brand_sel")
                    
                    if sel_brands_a:
                        filt_a = a_df[a_df['Brand'].isin(sel_brands_a)].copy()
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Campaigns", filt_a["Campaign"].nunique())
                        c2.metric("Total Clicks", f"{filt_a['Clicks'].sum():,}")
                        c3.metric("Total Amount", format_currency(filt_a['Amount'].sum()))
                        
                        st.dataframe(filt_a, use_container_width=True, height=400)
                        st.download_button("📥 Download Filtered Report", convert_to_excel(filt_a, 'Ads Filtered'), "ads_filtered_report.xlsx")
                else:
                    st.warning("Please upload Portfolio Report for brand filtering.")

            with a_tab3:
                st.subheader("Pivot Table Report - Brand Summary")
                if "Brand" in a_df.columns:
                    a_pivot = a_df.groupby("Brand", dropna=False).agg({
                        "Campaign": "count",
                        "Clicks": "sum",
                        "Amount": "sum",
                        "With GST Amount (18%)": "sum"
                    }).reset_index()
                    a_pivot.columns = ["Brand", "Total Campaigns", "Total Clicks", "Total Amount (excl. GST)", "Total Amount (incl. GST)"]
                    
                    a_summary = pd.DataFrame({
                        "Brand": ["Grand Total"],
                        "Total Campaigns": [a_pivot["Total Campaigns"].sum()],
                        "Total Clicks": [a_pivot["Total Clicks"].sum()],
                        "Total Amount (excl. GST)": [a_pivot["Total Amount (excl. GST)"].sum()],
                        "Total Amount (incl. GST)": [a_pivot["Total Amount (incl. GST)"].sum()]
                    })
                    a_pivot_disp = pd.concat([a_pivot, a_summary], ignore_index=True)
                    st.dataframe(a_pivot_disp.style.format({
                        "Total Campaigns": "{:,.0f}", "Total Clicks": "{:,.0f}",
                        "Total Amount (excl. GST)": format_currency, "Total Amount (incl. GST)": format_currency
                    }), use_container_width=True)
                    st.download_button("📥 Download Pivot Table", convert_to_excel(a_pivot_disp, 'Ads Pivot'), "ads_pivot_table.xlsx")
                    
                    st.bar_chart(a_pivot.set_index("Brand")["Total Amount (incl. GST)"])
                    combined_results.append(a_pivot[["Brand", "Total Amount (excl. GST)"]].rename(columns={"Total Amount (excl. GST)": "Ad Expense"}))
                else:
                    st.warning("Please upload Portfolio Report to generate pivot table.")
        else:
            st.error("Could not extract any advertisement data.")
    else:
        st.warning("Please upload Advertisement PDF invoices.")

# ==========================================
# TAB 7: REPLACEMENT LOGISTIC
# ==========================================
with tabs[6]:
    st.header("🔄 Replacement Logistic Analysis")
    if rev_log_file and pm_file:
        with st.spinner("Processing Replacement Logistic files..."):
            # 1. Read CSV with header at row 12 (0-indexed: 11)
            rl_df = pd.read_csv(rev_log_file, header=11, low_memory=False)

            # 2. Filter: type == "Order" AND product sales == 0
            rl_df = rl_df[
                (rl_df["type"].str.strip().str.lower() == "order") &
                (pd.to_numeric(rl_df["product sales"], errors="coerce") == 0)
            ]

            # 3. Drop rows where SKU is null/empty
            rl_df = rl_df[
                rl_df["Sku"].notna() &
                (rl_df["Sku"].astype(str).str.strip() != "")
            ]

            # 4. Clean SKU columns for matching
            rl_df["Sku"] = rl_df["Sku"].astype(str).str.strip().str.replace(".0", "", regex=False)
            
            # Map Brand and Brand Manager from PM
            pm_full_rl = pd.read_excel(pm_file)
            sku_col_pm = pm_full_rl.columns[2] # Based on NCEMI and Reverse_Logistic logic
            mgr_col_pm = pm_full_rl.columns[4]
            brand_col_pm = pm_full_rl.columns[6]
            
            pm_full_rl[sku_col_pm] = pm_full_rl[sku_col_pm].astype(str).str.strip().str.replace(".0", "", regex=False)
            
            brand_manager_map = pm_full_rl.set_index(sku_col_pm)[mgr_col_pm].to_dict()
            brand_map_rl = pm_full_rl.set_index(sku_col_pm)[brand_col_pm].to_dict()

            rl_df["Brand Manager"] = rl_df["Sku"].map(brand_manager_map)
            rl_df["Brand"] = rl_df["Sku"].map(brand_map_rl)

            # 5. Clean and convert total & quantity columns
            rl_df["total"] = (
                rl_df["total"]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.strip()
            )
            rl_df["total"] = pd.to_numeric(rl_df["total"], errors="coerce").fillna(0)
            rl_df["quantity"] = pd.to_numeric(rl_df["quantity"], errors="coerce").fillna(0)

        st.success(f"✅ Processed **{len(rl_df):,}** replacement logistic transactions")

        # Sub-tabs for Replacement Logistic
        rl_tab1, rl_tab2, rl_tab3 = st.tabs(["📊 Pivot Table Report", "👥 Brand Manager Analysis", "📋 Raw Data"])

        with rl_tab1:
            st.subheader("Brand-wise Replacement Logistic Summary")
            rl_pivot = rl_df.groupby("Brand").agg({
                "quantity": "sum",
                "total": "sum"
            }).reset_index()
            rl_pivot = rl_pivot.sort_values(by="total", ascending=True)
            
            # Grand Total
            rl_summary = pd.DataFrame({
                "Brand": ["Grand Total"],
                "quantity": [rl_pivot["quantity"].sum()],
                "total": [rl_pivot["total"].sum()]
            })
            rl_pivot_disp = pd.concat([rl_pivot, rl_summary], ignore_index=True)
            st.dataframe(
                rl_pivot_disp.style.format({"total": format_currency, "quantity": "{:,.0f}"})
                .background_gradient(subset=["total"], cmap="YlOrRd"),
                use_container_width=True
            )
            st.download_button("📥 Download Brand Summary", convert_to_excel(rl_pivot_disp, 'RL Brand Pivot'), "rl_brand_summary.xlsx")
            
            combined_results.append(rl_pivot[["Brand", "total"]].rename(columns={"total": "Replacement Logistic"}))

        with rl_tab2:
            st.subheader("Brand Manager-wise Replacement Logistic Summary")
            rl_pivot_mgr = rl_df.groupby("Brand Manager").agg({
                "quantity": "sum",
                "total": "sum"
            }).reset_index()
            rl_pivot_mgr = rl_pivot_mgr.sort_values(by="total", ascending=True)
            
            rl_mgr_summary = pd.DataFrame({
                "Brand Manager": ["Grand Total"],
                "quantity": [rl_pivot_mgr["quantity"].sum()],
                "total": [rl_pivot_mgr["total"].sum()]
            })
            rl_mgr_disp = pd.concat([rl_pivot_mgr, rl_mgr_summary], ignore_index=True)
            st.dataframe(rl_mgr_disp.style.format({"total": format_currency, "quantity": "{:,.0f}"}), use_container_width=True)
            st.download_button("📥 Download Manager Summary", convert_to_excel(rl_mgr_disp, 'RL Manager Pivot'), "rl_manager_summary.xlsx")

        with rl_tab3:
            st.subheader("Filtered Transaction Data")
            st.dataframe(rl_df, use_container_width=True)
            st.download_button("📥 Download Raw Data", convert_to_excel(rl_df, 'RL Raw Data'), "rl_raw_data.xlsx")
    else:
        st.warning("Please upload both Replacement Logistic CSV and PM file.")

# ==========================================
# TAB 8: BERGNER
# ==========================================
with tabs[7]:
    st.header("🏭 Bergner Support Analysis")
    if bergner_orders_file and pm_file and bergner_support_file:
        try:
            with st.spinner("Processing Bergner data..."):
                b_orders = pd.read_excel(bergner_orders_file)
                b_pm = pd.read_excel(pm_file)
                b_support = pd.read_excel(bergner_support_file, header=1)

                # Map Brand
                asin_brand_map = b_pm[["ASIN", "Brand"]].dropna().drop_duplicates(subset="ASIN").set_index("ASIN")["Brand"]
                b_orders["Brand"] = b_orders["asin"].map(asin_brand_map)

                # Reorder columns
                b_cols = list(b_orders.columns)
                b_cols.remove("Brand")
                b_insert_after = "product-name" if "product-name" in b_cols else "product_name"
                b_idx = b_cols.index(b_insert_after)
                b_cols.insert(b_idx + 1, "Brand")
                b_orders = b_orders[b_cols]

                # Clean data
                b_orders["item-price"] = b_orders["item-price"].replace(r"^\s*$", pd.NA, regex=True)
                b_orders = b_orders.dropna(subset=["item-price"])
                b_orders["item-price"] = pd.to_numeric(b_orders["item-price"], errors="coerce")
                b_orders = b_orders.dropna(subset=["item-price"])
                b_orders["quantity"] = pd.to_numeric(b_orders["quantity"], errors="coerce").fillna(0)

                # Pivot quantities
                pivot_qty = pd.pivot_table(b_orders, index=["Brand", "asin"], values="quantity", aggfunc="sum", fill_value=0).reset_index()
                pivot_qty.rename(columns={"quantity": "order_qty"}, inplace=True)

                # Map to Bergner Support
                asin_qty_map = pivot_qty.drop_duplicates(subset="asin").set_index("asin")["order_qty"]
                b_support["order qty"] = b_support["ASIN"].map(asin_qty_map)

                # Calculate P/L
                b_support["P/L"] = pd.to_numeric(b_support["P/L"], errors="coerce")
                b_support["order qty"] = pd.to_numeric(b_support["order qty"], errors="coerce").fillna(0)
                b_support["P/L on orders qty"] = (b_support["P/L"] * b_support["order qty"]).round(2)

                # Support Value
                if "Support Approved" in b_support.columns:
                    b_support["Support Value"] = b_support["P/L"] * b_support["Support Approved"]

                # Total row
                total_pl = b_support["P/L on orders qty"].sum()
                total_row = {col: "" for col in b_support.columns}
                total_row["P/L on orders qty"] = total_pl
                total_row["ASIN"] = "TOTAL"
                b_support = pd.concat([b_support, pd.DataFrame([total_row])], ignore_index=True)

            st.success(f"✅ Bergner data processed! Total P/L: ₹{total_pl:,.2f}")

            # Sub-tabs
            bg_tab1, bg_tab2, bg_tab3 = st.tabs(["📊 Bergner Support Analysis", "📋 Quantity Pivot", "📄 Processed Orders"])

            with bg_tab1:
                st.subheader("Bergner Support with P/L")
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Orders", f"{len(b_orders):,}")
                col2.metric("Unique ASINs", f"{len(pivot_qty):,}")
                col3.metric("Total P/L on Orders", f"₹{total_pl:,.2f}")
                st.dataframe(b_support, use_container_width=True, height=400)
                st.download_button("📥 Download Bergner Support", convert_to_excel(b_support, 'Bergner Support'), "bergner_support.xlsx")

            with bg_tab2:
                st.subheader("Order Quantity by Brand and ASIN")
                st.dataframe(pivot_qty, use_container_width=True, height=400)
                st.download_button("📥 Download Quantity Pivot", convert_to_excel(pivot_qty, 'Order Quantities'), "bergner_quantities.xlsx")

            with bg_tab3:
                st.subheader("Processed Orders Data")
                st.dataframe(b_orders.head(100), use_container_width=True, height=400)
                st.info(f"Showing first 100 rows of {len(b_orders):,} total records")

            # For Combined Summary - brand-wise P/L
            b_brand_pl = b_support[b_support["ASIN"] != "TOTAL"].copy()
            if "Brand" not in b_brand_pl.columns:
                b_brand_pl["Brand"] = b_brand_pl["ASIN"].map(asin_brand_map)
            b_brand_pivot = b_brand_pl.groupby("Brand")["P/L on orders qty"].sum().reset_index()
            b_brand_pivot.columns = ["Brand", "Bergner P/L"]
            b_brand_pivot["Bergner P/L"] = pd.to_numeric(b_brand_pivot["Bergner P/L"], errors="coerce").fillna(0)
            combined_results.append(b_brand_pivot)

        except Exception as e:
            st.error(f"❌ Error processing Bergner data: {str(e)}")
    else:
        st.warning("Please upload Bergner Orders, PM file, and Bergner Support Excel.")

# ==========================================
# TAB 9: DYSON
# ==========================================
with tabs[8]:
    st.header("🧮 Dyson Support Analysis")

    dy_tab1, dy_tab2, dy_tab3, dy_tab4 = st.tabs([
        "📊 B2B Analysis",
        "📈 B2C Analysis",
        "🔄 Combined Analysis",
        "🧾 Invoice Qty"
    ])

    def render_dyson_tab(dy_tab, key):
        """Render B2B, B2C or Combined sub-tab for Dyson"""
        with dy_tab:
            st.subheader(f"{key} Support Calculation")

            if key == "Combined":
                # Combined uses both B2B + B2C ZIPs from sidebar
                all_zips_for_scan = (dyson_b2b_zips if dyson_b2b_zips else []) + (dyson_b2c_zips if dyson_b2c_zips else [])
                available_months = get_dyson_available_months(all_zips_for_scan) if all_zips_for_scan else []
            else:
                zip_files_for_tab = dyson_b2b_zips if key == "B2B" else dyson_b2c_zips
                available_months = get_dyson_available_months(zip_files_for_tab) if zip_files_for_tab else []

            # Show past months multiselect only if ZIP files are uploaded
            if available_months:
                past_months = st.multiselect(
                    f"Select past months to remove Refund data ({key})",
                    options=available_months,
                    default=[],
                    key=f'dyson_past_months_{key}',
                    help="These months were found in Invoice Date column. Select which ones to remove Refund data from."
                )
            else:
                past_months = []

            # Calculate button
            if key == "Combined":
                if st.button(f"🔄 Calculate {key} Support", type="primary", use_container_width=True, key=f"dyson_calc_{key}"):
                    all_zips = (dyson_b2b_zips if dyson_b2b_zips else []) + (dyson_b2c_zips if dyson_b2c_zips else [])
                    if all_zips and pm_file and dyson_promo_file:
                        with st.spinner("Processing combined Dyson data..."):
                            pivot, processed = process_dyson_data(all_zips, pm_file, dyson_promo_file, past_months)
                            if pivot is not None:
                                st.session_state[f'dyson_{key}_pivot'] = pivot
                                st.session_state[f'dyson_{key}_processed'] = processed
                                st.success(f"✅ {key} data processed successfully!")
                    else:
                        st.warning("⚠️ Please upload at least one report ZIP and both PM/Promo files.")
            else:
                zip_files_for_tab = dyson_b2b_zips if key == "B2B" else dyson_b2c_zips
                if st.button(f"🔄 Calculate {key} Support", type="primary", use_container_width=True, key=f"dyson_calc_{key}"):
                    if zip_files_for_tab and pm_file and dyson_promo_file:
                        with st.spinner(f"Processing {key} Dyson data..."):
                            pivot, processed = process_dyson_data(zip_files_for_tab, pm_file, dyson_promo_file, past_months)
                            if pivot is not None:
                                st.session_state[f'dyson_{key}_pivot'] = pivot
                                st.session_state[f'dyson_{key}_processed'] = processed
                                st.success(f"✅ {key} data processed successfully!")
                    else:
                        st.warning("⚠️ Please upload ZIP file(s), PM file, and Dyson Promo file.")

            # -------- PROCESSED DATA --------
            if f'dyson_{key}_processed' in st.session_state:
                st.markdown("---")
                st.markdown("### 🧾 Processed Dyson Data (Before Pivot)")
                st.dataframe(
                    st.session_state[f'dyson_{key}_processed'],
                    height=350,
                    use_container_width=True
                )
                csv_proc = convert_dyson_df_to_csv(st.session_state[f'dyson_{key}_processed'])
                st.download_button(
                    label="📥 Download Processed Data (Before Pivot)",
                    data=csv_proc,
                    file_name=f"dyson_{key.lower()}_processed_before_pivot.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key=f"dyson_dl_proc_{key}"
                )
                st.markdown("---")

            # -------- FINAL RESULT --------
            if f'dyson_{key}_pivot' in st.session_state:
                result = st.session_state[f'dyson_{key}_pivot']

                # Key Metrics
                grand_total_row = result[result['Asin'] == 'Grand Total'].iloc[0]

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Shipments", f"{int(grand_total_row.get('Shipment', 0)):,}")
                with col2:
                    st.metric("Net Sales", f"{int(grand_total_row.get('Net Sale / Actual Shipment', 0)):,}")
                with col3:
                    metric_label = "Total Cancels" if key == "B2B" else "Total Refunds"
                    metric_value = grand_total_row.get('Cancel', 0) if key == "B2B" else grand_total_row.get('Refund', 0)
                    st.metric(metric_label, f"{int(metric_value):,}")
                with col4:
                    support_total = grand_total_row.get('SUPPORT AS PER NET SALE', 0)
                    st.metric("Total Support", format_currency(support_total))

                st.markdown("---")

                # Data table
                st.markdown("### 📊 Final Support Calculation")

                # Format numeric columns for display
                display_df = result.copy()
                numeric_cols_fmt = ['SSP', 'Cons Promo', 'Support', 'SUPPORT AS PER NET SALE', 'Base Amount']
                for col in numeric_cols_fmt:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(lambda x: format_currency(x) if pd.notna(x) else '-')

                # Highlight Grand Total row
                def highlight_gt(row):
                    if row['Asin'] == 'Grand Total':
                        return ['background-color: #dbeafe; font-weight: bold'] * len(row)
                    return [''] * len(row)

                st.dataframe(
                    display_df.style.apply(highlight_gt, axis=1),
                    use_container_width=True,
                    height=400
                )

                # Download button
                csv_final = convert_dyson_df_to_csv(result)
                st.download_button(
                    label=f"📥 Download {key} Final Results as CSV",
                    data=csv_final,
                    file_name=f"dyson_{key.lower()}_final_support_analysis.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key=f"dyson_dl_final_{key}"
                )

                # Add to combined summary
                dy_support_total = grand_total_row.get('SUPPORT AS PER NET SALE', 0)
                dy_combined_df = pd.DataFrame({"Brand": [f"Dyson ({key})"], f"Dyson {key} Support": [dy_support_total]})
                combined_results.append(dy_combined_df)

    # Render B2B, B2C, Combined tabs
    render_dyson_tab(dy_tab1, "B2B")
    render_dyson_tab(dy_tab2, "B2C")
    render_dyson_tab(dy_tab3, "Combined")

    # ---------------- INVOICE QTY REPORT ----------------
    with dy_tab4:
        st.subheader("🧾 Invoice Qty Report")
        st.info("Upload Invoice file and Promo CN file in the sidebar to generate report")

        handling_rate = st.number_input(
            "Enter Handling Charges (₹ per Qty)",
            min_value=0.0,
            value=270.0,
            step=10.0,
            key="dyson_handling_rate"
        )

        if st.button("🔄 Generate Invoice Qty Report", type="primary", use_container_width=True, key="dyson_invoice_calc"):
            if dyson_invoice_file is not None and dyson_invoice_promo_file is not None:
                try:
                    df_invoice = pd.read_excel(dyson_invoice_file)
                    df_invoice.columns = df_invoice.columns.str.strip()

                    promo_df = pd.read_excel(dyson_invoice_promo_file)
                    promo_df.columns = promo_df.columns.str.strip()

                    # ---- STEP 1: BASIC PIVOT ----
                    pivot_invoice = pd.pivot_table(
                        df_invoice,
                        index="Material_Cd",
                        values=["Qty", "Total_Val"],
                        aggfunc="sum",
                        fill_value=0,
                        margins=True,
                        margins_name="Grand Total"
                    ).reset_index()

                    df_invoice = pivot_invoice.copy()

                    # ---- CREATE CONSUMER PROMO (VLOOKUP Equivalent) ----
                    lookup_column = promo_df.columns[3]   # Column D
                    return_column = promo_df.columns[11]  # Column L (D to L = 9th column)
                    promo_map = promo_df.set_index(lookup_column)[return_column]
                    df_invoice["Consumer Promo"] = df_invoice["Material_Cd"].map(promo_map)

                    # ---------- CALCULATIONS ----------
                    df_invoice["Total Amount"] = df_invoice["Consumer Promo"].fillna(0) * df_invoice["Qty"]
                    df_invoice["1% CN"] = df_invoice["Total Amount"] * 0.01
                    df_invoice["Without GST (CN Base)"] = df_invoice["1% CN"] / 1.18
                    df_invoice["Wt Handling"] = handling_rate * df_invoice["Qty"]
                    df_invoice["Without GST per Handling"] = df_invoice["Wt Handling"] / 1.18
                    df_invoice["Total"] = df_invoice["1% CN"] + df_invoice["Wt Handling"]
                    df_invoice["Total Base"] = df_invoice["Total"] / 1.18

                    desired_order = ["Material_Cd", "Qty", "Total_Val", "Consumer Promo", "Total Amount",
                                     "1% CN", "Without GST (CN Base)", "Wt Handling",
                                     "Without GST per Handling", "Total", "Total Base"]
                    df_invoice = df_invoice[desired_order]

                    st.success("✅ Invoice Qty Report Generated Successfully!")

                    st.markdown("### 📊 Pivot Table")
                    st.dataframe(df_invoice, use_container_width=True, height=400)

                    csv_inv = convert_dyson_df_to_csv(df_invoice)
                    st.download_button(
                        label="📥 Download Invoice Qty Report",
                        data=csv_inv,
                        file_name="dyson_invoice_qty_report.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key="dyson_dl_invoice"
                    )

                except Exception as e:
                    st.error(f"Error processing Invoice Qty: {str(e)}")
            else:
                st.warning("⚠️ Please upload both Invoice file and Promo CN file in the sidebar.")

# ==========================================
# TAB 10: TRAMONTINA
# ==========================================
with tabs[9]:
    st.header("📦 Tramontina Support Analysis")
    if tramontina_orders_file and pm_file and tramontina_bau_file:
        try:
            with st.spinner("Processing Tramontina data..."):
                t_orders = pd.read_excel(tramontina_orders_file)
                t_pm = pd.read_excel(pm_file)
                t_bau_sheet = pd.read_excel(tramontina_bau_file, sheet_name="Amazon BAU Price")
                t_freebie_sheet = pd.read_excel(tramontina_bau_file, sheet_name="Freebie")
                t_coupon_sheet = pd.read_excel(tramontina_bau_file, sheet_name="Coupon")

                # Map Brand
                t_asin_brand_map = t_pm[["ASIN", "Brand"]].dropna().drop_duplicates(subset="ASIN").set_index("ASIN")["Brand"]
                t_orders["Brand"] = t_orders["asin"].map(t_asin_brand_map)

                # Reorder columns
                t_cols = list(t_orders.columns)
                t_cols.remove("Brand")
                t_insert_after = "product-name" if "product-name" in t_cols else "product_name"
                t_idx = t_cols.index(t_insert_after)
                t_cols.insert(t_idx + 1, "Brand")
                t_orders = t_orders[t_cols]

                # Clean data
                t_orders["item-price"] = pd.to_numeric(t_orders["item-price"].replace(r"^\s*$", pd.NA, regex=True), errors="coerce")
                t_orders = t_orders.dropna(subset=["item-price"])
                t_orders["quantity"] = pd.to_numeric(t_orders["quantity"], errors="coerce").fillna(0)

                # ASIN to quantity map
                t_asin_qty_map = t_orders.groupby("asin")["quantity"].sum().to_dict()

                # Shipped orders
                t_shipped = t_orders[t_orders['order-status'] == 'Shipped'].copy()
                t_shipped = t_shipped.sort_values(by='purchase-date', ascending=False)

                # State analysis
                t_state_counts = t_shipped['ship-state'].value_counts().reset_index()
                t_state_counts.columns = ['ship-state', 'count']
                t_state_analysis = pd.concat([
                    t_state_counts,
                    pd.DataFrame({'ship-state': ['TOTAL'], 'count': [t_state_counts['count'].sum()]})
                ], ignore_index=True)

                # BAU Price
                t_bau = t_bau_sheet.copy()
                t_bau["order qty"] = t_bau["ASIN"].map(t_asin_qty_map).fillna(0)
                t_bau["P/l"] = pd.to_numeric(t_bau["P/l"], errors="coerce").fillna(0)
                t_bau["P/l on orders qty"] = t_bau["P/l"] * t_bau["order qty"]
                t_total_pl = t_bau["P/l on orders qty"].sum()
                t_bau_final = pd.concat([
                    t_bau,
                    pd.DataFrame([{col: "" for col in t_bau.columns} | {"ASIN": "TOTAL", "P/l on orders qty": t_total_pl}])
                ], ignore_index=True)

                # Freebie
                t_freebie = t_freebie_sheet.copy()
                t_freebie["order qty"] = t_freebie["Freebie ASIN"].map(t_asin_qty_map).fillna(0)
                t_freebie["order qty"] = t_freebie["order qty"].where(~t_freebie.duplicated(subset="Freebie ASIN"), 0)
                t_freebie["MRP.1"] = pd.to_numeric(t_freebie["MRP.1"], errors="coerce").fillna(0)
                t_freebie["mrp on order qty"] = t_freebie["MRP.1"] * t_freebie["order qty"]
                t_total_mrp = t_freebie["mrp on order qty"].sum()
                t_freebie_final = pd.concat([
                    t_freebie,
                    pd.DataFrame([{col: "" for col in t_freebie.columns} | {"Freebie ASIN": "TOTAL", "mrp on order qty": t_total_mrp}])
                ], ignore_index=True)

                # Coupon
                t_coupon = t_coupon_sheet.copy()
                t_coupon["order qty"] = t_coupon["ASIN"].map(t_asin_qty_map).fillna(0)
                t_coupon["Coupon Amt"] = pd.to_numeric(t_coupon["Coupon Amt"], errors="coerce").fillna(0)
                t_coupon["coupon value on order qty"] = t_coupon["Coupon Amt"] * t_coupon["order qty"]
                t_total_coupon = t_coupon["coupon value on order qty"].sum()
                t_coupon_final = pd.concat([
                    t_coupon,
                    pd.DataFrame([{col: "" for col in t_coupon.columns} | {"ASIN": "TOTAL", "coupon value on order qty": t_total_coupon}])
                ], ignore_index=True)

            st.success(f"✅ Tramontina processed! Shipped: {len(t_shipped):,} | P/L: ₹{t_total_pl:,.2f} | Freebie MRP: ₹{t_total_mrp:,.2f} | Coupon: ₹{t_total_coupon:,.0f}")

            # Metrics
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Total Orders", f"{len(t_orders):,}")
            m2.metric("Shipped Orders", f"{len(t_shipped):,}")
            m3.metric("States", f"{len(t_state_analysis)-1}")
            m4.metric("Total P/L", f"₹{t_total_pl:,.2f}")
            m5.metric("Coupon Value", f"₹{t_total_coupon:,.0f}")

            # Sub-tabs
            tr_tab1, tr_tab2, tr_tab3, tr_tab4, tr_tab5 = st.tabs(["💰 BAU Price", "🎁 Freebie", "🎫 Coupon", "📦 Shipped Orders", "📊 State Analysis"])

            with tr_tab1:
                st.subheader("BAU Price Analysis")
                st.dataframe(t_bau_final, use_container_width=True, height=400)
                st.info(f"**Total P/L: ₹{t_total_pl:,.2f}**")
                st.download_button("📥 Download BAU Price", convert_to_excel(t_bau_final, 'BAU Price'), "tramontina_bau.xlsx")

            with tr_tab2:
                st.subheader("Freebie Analysis")
                st.dataframe(t_freebie_final, use_container_width=True, height=400)
                st.info(f"**Total MRP: ₹{t_total_mrp:,.2f}**")
                st.download_button("📥 Download Freebie", convert_to_excel(t_freebie_final, 'Freebie'), "tramontina_freebie.xlsx")

            with tr_tab3:
                st.subheader("Coupon Analysis")
                st.dataframe(t_coupon_final, use_container_width=True, height=400)
                st.info(f"**Total Coupon: ₹{t_total_coupon:,.2f}**")
                st.download_button("📥 Download Coupon", convert_to_excel(t_coupon_final, 'Coupon'), "tramontina_coupon.xlsx")

            with tr_tab4:
                st.subheader("Shipped Orders")
                st.dataframe(t_shipped.head(100), use_container_width=True, height=400)
                st.info(f"Showing first 100 of {len(t_shipped):,} shipped orders")
                st.download_button("📥 Download Shipped Orders", convert_to_excel(t_shipped, 'Shipped Orders'), "tramontina_shipped.xlsx")

            with tr_tab5:
                st.subheader("State-wise Distribution")
                st.dataframe(t_state_analysis, use_container_width=True)
                st.download_button("📥 Download State Analysis", convert_to_excel(t_state_analysis, 'State Analysis'), "tramontina_state.xlsx")

            # Combined download
            st.markdown("---")
            t_combined_buffer = io.BytesIO()
            with pd.ExcelWriter(t_combined_buffer, engine='xlsxwriter') as writer:
                t_shipped.to_excel(writer, index=False, sheet_name='Shipped Orders')
                t_state_analysis.to_excel(writer, index=False, sheet_name='State Analysis')
                t_bau_final.to_excel(writer, index=False, sheet_name='BAU Price')
                t_freebie_final.to_excel(writer, index=False, sheet_name='Freebie')
                t_coupon_final.to_excel(writer, index=False, sheet_name='Coupon')
            st.download_button("📥 Download All Tramontina Reports", t_combined_buffer.getvalue(), "tramontina_all_reports.xlsx")

            # For Combined Summary
            t_total_support = t_total_pl + t_total_mrp + t_total_coupon
            tramontina_combined = pd.DataFrame({"Brand": ["Tramontina"], "Tramontina Support": [t_total_support]})
            combined_results.append(tramontina_combined)

        except Exception as e:
            st.error(f"❌ Error processing Tramontina data: {str(e)}")
    else:
        st.warning("Please upload Tramontina Orders, PM file, and BAU Offer Excel.")

# ==========================================
# TAB 11: BERGNER SECONDARY
# ==========================================
with tabs[10]:
    st.header("🏭 Bergner Secondary Support")
    if bergner_sec_orders_file and pm_file and bergner_sec_file:
        try:
            with st.spinner("Processing Bergner Secondary data..."):
                # Load files
                bs_bergner = pd.read_excel(bergner_sec_file, header=1)
                bs_df = pd.read_csv(bergner_sec_orders_file, sep="\t", low_memory=False)
                bs_pm = pd.read_excel(pm_file)

                # Clean orders
                if 'product-name' in bs_df.columns:
                    bs_df = bs_df[bs_df['product-name'] != '-']
                bs_df = bs_df[bs_df['item-price'].notna() & (bs_df['item-price'].astype(str).str.strip() != '')]

                # Merge Brand from PM
                bs_df['asin'] = bs_df['asin'].astype(str)
                bs_pm['ASIN'] = bs_pm['ASIN'].astype(str)
                bs_df = bs_df.merge(bs_pm[['ASIN', 'Brand']], left_on='asin', right_on='ASIN', how='left')
                bs_df.drop(columns=['ASIN'], inplace=True)

                # Filter Bergner orders
                bs_df_bergner = bs_df[bs_df['Brand'] == 'Bergner'].copy()

                # Build pivot of quantity by ASIN & item-status
                bs_pivot = pd.pivot_table(bs_df, index='asin', columns='item-status', values='quantity',
                                          aggfunc='sum', fill_value=0)
                exclude_status = ['Cancelled']
                bs_pivot['Net Quantity'] = bs_pivot.loc[:, ~bs_pivot.columns.isin(exclude_status)].sum(axis=1)
                bs_pivot.columns.name = None
                bs_pivot.reset_index(inplace=True)
                bs_pivot.columns = bs_pivot.columns.str.strip()

                # Merge into Bergner file
                bs_bergner['ASIN'] = bs_bergner['ASIN'].astype(str)
                bs_pivot['asin'] = bs_pivot['asin'].astype(str)
                bs_bergner = bs_bergner.merge(bs_pivot[['asin', 'Net Quantity']], left_on='ASIN', right_on='asin', how='left')
                bs_bergner.rename(columns={'Net Quantity': 'Month Order Count'}, inplace=True)
                bs_bergner.drop(columns=['asin'], inplace=True)

                # Calculate Extra P&L
                bs_bergner['Month Order Count'] = pd.to_numeric(bs_bergner['Month Order Count'], errors='coerce').fillna(0)
                bs_bergner['P/L'] = pd.to_numeric(bs_bergner['P/L'], errors='coerce').fillna(0)
                bs_bergner['Extra P&L'] = (bs_bergner['Month Order Count'] * bs_bergner['P/L']).round(2)

                # Grand Total row
                bs_total_extra_pl = bs_bergner['Extra P&L'].sum()
                bs_total_row = pd.DataFrame({col: [''] for col in bs_bergner.columns})
                bs_total_row.iloc[0, bs_bergner.columns.get_loc('ASIN')] = 'Grand Total'
                bs_total_row['Extra P&L'] = bs_total_extra_pl
                bs_bergner = pd.concat([bs_bergner, bs_total_row], ignore_index=True)

            st.success(f"✅ Bergner Secondary processed! Grand Total Extra P&L: ₹{bs_total_extra_pl:,.2f}")

            # Sub-tabs
            bs_tab1, bs_tab2 = st.tabs(["📋 Bergner Orders", "📊 Bergner File with P&L"])

            with bs_tab1:
                st.subheader("Bergner Filtered Orders")
                st.caption(f"{len(bs_df_bergner):,} rows")
                st.dataframe(bs_df_bergner, use_container_width=True, height=400)
                st.download_button("📥 Download Bergner Orders", convert_to_excel(bs_df_bergner, 'Bergner Orders'), "bergner_sec_orders.xlsx")

            with bs_tab2:
                st.subheader("Bergner File with Month Order Count & Extra P&L")
                st.caption(f"Grand Total Extra P&L: ₹{bs_total_extra_pl:,.2f}")
                st.dataframe(bs_bergner, use_container_width=True, height=400)
                st.download_button("📥 Download Bergner Support File", convert_to_excel(bs_bergner, 'Bergner File'), "bergner_sec_file.xlsx")

            # Combined download
            bs_combined_buf = io.BytesIO()
            with pd.ExcelWriter(bs_combined_buf, engine='xlsxwriter') as writer:
                bs_df_bergner.to_excel(writer, index=False, sheet_name='Bergner Orders')
                bs_bergner.to_excel(writer, index=False, sheet_name='Bergner File')
            st.download_button("📥 Download Both Reports", bs_combined_buf.getvalue(), "bergner_sec_combined.xlsx")

            # For Combined Summary
            bs_combined_df = pd.DataFrame({"Brand": ["Bergner (Secondary)"], "Bergner Sec Extra P&L": [bs_total_extra_pl]})
            combined_results.append(bs_combined_df)

        except Exception as e:
            st.error(f"❌ Error processing Bergner Secondary: {str(e)}")
    else:
        st.warning("Please upload Bergner Sec Orders TXT, PM file, and Bergner Sec Support Excel.")

# ==========================================
# TAB 12: TRAMONTINA SECONDARY
# ==========================================
with tabs[11]:
    st.header("📦 Tramontina Secondary Support")
    if tramontina_sec_orders_file and pm_file and tramontina_sec_file:
        try:
            with st.spinner("Processing Tramontina Secondary data..."):
                # Load files
                ts_tra = pd.read_excel(tramontina_sec_file)
                ts_df = pd.read_csv(tramontina_sec_orders_file, sep="\t", low_memory=False)
                ts_pm = pd.read_excel(pm_file)

                # Clean orders
                if 'product-name' in ts_df.columns:
                    ts_df = ts_df[ts_df['product-name'] != '-']
                ts_df = ts_df[ts_df['item-price'].notna() & (ts_df['item-price'].astype(str).str.strip() != '')]

                # Merge Brand from PM
                ts_df['asin'] = ts_df['asin'].astype(str)
                ts_pm['ASIN'] = ts_pm['ASIN'].astype(str)
                ts_df = ts_df.merge(ts_pm[['ASIN', 'Brand']], left_on='asin', right_on='ASIN', how='left')
                ts_df.drop(columns=['ASIN'], inplace=True)

                # Filter Tramontina orders
                ts_df_tra = ts_df[ts_df['Brand'] == 'Tramontina'].copy()

                # Build pivot of quantity by ASIN & item-status
                ts_pivot = pd.pivot_table(ts_df, index='asin', columns='item-status', values='quantity',
                                          aggfunc='sum', fill_value=0)
                exclude_status = ['Cancelled']
                ts_pivot['Net Quantity'] = ts_pivot.loc[:, ~ts_pivot.columns.isin(exclude_status)].sum(axis=1)
                ts_pivot.columns.name = None
                ts_pivot.reset_index(inplace=True)
                ts_pivot.columns = ts_pivot.columns.str.strip()

                # Merge into Tramontina file
                ts_tra['ASIN'] = ts_tra['ASIN'].astype(str)
                ts_pivot['asin'] = ts_pivot['asin'].astype(str)
                ts_tra = ts_tra.merge(ts_pivot[['asin', 'Net Quantity']], left_on='ASIN', right_on='asin', how='left')
                ts_tra.rename(columns={'Net Quantity': 'Month Order Count'}, inplace=True)
                ts_tra.drop(columns=['asin'], inplace=True)

                # Calculate Extra P&L
                ts_tra['Month Order Count'] = pd.to_numeric(ts_tra['Month Order Count'], errors='coerce').fillna(0)
                ts_tra['P/l'] = pd.to_numeric(ts_tra['P/l'], errors='coerce').fillna(0)
                ts_tra['Extra P&L'] = (ts_tra['Month Order Count'] * ts_tra['P/l']).round(2)

                # Grand Total row
                ts_total_extra_pl = ts_tra['Extra P&L'].sum()
                ts_total_row = pd.DataFrame({col: [''] for col in ts_tra.columns})
                ts_total_row.iloc[0, ts_tra.columns.get_loc('ASIN')] = 'Grand Total'
                ts_total_row['Extra P&L'] = ts_total_extra_pl
                ts_tra = pd.concat([ts_tra, ts_total_row], ignore_index=True)

            st.success(f"✅ Tramontina Secondary processed! Grand Total Extra P&L: ₹{ts_total_extra_pl:,.2f}")

            # Sub-tabs
            ts_tab1, ts_tab2 = st.tabs(["📋 Tramontina Orders", "📊 Tramontina File with P&L"])

            with ts_tab1:
                st.subheader("Tramontina Filtered Orders")
                st.caption(f"{len(ts_df_tra):,} rows")
                st.dataframe(ts_df_tra, use_container_width=True, height=400)
                st.download_button("📥 Download Tramontina Orders", convert_to_excel(ts_df_tra, 'Tramontina Orders'), "tramontina_sec_orders.xlsx")

            with ts_tab2:
                st.subheader("Tramontina File with Month Order Count & Extra P&L")
                st.caption(f"Grand Total Extra P&L: ₹{ts_total_extra_pl:,.2f}")
                st.dataframe(ts_tra, use_container_width=True, height=400)
                st.download_button("📥 Download Tramontina Support File", convert_to_excel(ts_tra, 'Tramontina File'), "tramontina_sec_file.xlsx")

            # Combined download
            ts_combined_buf = io.BytesIO()
            with pd.ExcelWriter(ts_combined_buf, engine='xlsxwriter') as writer:
                ts_df_tra.to_excel(writer, index=False, sheet_name='Tramontina Orders')
                ts_tra.to_excel(writer, index=False, sheet_name='Tramontina File')
            st.download_button("📥 Download Both Reports", ts_combined_buf.getvalue(), "tramontina_sec_combined.xlsx")

            # For Combined Summary
            ts_combined_df = pd.DataFrame({"Brand": ["Tramontina (Secondary)"], "Tramontina Sec Extra P&L": [ts_total_extra_pl]})
            combined_results.append(ts_combined_df)

        except Exception as e:
            st.error(f"❌ Error processing Tramontina Secondary: {str(e)}")
    else:
        st.warning("Please upload Tramontina Sec Orders TXT, PM file, and Tramontina Sec Support Excel.")

# ==========================================
# FINAL COMBINED REPORT POPULATION
# ==========================================
with tabs[0]:
    if combined_results:
        final_df = combined_results[0]
        for next_df in combined_results[1:]:
            final_df = pd.merge(final_df, next_df, on="Brand", how="outer")
        
        final_df["Brand"] = final_df["Brand"].fillna("Unknown/Unmapped")
        final_df = final_df.fillna(0)
        
        # Calculate Total
        numeric_cols = [c for c in final_df.columns if c != "Brand"]
        final_df["Grand Total"] = final_df[numeric_cols].sum(axis=1)
        
        # Sort by total
        final_df = final_df.sort_values("Grand Total", ascending=False)
        
        # Show Metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Brands", len(final_df[final_df['Brand'] != "Unknown/Unmapped"]))
        m2.metric("Total Support Cost", format_currency(final_df["Grand Total"].sum()))
        m3.metric("Max Expense Brand", final_df.iloc[0]["Brand"] if not final_df.empty else "N/A")
        m4.metric("Avg Support/Brand", format_currency(final_df["Grand Total"].mean()))
        
        st.markdown("---")
        
        # Add Summary Row
        summary_row = final_df[numeric_cols + ["Grand Total"]].sum().to_frame().T
        summary_row["Brand"] = "TOTAL"
        final_df = pd.concat([final_df, summary_row], ignore_index=True)
        
        # Display with dynamic coloring
        st.dataframe(
            final_df.style.format({c: format_currency for c in numeric_cols + ["Grand Total"]})
            .background_gradient(subset=["Grand Total"], cmap="YlOrRd"),
            use_container_width=True,
            height=500
        )
        
        # Download button
        st.download_button(
            "📥 Download Combined Report",
            convert_to_excel(final_df, "Combined Support"),
            "combined_amazon_support_report.xlsx"
        )
        
        # Visualization
        st.subheader("📊 Support Cost Distribution by Brand")
        chart_data = final_df[final_df["Brand"] != "TOTAL"].copy()
        st.bar_chart(chart_data.set_index("Brand")["Grand Total"])

    else:
        st.info("Upload files to see the combined brand-wise summary.")

# Footer
st.markdown("---")
st.caption(f"Amazon Support Unified App | Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}")

