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

def make_arrow_safe(df):
    """Convert all columns to string to avoid Arrow serialization issues in streamlit"""
    df = df.copy().reset_index(drop=True)
    for col in df.columns:
        df[col] = df[col].astype(str)
    return df


@st.cache_data
def convert_to_excel(df, sheet_name="Report"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

def format_currency(val):
    if pd.isna(val): return "₹ 0.00"
    return f"₹ {val:,.2f}"

@st.cache_data
def load_pm_cached(pm_bytes, is_csv=False):
    """Load and cache Purchase Master — handles standard Excel or ProductAttribute CSV"""
    if is_csv:
        df = pd.read_csv(io.BytesIO(pm_bytes), low_memory=False)
        # Standardize ProductAttribute CSV columns
        # E:N vlookup confirms index mapping (COST is col 10 in E:N range, E=SKU)
        rename_map = {
            "SKU": "SKU",
            "Brand": "Brand",
            "ModelNum": "ModelNum",
            "ProductName": "Product Name",
            "COST": "CP"
        }
        df.rename(columns=rename_map, inplace=True)
        # Convert numeric columns
        for c in ["CP", "MRP"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        
        # Use SKU as ASIN placeholder if missing for better compatibility
        if "ASIN" not in df.columns:
            if "SKU" in df.columns:
                df["ASIN"] = df["SKU"].astype(str)
            elif "ModelNum" in df.columns:
                df["ASIN"] = df["ModelNum"].astype(str)
            else:
                df["ASIN"] = df.index.astype(str)
    else:
        df = pd.read_excel(io.BytesIO(pm_bytes))
        # Alias common Excel variations to standard names
        ex_rename = {"Amazon SKU Name": "SKU", "Amazon Sku Name": "SKU"}
        df.rename(columns=ex_rename, inplace=True)
    
    # Ensure critical columns used by tabs are present
    if "ASIN" not in df.columns and "SKU" in df.columns:
        df["ASIN"] = df["SKU"].astype(str)
        
    df["ASIN"] = df["ASIN"].astype(str) if "ASIN" in df.columns else df.index.astype(str)
    
    # Standard column checks for all tools
    for col in ["Brand", "CP", "SKU"]:
        if col not in df.columns:
            df[col] = "Unknown" if col == "Brand" else (0.0 if col == "CP" else df["ASIN"])

    brand_map = df.drop_duplicates("ASIN").set_index("ASIN")["Brand"].to_dict()
    return df, brand_map

# ── Net Sale Dashboard Helper Functions ─────────────────────────────────────────

def fmt_net(val):
    try:    return f"₹{val:,.2f}"
    except: return val

def clean_sku_universal(s):
    """Clean SKUs by stripping backticks, commas, whitespace, and normalizing spaces."""
    if s is None or pd.isna(s): return ""
    return " ".join(str(s).upper().strip().replace("`","").replace(",","").split())

def clean_sku_net(s):
    return clean_sku_universal(s)

@st.cache_data(show_spinner=False)
def to_excel_bytes_net(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    return buf.getvalue()

@st.cache_data(show_spinner="Processing data…")
def run_net_pipeline(csv_bytes: bytes, pm_bytes: bytes, refund_bytes: bytes, is_pm_csv: bool = False):
    # 1. Load transaction CSV
    ns = pd.read_csv(io.BytesIO(csv_bytes), header=11, low_memory=False)
    ns.columns = ns.columns.str.lower()
    num_cols = ["quantity","product sales","total sales tax liable(gst before adjusting tcs)","total"]
    ns[num_cols] = ns[num_cols].replace(",","",regex=True)
    ns[num_cols] = ns[num_cols].apply(pd.to_numeric, errors="coerce")
    ns = ns[ns["type"] == "Order"]
    ns = ns[ns["product sales"] != 0]
    ns = ns.sort_values("order id").reset_index(drop=True)

    # 2. Use the unified loader logic
    pm, _ = load_pm_cached(pm_bytes, is_csv=is_pm_csv)
    pm.columns = pm.columns.str.lower()
    
    # Mapping for Net Sale pipeline - expected col names
    if "amazon sku name" not in pm.columns and "sku" in pm.columns:
        pm["amazon sku name"] = pm["sku"]
    
    if "amazon sku name" in pm.columns:
        pm["amazon sku name"] = pm["amazon sku name"].apply(clean_sku_universal)
    
    # Ensure CP is numeric
    if "cp" in pm.columns:
        pm["cp"] = pd.to_numeric(pm["cp"].astype(str).str.replace(",","",regex=False), errors="coerce")

    pm_lk = pm[["amazon sku name","asin","brand manager","brand","cp"]].drop_duplicates("amazon sku name")

    # 3. Enrich netsale
    ns["sku"] = ns["sku"].apply(clean_sku_universal)
    ns = ns.merge(pm_lk, left_on="sku", right_on="amazon sku name", how="left")
    ns["cp"] = pd.to_numeric(ns["cp"].astype(str).str.replace(",","",regex=False), errors="coerce").fillna(0)
    ns["cp as per qty"] = ns["cp"] * ns["quantity"].fillna(0)

    # 4. Groupby pivot
    pivot = (
        ns.groupby(["sku","order id","asin","brand"], dropna=False)[
            ["quantity","product sales","total sales tax liable(gst before adjusting tcs)","total","cp","cp as per qty"]
        ].sum().reset_index()
    )
    pivot["Sales Amount (Turn Over)"] = pivot["product sales"] + pivot["total sales tax liable(gst before adjusting tcs)"]
    pivot["Amazon Total Deducation"]   = pivot["Sales Amount (Turn Over)"] - pivot["total"]
    pivot["Amazon Total Deducation %"] = (pivot["Amazon Total Deducation"] / pivot["Sales Amount (Turn Over)"] * 100).round(2)
    pivot["profit"] = pivot["total"] - pivot["cp as per qty"]

    # 5. Refund IDs
    ref_ids_df = pd.read_csv(io.BytesIO(refund_bytes), header=11, usecols=["type","order id"], low_memory=False)
    refund_ids = set(ref_ids_df.loc[ref_ids_df["type"]=="Refund","order id"].dropna())

    # 6. Full refund rows
    ref_full = pd.read_csv(io.BytesIO(refund_bytes), header=11, low_memory=False)
    ref_full = ref_full[ref_full["type"]=="Refund"].reset_index(drop=True)

    # 7. Split pivot
    mask = pivot["order id"].isin(refund_ids)
    netsale_refund_nan = pivot[~mask].copy()
    refunded = pivot[mask].copy()

    # 8. Brand pivot
    bp = netsale_refund_nan.groupby("brand", dropna=False)[
        ["quantity","Sales Amount (Turn Over)","total","cp as per qty","profit"]
    ].sum()
    bp.loc["Grand Total"] = bp.sum()
    bp = bp.reset_index()
    bp["quantity"] = bp["quantity"].astype(int)
    bp = bp[["brand","quantity","Sales Amount (Turn Over)","total","cp as per qty","profit"]]

    return ns, ref_full, netsale_refund_nan, refunded, bp

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


def process_dyson_data(zip_files, pm_df_input, promo_file, past_months):
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
        
        # Use pm_df_input instead of reading from file
        PM = pm_df_input.copy()
        
        # Read promo file
        Promo = pd.read_excel(promo_file)
        
        if "SKU" in PM.columns:
            PM["SKU"] = PM["SKU"].astype(str).str.strip()
        
        PM["ASIN"] = PM["ASIN"].astype(str).str.strip()
        Promo["ASIN"] = Promo["ASIN"].astype(str).str.strip()

        # ---------- STEP 1: BRAND MAP via SKU ----------
        if "SKU" in PM.columns:
            brand_map_sku = PM.groupby("SKU", as_index=True)["Brand"].first()
        else:
            brand_map_sku = PM.groupby(PM.columns[0], as_index=True)["Brand"].first() # Fallback
            
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
        if "Order Id" in cols and "Order Status" in cols:
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
            (pivot["SSP"].fillna(0) - pivot["Cons Promo"].fillna(0))
            * (1 - pivot["Margin %"].fillna(0) / 100)
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
        import traceback
        st.code(traceback.format_exc())
        return None, None


def convert_dyson_df_to_csv(df):
    """Convert dataframe to CSV for Dyson downloads"""
    return df.to_csv(index=False).encode('utf-8')

# ==========================================
# SIDEBAR - GROUPED FILE UPLOADS
# ==========================================
st.sidebar.title("📤 Upload Center")

with st.sidebar.expander("📁 Core Configuration", expanded=True):
    st.caption("Common Support Files")
    pm_file = st.file_uploader("Purchase Master (PM) Excel", type=["xlsx", "xls"], key="pm_up",
                                 help="Standard Purchase Master Excel with ASIN, SKU, Brand, CP")
    asin_months_file = st.file_uploader("ASIN Months Tracker (Excel)", type=["xlsx"], key="asin_months_up")
    portfolio_file = st.file_uploader("Portfolio Report (Ads)", type=["xlsx", "xls"], key="portfolio_global",
                                      help="Excel mapping campaigns/portfolios → brands")

with st.sidebar.expander("🏷️ Coupon"):
    st.caption("Tab-separated TXT order report • Requires PM")
    coupon_file = st.file_uploader("Coupon Orders (TXT)", type=["txt"], key="coupon_up",
                                   help="Columns: asin, product-name, item-status, promotion-ids, item-promotion-discount")

with st.sidebar.expander("🔄 Exchange"):
    st.caption("Excel with brand & seller funding columns")
    exchange_file = st.file_uploader("Exchange Data (Excel)", type=["xlsx", "xls"], key="exchange_up",
                                     help="Columns: brand, order_day, seller funding, liquidator funding")

with st.sidebar.expander("🎁 Freebies"):
    st.caption("Tab-separated TXT order report • Requires PM")
    freebies_file = st.file_uploader("Freebies Orders (TXT)", type=["txt"], key="freebies_up",
                                     help="Columns: asin, product-name, item-status, promotion-ids (BOGO)")

with st.sidebar.expander("💳 NCEMI"):
    st.caption("Payment CSV (header at row 12) • Requires PM")
    ncemi_payment_file = st.file_uploader("Payment CSV", type=["csv"], key="ncemi_pay_up",
                                          help="Columns: type, Sku, product sales, total")
    ncemi_support_files = st.file_uploader("B2B/B2C Files", type=["csv", "zip"], accept_multiple_files=True, key="ncemi_sup_up",
                                           help="CSV or ZIP with CSV for SKU mapping")

with st.sidebar.expander("📢 Advertisement"):
    st.caption("PDF invoices from Amazon Ads")
    adv_files = st.file_uploader("Invoice PDFs", type=["pdf"], accept_multiple_files=True, key="adv_up",
                                  help="Data extracted automatically from PDF invoices")

with st.sidebar.expander("🔄 Replacement Logistic"):
    st.caption("Unified Transaction CSV (header at row 13) • Requires PM")
    rev_log_file = st.file_uploader("Transaction CSV", type=["csv"], key="rev_log_up",
                                    help="Columns: type, Sku, product sales, quantity, total")


with st.sidebar.expander("🧮 Dyson"):
    st.caption("B2B/B2C ZIPs + Promo/Invoice Excels • Requires PM")
    dyson_b2b_zips = st.file_uploader("B2B Report (ZIP)", type=["zip"], accept_multiple_files=True, key="dyson_b2b_up",
                                      help="ZIP with CSV: Sku, Asin, Quantity, Transaction Type, Order Id, Invoice Date")
    dyson_b2c_zips = st.file_uploader("B2C Report (ZIP)", type=["zip"], accept_multiple_files=True, key="dyson_b2c_up")
    dyson_promo_file = st.file_uploader("Promo (Excel)", type=["xlsx", "xls"], key="dyson_promo_up",
                                        help="Columns: ASIN, SKU Code, SSP, Cons Promo, Margin")
    dyson_invoice_file = st.file_uploader("Invoice (Excel)", type=["xlsx", "xls"], key="dyson_invoice_up",
                                          help="Columns: Material_Cd, Qty, Total_Val")
    dyson_invoice_promo_file = st.file_uploader("Invoice Promo CN (Excel)", type=["xlsx", "xls"], key="dyson_inv_promo_up",
                                                help="Col D = lookup key, Col L = Consumer Promo value")

with st.sidebar.expander("🏭 Bergner Secondary"):
    st.caption("Bergner Support Excel + Orders TXT • Requires PM")
    bergner_secondary_support_file = st.file_uploader("Bergner Support (Excel)", type=["xlsx", "xls"], key="berg_sec_sup_up",
                                                   help="Excel (header row 2): ASIN, P/L, order qty, P/L on orders qty")
    bergner_secondary_orders_file = st.file_uploader("Orders (TXT)", type=["txt", "tsv", "csv"], key="berg_sec_orders_up",
                                                  help="Tab-separated: asin, quantity, item-price")

with st.sidebar.expander("📦 Tramontina Secondary"):
    st.caption("Tramontina Support Excel + Orders TXT • Requires PM")
    tramontina_secondary_support_file = st.file_uploader("Tramontina Support (Excel)", type=["xlsx", "xls"], key="tram_sec_sup_up",
                                                     help="Excel: Amazon ASIN, L/P, Event CSP, SKU Code")
    tramontina_secondary_orders_file = st.file_uploader("Orders (TXT)", type=["txt", "tsv", "csv"], key="tram_sec_orders_up",
                                                    help="Tab-separated: asin, quantity, item-price")



with st.sidebar.expander("🍳 Wonderchef Secondary"):
    st.caption("GIF Support Excel + Orders TXT • Requires PM")
    wonderchef_support_file = st.file_uploader("GIF Support (Excel)", type=["xlsx", "xls"], key="wcf_sup_up",
                                               help="Excel (header row 2): Amazon ASIN, L/P, Event price, Sold Units")
    wonderchef_orders_file = st.file_uploader("Orders (TXT)", type=["txt", "tsv", "csv"], key="wcf_orders_up",
                                              help="Tab-separated: asin, quantity, item-price")

with st.sidebar.expander("🍴 Hafele Secondary"):
    st.caption("Hafele Support Excel + Orders TXT • Requires PM")
    hafele_support_file = st.file_uploader("Hafele Support (Excel)", type=["xlsx", "xls"], key="hafele_sup_up",
                                            help="Excel: Amazon ASIN, L/P, Event CSP, SKU Code")
    hafele_orders_file = st.file_uploader("Orders (TXT)", type=["txt", "tsv", "csv"], key="hafele_orders_up",
                                           help="Tab-separated: asin, quantity, item-price")

with st.sidebar.expander("📺 Panasonic Secondary"):
    st.caption("Panasonic Support Excel + Orders TXT • Requires PM")
    panasonic_support_file = st.file_uploader("Panasonic Support (Excel)", type=["xlsx", "xls"], key="pana_sup_up",
                                               help="Excel (header row 2): Amazon ASIN, L/P, Current CSP, SKU Code")
    panasonic_orders_file = st.file_uploader("Orders (TXT)", type=["txt", "tsv", "csv"], key="pana_orders_up",
                                              help="Tab-separated: asin, quantity, item-price")

with st.sidebar.expander("📦 Inalsa Secondary"):
    st.caption("B2B/B2C ZIPs + Unified CSV + Storage CSV • Requires PM")
    inalsa_b2b_zips = st.file_uploader("B2B Report ZIP(s)", type=["zip"], key="inalsa_b2b_up", accept_multiple_files=True,
                                       help="Amazon B2B tax reports in ZIP format")
    inalsa_b2c_zips = st.file_uploader("B2C Report ZIP(s)", type=["zip"], key="inalsa_b2c_up", accept_multiple_files=True,
                                       help="Amazon B2C tax reports in ZIP format")
    inalsa_unified_csv = st.file_uploader("Unified Transaction CSV", type=["csv"], key="inalsa_unified_up",
                                          help="Unified transaction report (header row 11)")
    inalsa_storage_csv = st.file_uploader("Storage Fee CSV", type=["csv"], key="inalsa_storage_up",
                                          help="Storage fee report (e.g., 399153020430.csv)")

with st.sidebar.expander("🔪 Victorinox Secondary"):
    st.caption("Support Excel (multi-sheet) + Orders TXT • Requires PM")
    victorinox_support_file = st.file_uploader("Support Excel", type=["xlsx", "xls"], key="vic_sup_up",
                                               help="e.g. CN Support Working for Nov Dec25. Multi-sheet supported.")
    victorinox_orders_file = st.file_uploader("Orders TXT/TSV", type=["txt", "tsv", "csv"], key="vic_orders_up",
                                              help="Tab-separated: asin, quantity, item-price")

with st.sidebar.expander("🛒 Inventory & Reimbursements"):
    st.caption("FBA/Seller Inventory, Reimbursements, Storage & Loss/Damage • Requires PM")
    current_inv_file = st.file_uploader("Inventory CSV", type=["csv"], key="curr_inv_up",
                                         help="Amazon inventory CSV export (e.g. 450281020518.csv)")
    reimb_fba_file = st.file_uploader("Reimbursement (FBA) CSV", type=["csv"], key="reimb_fba_up",
                                      help="Amazon FBA reimbursement export (e.g. 'Or Reimbursement.csv')")
    reimb_seller_file = st.file_uploader("Reimbursement (Seller) CSV", type=["csv"], key="reimb_seller_up",
                                         help="Monthly Unified Transaction export (skip 11 rows)")
    amazon_storage_file = st.file_uploader("Storage Fees CSV", type=["csv"], key="storage_up",
                                            help="Amazon FBA storage fees export")
    loss_damage_fba_file = st.file_uploader("Loss/Damage (FBA) CSV", type=["csv"], key="loss_fba_up",
                                            help="Amazon FBA returns export (e.g. 450296020518.csv)")
    loss_damage_seller_file = st.file_uploader("Loss/Damage (Seller) Excel", type=["xlsx", "xls"], key="loss_seller_up",
                                               help="Seller Flex Damage Excel (Amazon sheet)")

with st.sidebar.expander("📦 Reverse Logistics"):
    st.caption("FBA & Seller Reverse Logistics Analysis • Requires PM")
    rev_fba_txn_file = st.file_uploader("FBA Transaction CSV", type=["csv"], key="rev_fba_txn_up",
                                        help="Amazon Unified Transaction Report (skips 11 rows)")
    rev_fba_ret_file = st.file_uploader("FBA Returns CSV", type=["csv"], key="rev_fba_ret_up",
                                        help="Amazon FBA Returns Report")
    rev_sel_txn_file = st.file_uploader("Seller Transaction CSV", type=["csv"], key="rev_sel_txn_up",
                                        help="Amazon Unified Transaction Report (skips 11 rows)")
    rev_sel_ret_file = st.file_uploader("Seller Returns Reconciliation CSV", type=["csv"], key="rev_sel_ret_up",
                                        help="QWTT Returns Reconciliation Report")
    rev_sel_ws_file = st.file_uploader("Working Sheet 2 Excel", type=["xlsx", "xls"], key="rev_sel_ws_up",
                                       help="Excel with 'Bluk Return Upload Snaphire' sheet")

with st.sidebar.expander("📊 Sales Analysis"):
    st.caption("Net Sale & PnL Analysis • Requires PM")
    net_sale_txn_file = st.file_uploader("Net Sale Transaction CSV (Orders)", type=["csv"], key="net_sale_up",
                                         help="Amazon Unified Transaction Report (skips 11 rows)")
    net_sale_refund_file = st.file_uploader("Net Sale Refund CSV", type=["csv"], key="net_sale_refund_up",
                                            help="Separate Transaction Report for Refunds (skips 11 rows)")
    interest_damage_file = st.file_uploader("Interest & Damage Resolve File", type=["xlsx", "xls"], key="int_dam_up",
                                            help="Optional: Interest & Damage Resolve.xlsx for vlookups")

with st.sidebar.expander("🏥 Current Damage"):
    st.caption("Inventory Report + Product Attributes CSV")
    inv_rep_file = st.file_uploader("Inventory Report CSV", type=["csv"], key="inv_rep_up")
    prod_attr_file = st.file_uploader("Product Attributes CSV", type=["csv"], key="prod_attr_up")





# ==========================================
# DATA LOADING & INITIAL MAPPING
# ==========================================
brand_mapping = {}
pm_df = None
if pm_file:
    pm_bytes = pm_file.read()
    pm_file.seek(0)
    pm_df, brand_mapping = load_pm_cached(pm_bytes, is_csv=False)

# ==========================================
# MAIN TABS INITIALIZATION
# ==========================================
st.title("🚀 Amazon Support Unified Dashboard")

any_files = (pm_file or coupon_file or exchange_file or freebies_file or ncemi_payment_file or adv_files or rev_log_file or dyson_b2b_zips or dyson_b2c_zips or dyson_invoice_file or bergner_secondary_orders_file or tramontina_secondary_orders_file or wonderchef_orders_file or hafele_orders_file or panasonic_orders_file or inalsa_b2b_zips or inalsa_b2c_zips or inalsa_unified_csv or inalsa_storage_csv or victorinox_support_file or victorinox_orders_file or current_inv_file or reimb_fba_file or reimb_seller_file or amazon_storage_file or loss_damage_fba_file or loss_damage_seller_file or rev_fba_txn_file or rev_fba_ret_file or rev_sel_txn_file or rev_sel_ret_file or rev_sel_ws_file or net_sale_txn_file or net_sale_refund_file or interest_damage_file or asin_months_file or dyson_promo_file or inv_rep_file or prod_attr_file)




if not any_files:
    st.info("👋 Welcome! Open a section in the **sidebar** ← and upload your files to get started.")
    st.markdown("### 📊 Available Tools")
    tool_info = [
        ("🏷️ Coupon", "PLM promotion discount analysis", "PM + Coupon TXT"),
        ("🔄 Exchange", "Seller & liquidator funding", "Exchange Excel"),
        ("🎁 Freebies", "BOGO promotion analysis", "PM + Freebies TXT"),
        ("💳 NCEMI", "No-cost EMI funding breakup", "PM + Payment CSV"),
        ("📢 Ads", "Ad invoice campaign analysis", "Ad PDFs"),
        ("🔄 Repl. Logistic", "Zero-sale replacement orders", "PM + Transaction CSV"),
        ("🧮 Dyson", "Support per net sale", "PM + ZIPs + Promo"),
        ("🏭 Bergner Sec.", "Secondary P/L analysis", "PM + Support + Orders"),
        ("📦 Tramontina Sec.", "Secondary P/L analysis", "PM + Support + Orders"),
        ("📦 Inventory", "Current warehouse stock value", "PM + Inventory CSV"),
        ("💰 Reimb. FBA", "Customer return reimbursements", "PM + Reimb CSV"),
        ("🛒 Reimb. Seller", "SAFE-T & Seller reimbursements", "PM + Unified CSV"),
        ("🏭 Storage", "Monthly FBA storage fees", "PM + Storage CSV"),
        ("📉 Loss/Dmg FBA", "Damaged item cost at FBA", "PM + Returns CSV"),
        ("🏬 Loss/Dmg Sel", "Seller Flex damage cost", "Seller Damage Excel"),
        ("📦 Rev Log FBA", "FBA Transaction vs Returns", "PM + Txn + Returns"),
        ("🏬 Rev Log Sel", "Seller Flex Transaction vs QWTT", "PM + Txn + QWTT + WS"),
        ("📊 Net Sale", "Deep sales analysis (Excl. Refunds)", "PM + Transaction CSV"),
        ("🏥 Current Damage", "Brand-wise inventory cost summary", "Inventory CSV + Prod Attr CSV"),
    ]

    cols = st.columns(3)
    for i, (name, desc, files) in enumerate(tool_info):
        with cols[i % 3]:
            st.markdown(f"""
            <div style="background:white;padding:14px 16px;border-radius:10px;border:1px solid #e5e7eb;margin-bottom:10px;">
                <b style="font-size:15px;">{name}</b><br>
                <span style="color:#6b7280;font-size:13px;">{desc}</span><br>
                <span style="color:#9ca3af;font-size:12px;">📂 {files}</span>
            </div>
            """, unsafe_allow_html=True)
    st.stop()

tabs = st.tabs(["🏠 Combined Summary", "🏷️ Coupon", "🔄 Exchange", "🎁 Freebies", "💳 NCEMI", "📢 Advertisement", "🔄 Replacement Logistic", "🧮 Dyson", "🏭 Bergner Secondary", "📦 Tramontina Secondary", "🍳 Wonderchef Secondary", "🍴 Hafele Secondary", "📺 Panasonic Secondary", "📦 Inalsa Secondary", "🔪 Victorinox Secondary", "📦 Current Inventory", "💰 Reimbursement FBA", "🛒 Reimbursement Seller", "🏭 Amazon Storage", "📉 Loss/Damage FBA", "🏬 Loss/Damage Seller", "📦 Reverse Logistic FBA", "🏬 Reverse Logistic Seller", "📊 Net Sale Analyzer", "🏥 Current Damage"])




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
            combined_results.append(c_pivot.rename(columns={"Coupon Discount": "Coupon Support"}))
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
        combined_results.append(e_pivot_final.rename(columns={"Exchange Funding": "Exchange Support"}))
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
            
            combined_results.append(f_pivot[["Brand", "Freebies Discount"]].rename(columns={"Freebies Discount": "Freebies Support"}))
    else:
        st.warning("Please upload both Order TXT and PM file for Freebies analysis.")

# ==========================================
# TAB 5: NCEMI
# ==========================================
with tabs[4]:
    st.header("💳 NCEMI Promotion Analysis")
    if ncemi_payment_file and pm_file:
        try:
            # Payment CSV Loading
            p_df = pd.read_csv(ncemi_payment_file, skiprows=11)
            
            # Cleaning numeric columns like the new support_ncemi.py
            pay_num_cols = ["other transaction fees", "other", "total", "product sales"]
            for col in pay_num_cols:
                if col in p_df.columns:
                    p_df[col] = pd.to_numeric(p_df[col].astype(str).str.replace(",",""), errors="coerce")
            
            n_df = p_df[p_df["type"] == "Order"].copy()
            n_df = n_df[n_df["product sales"] == 0]
            
            n_df["Sku"] = n_df["Sku"].apply(normalize_sku)
            n_df["order id"] = n_df["order id"].apply(normalize_sku)
            
            # Filter rows with missing SKU only (as per latest script logic if applicable)
            # Actually, the user script does: df = df[df["Sku"].isna() | (df["Sku"] == "")]
            # But the unified app merges. Let's stick closer to the user script's filtering if that was the intent.
            # Looking at support_ncemi.py line 112: df = df[df["Sku"].isna() | (df["Sku"] == "")]
            # I will apply this filter to strictly follow the "updated" logic.
            n_df = n_df[n_df["Sku"].isna() | (n_df["Sku"] == "")]
            
            if ncemi_support_files:
                for f in ncemi_support_files:
                    try:
                        df_rep = None
                        if f.name.endswith(".zip"):
                            with zipfile.ZipFile(f) as z:
                                csv_name = [name for name in z.namelist() if name.endswith(".csv")][0]
                                with z.open(csv_name) as zf:
                                    df_rep = pd.read_csv(zf)
                        else:
                            df_rep = pd.read_csv(f)
                            
                        if df_rep is not None:
                            # Using helper logic for filling SKU
                            # order_col_idx=4, sku_col_idx=13 as per latest script
                            order_col = df_rep.columns[4]
                            sku_col = df_rep.columns[13]
                            
                            df_rep[order_col] = df_rep[order_col].apply(normalize_sku)
                            df_rep[sku_col] = df_rep[sku_col].apply(normalize_sku)
                            
                            lookup = (
                                df_rep.dropna(subset=[order_col])
                                .drop_duplicates(order_col)
                                .set_index(order_col)[sku_col]
                                .to_dict()
                            )
                            
                            mask = n_df["Sku"].isna() | (n_df["Sku"] == "")
                            n_df.loc[mask, "Sku"] = n_df.loc[mask, "order id"].map(lookup)
                    except Exception as e:
                        st.warning(f"Error processing {f.name}: {e}")

            # Re-apply PM Mapping
            pm_full = pm_df.copy()
            # Mapping columns: 2-SKU, 4-Manager, 6-Brand, 3-Vendor SKU, 7-Product Name
            sku_key = pm_full.columns[2]
            pm_full[sku_key] = pm_full[sku_key].apply(normalize_sku)
            n_df["Sku"] = n_df["Sku"].apply(normalize_sku)
            
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
                
                grand_total_n = n_pivot_brand["total"].sum()
                summary_n = pd.DataFrame({"Brand": ["Grand Total"], "total": [grand_total_n]})
                n_pivot_brand_display = pd.concat([n_pivot_brand, summary_n], ignore_index=True)
                
                st.dataframe(make_arrow_safe(n_pivot_brand_display), use_container_width=True)
                st.download_button("📥 Download Brand Analysis (CSV)", n_pivot_brand_display.to_csv(index=False).encode(), "ncemi_brand_analysis.csv")
                
                # For combined summary - use absolute values if needed or raw?
                # The user script doesn't abs() in create_pivot_table, but does in display?
                # Looking at support_ncemi.py line 161: grand_total = pivot["total"].sum()
                # Line 249: Download CSV. 
                # I'll keep it as is.
                combined_results.append(n_pivot_brand.rename(columns={"total": "NCEMI Support"}))

            with n_tab2:
                st.subheader("Brand Manager-wise Summary")
                n_pivot_mgr = n_df.groupby("Brand Manager")["total"].sum().reset_index()
                
                summary_mgr = pd.DataFrame({"Brand Manager": ["Grand Total"], "total": [n_pivot_mgr["total"].sum()]})
                n_pivot_mgr_display = pd.concat([n_pivot_mgr, summary_mgr], ignore_index=True)
                
                st.dataframe(make_arrow_safe(n_pivot_mgr_display), use_container_width=True)
                st.download_button("📥 Download Manager Analysis (CSV)", n_pivot_mgr_display.to_csv(index=False).encode(), "ncemi_manager_analysis.csv")

            with n_tab3:
                st.subheader("Service Fees Breakdown")
                sf_df = p_df[p_df["type"] == "Service Fee"].copy()
                
                summary_sf = sf_df[["other transaction fees", "other", "total"]].sum()
                c1, c2, c3 = st.columns(3)
                c1.metric("Transaction Fees", format_currency(summary_sf["other transaction fees"]))
                c2.metric("Other Fees", format_currency(summary_sf["other"]))
                c3.metric("Total Service Fees", format_currency(summary_sf["total"]))
                
                st.dataframe(make_arrow_safe(sf_df), use_container_width=True)
                st.download_button("📥 Download Service Fees (CSV)", sf_df.to_csv(index=False).encode(), "ncemi_service_fees.csv")

            with n_tab4:
                st.subheader("Raw Data with Mapping")
                st.dataframe(make_arrow_safe(n_df), use_container_width=True)
                st.download_button("📥 Download Raw Data (CSV)", n_df.to_csv(index=False).encode(), "ncemi_raw_data.csv")
                
        except Exception as e:
            st.error(f"❌ Error processing NCEMI: {e}")
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
                    combined_results.append(a_pivot[["Brand", "Total Amount (incl. GST)"]].rename(columns={"Total Amount (incl. GST)": "Advertising support"}))
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
            pm_full_rl = pm_df.copy()
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
            
            combined_results.append(rl_pivot[["Brand", "total"]].rename(columns={"total": "Replacement charges"}))

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
# TAB 8: DYSON
# ==========================================
with tabs[7]:
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
                            pivot, processed = process_dyson_data(all_zips, pm_df, dyson_promo_file, past_months)
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
                            pivot, processed = process_dyson_data(zip_files_for_tab, pm_df, dyson_promo_file, past_months)
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
                    # Excel logic: VLOOKUP(Material_Code, D:L, 9, 0)
                    lookup_column_idx = 3   # Column D (0-indexed)
                    return_column_idx = 11  # Column L (0-indexed)
                    
                    if len(promo_df.columns) > return_column_idx:
                        lookup_column = promo_df.columns[lookup_column_idx]
                        return_column = promo_df.columns[return_column_idx]
                        promo_map = promo_df.set_index(lookup_column)[return_column]
                        df_invoice["Consumer Promo"] = df_invoice["Material_Cd"].map(promo_map)
                    else:
                        st.error(f"Promo CN file must have at least {return_column_idx + 1} columns.")
                        st.stop()

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
                    
                    # Ensure all desired columns exist
                    for col in desired_order:
                        if col not in df_invoice.columns:
                            df_invoice[col] = 0

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
                    import traceback
                    st.code(traceback.format_exc())
            else:
                st.warning("⚠️ Please upload both Invoice file and Promo CN file in the sidebar.")

# ==========================================
# TAB 9: BERGNER SECONDARY
# ==========================================
with tabs[8]:
    st.header("🏭 Bergner Secondary Support")
    if bergner_secondary_support_file and bergner_secondary_orders_file and pm_file:
        try:
            with st.spinner("Processing Bergner Secondary data..."):
                b_sec_support = pd.read_excel(bergner_secondary_support_file, header=1)
                b_sec_orders = pd.read_csv(bergner_secondary_orders_file, sep="\t", low_memory=False, dtype=str)
                b_sec_pm = pm_df.copy()

                b_sec_orders['asin'] = b_sec_orders['asin'].astype(str).str.strip()
                b_sec_pm['ASIN'] = b_sec_pm['ASIN'].astype(str).str.strip()
                b_sec_orders = b_sec_orders.merge(b_sec_pm[['ASIN', 'Brand']], left_on='asin', right_on='ASIN', how='left')
                b_sec_orders['item-price'] = pd.to_numeric(b_sec_orders['item-price'], errors='coerce')
                b_sec_orders = b_sec_orders[b_sec_orders['item-price'].notna()]
                b_sec_orders['quantity'] = pd.to_numeric(b_sec_orders['quantity'], errors='coerce').fillna(0)
                b_sec_brand_orders = b_sec_orders[b_sec_orders['Brand'] == 'Bergner'].copy()

                b_sec_pivot = (
                    pd.pivot_table(b_sec_brand_orders, index='asin', values='quantity', aggfunc='sum')
                    .reset_index()
                )
                b_sec_pivot.columns = ['ASIN', 'Sold Units']

                b_sec_support['ASIN'] = b_sec_support['ASIN'].astype(str).str.strip()
                b_sec_pivot['ASIN'] = b_sec_pivot['ASIN'].astype(str).str.strip()
                asin_to_units = dict(zip(b_sec_pivot['ASIN'], b_sec_pivot['Sold Units']))
                b_sec_support['order qty'] = b_sec_support['ASIN'].map(asin_to_units).fillna(0)

                b_sec_support['P/L'] = pd.to_numeric(b_sec_support['P/L'], errors='coerce').fillna(0)
                b_sec_support['P/L on orders qty'] = b_sec_support['P/L'] * b_sec_support['order qty']

                b_sec_total_pl = b_sec_support['P/L on orders qty'].sum()
                total_row = {col: None for col in b_sec_support.columns}
                total_row['ASIN'] = 'Grand Total'
                total_row['P/L on orders qty'] = b_sec_total_pl
                b_sec_support = pd.concat([b_sec_support, pd.DataFrame([total_row])], ignore_index=True)

            st.success(f"✅ Bergner Secondary processed! Total P/L: ₹{b_sec_total_pl:,.2f}")
            st.dataframe(b_sec_support, use_container_width=True, height=400)
            st.download_button("📥 Download Bergner Sec Support", convert_to_excel(b_sec_support, 'Bergner Sec Support'), "bergner_sec_support.xlsx")

            b_sec_combined_df = pd.DataFrame({"Brand": ["Bergner (Secondary)"], "Bergner Sec Support": [b_sec_total_pl]})
            combined_results.append(b_sec_combined_df)

        except Exception as e:
            st.error(f"❌ Error processing Bergner Secondary: {str(e)}")
    else:
        st.warning("Please upload Bergner Secondary files and PM file.")

# ==========================================
# TAB 10: TRAMONTINA SECONDARY
# ==========================================
with tabs[9]:
    st.header("📦 Tramontina Secondary Support")
    if tramontina_secondary_support_file and tramontina_secondary_orders_file and pm_file:
        try:
            with st.spinner("Processing Tramontina Secondary data..."):
                t_sec_support = pd.read_excel(tramontina_secondary_support_file, header=0)
                t_sec_orders = pd.read_csv(tramontina_secondary_orders_file, sep="\t", low_memory=False, dtype=str)
                t_sec_pm = pm_df.copy()

                t_sec_orders['asin'] = t_sec_orders['asin'].astype(str).str.strip()
                t_sec_pm['ASIN'] = t_sec_pm['ASIN'].astype(str).str.strip()
                t_sec_orders = t_sec_orders.merge(t_sec_pm[['ASIN', 'Brand']], left_on='asin', right_on='ASIN', how='left')
                t_sec_orders['item-price'] = pd.to_numeric(t_sec_orders['item-price'], errors='coerce')
                t_sec_orders = t_sec_orders[t_sec_orders['item-price'].notna()]
                t_sec_orders['quantity'] = pd.to_numeric(t_sec_orders['quantity'], errors='coerce').fillna(0)
                t_sec_brand_orders = t_sec_orders[t_sec_orders['Brand'] == 'Tramontina'].copy()

                t_sec_pivot = (
                    pd.pivot_table(t_sec_brand_orders, index='asin', values='quantity', aggfunc='sum')
                    .reset_index()
                )
                t_sec_pivot.columns = ['ASIN', 'Sold Units']

                t_sec_support['Amazon ASIN'] = t_sec_support['Amazon ASIN'].astype(str).str.strip()
                t_sec_pivot['ASIN'] = t_sec_pivot['ASIN'].astype(str).str.strip()
                asin_to_units = dict(zip(t_sec_pivot['ASIN'], t_sec_pivot['Sold Units']))
                t_sec_support['Sold Units'] = t_sec_support['Amazon ASIN'].map(asin_to_units).fillna(0)

                t_sec_support['L/P'] = pd.to_numeric(t_sec_support['L/P'], errors='coerce').fillna(0)
                t_sec_support['Support'] = t_sec_support['L/P'] * t_sec_support['Sold Units']

                t_sec_total_support = t_sec_support['Support'].sum()
                total_row = {col: None for col in t_sec_support.columns}
                total_row['Amazon ASIN'] = 'Grand Total'
                total_row['Support'] = t_sec_total_support
                t_sec_support = pd.concat([t_sec_support, pd.DataFrame([total_row])], ignore_index=True)

            st.success(f"✅ Tramontina Secondary processed! Total Support: ₹{t_sec_total_support:,.2f}")
            st.dataframe(t_sec_support, use_container_width=True, height=400)
            st.download_button("📥 Download Tramontina Sec Support", convert_to_excel(t_sec_support, 'Tramontina Sec Support'), "tramontina_sec_support.xlsx")

            t_sec_combined_df = pd.DataFrame({"Brand": ["Tramontina (Secondary)"], "Tramontina Sec Support": [t_sec_total_support]})
            combined_results.append(t_sec_combined_df)

        except Exception as e:
            st.error(f"❌ Error processing Tramontina Secondary: {str(e)}")
    else:
        st.warning("Please upload Tramontina Secondary files and PM file.")

# ==========================================
# TAB 11: WONDERCHEF SECONDARY
# ==========================================
with tabs[10]:
    st.header("🍳 Wonderchef Secondary Support")
    if wonderchef_support_file and wonderchef_orders_file and pm_file:
        try:
            with st.spinner("Processing Wonderchef Secondary data..."):
                # Load GIF Support sheet (header at row 2)
                wcf_support = pd.read_excel(wonderchef_support_file, header=1)

                # Load orders (tab-separated TXT)
                wcf_orders = pd.read_csv(wonderchef_orders_file, sep="\t", low_memory=False, dtype=str)
                wcf_pm = pm_df.copy()

                # Clean ASINs
                wcf_orders['asin'] = wcf_orders['asin'].astype(str).str.strip()
                wcf_pm['ASIN'] = wcf_pm['ASIN'].astype(str).str.strip()

                # Merge Brand from PM
                wcf_orders = wcf_orders.merge(wcf_pm[['ASIN', 'Brand']], left_on='asin', right_on='ASIN', how='left')
                wcf_orders.drop(columns=['ASIN'], inplace=True, errors='ignore')

                # Convert numeric cols
                wcf_orders['item-price'] = pd.to_numeric(wcf_orders['item-price'], errors='coerce')
                wcf_orders = wcf_orders[wcf_orders['item-price'].notna()]
                wcf_orders['quantity'] = pd.to_numeric(wcf_orders['quantity'], errors='coerce').fillna(0)

                # Filter Wonderchef brand
                wcf_brand_orders = wcf_orders[wcf_orders['Brand'] == 'Wonderchef'].copy()

                # Pivot: sold units per ASIN
                wcf_pivot = (
                    pd.pivot_table(wcf_brand_orders, index='asin', values='quantity', aggfunc='sum')
                    .sort_values(by='quantity', ascending=False)
                    .reset_index()
                )
                wcf_pivot.columns = ['Amazon ASIN', 'Sold Units']

                # Map sold units into support sheet
                wcf_support['Amazon ASIN'] = wcf_support['Amazon ASIN'].astype(str).str.strip()
                wcf_pivot['Amazon ASIN'] = wcf_pivot['Amazon ASIN'].astype(str).str.strip()
                asin_to_units = dict(zip(wcf_pivot['Amazon ASIN'], wcf_pivot['Sold Units']))
                wcf_support['Sold Units'] = wcf_support['Amazon ASIN'].map(asin_to_units).fillna(0)

                # Compute Support and Plan Sales Value
                wcf_support['L/P'] = pd.to_numeric(wcf_support['L/P'], errors='coerce').fillna(0)
                wcf_support['Sold Units'] = pd.to_numeric(wcf_support['Sold Units'], errors='coerce').fillna(0)
                wcf_support['Support'] = wcf_support['L/P'] * wcf_support['Sold Units']
                wcf_support['Plan Sales Value'] = wcf_support['Sold Units'] * pd.to_numeric(wcf_support['Event price'], errors='coerce').fillna(0)

                # Grand Total row
                wcf_total_support = wcf_support['Support'].sum()
                wcf_total_psv = wcf_support['Plan Sales Value'].sum()
                wcf_total_units = wcf_support['Sold Units'].sum()
                wcf_support_pct = (wcf_total_support / wcf_total_psv * 100) if wcf_total_psv != 0 else 0

                total_row = {col: None for col in wcf_support.columns}
                total_row['Amazon ASIN'] = 'Grand Total'
                total_row['Sold Units'] = wcf_total_units
                total_row['Support'] = wcf_total_support
                total_row['Plan Sales Value'] = wcf_total_psv
                wcf_support = pd.concat([wcf_support, pd.DataFrame([total_row])], ignore_index=True)

                pct_row = {col: None for col in wcf_support.columns}
                pct_row['Amazon ASIN'] = 'Support %'
                pct_row['Support'] = wcf_support_pct
                wcf_support = pd.concat([wcf_support, pd.DataFrame([pct_row])], ignore_index=True)

            st.success(f"✅ Wonderchef Secondary processed! Total Support: ₹{wcf_total_support:,.2f} | Support %: {wcf_support_pct:.2f}%")

            # KPI Cards
            data_rows_wcf = wcf_support[~wcf_support['Amazon ASIN'].isin(['Grand Total', 'Support %'])]
            profitable = int((pd.to_numeric(data_rows_wcf['L/P'], errors='coerce') > 0).sum())
            loss_making = int((pd.to_numeric(data_rows_wcf['L/P'], errors='coerce') < 0).sum())

            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Total SKUs", len(data_rows_wcf))
            k2.metric("Profitable", profitable)
            k3.metric("Loss-Making", loss_making)
            k4.metric("Total Support", f"₹{wcf_total_support:,.0f}")
            k5.metric("Support %", f"{wcf_support_pct:.2f}%")

            # Sub-tabs
            wcf_tab1, wcf_tab2, wcf_tab3 = st.tabs(["📋 Final Support Table", "📦 Units Sold Pivot", "🔍 Wonderchef Orders"])

            with wcf_tab1:
                st.subheader("Wonderchef Support Sheet (Final)")
                show_cols = [c for c in ['Amazon ASIN', 'SKU Code', 'Desc', 'Sold Units', 'Corrected NLC',
                             'Event price', 'Current Commission', 'Total FBA Fee +GST', 'DB Margin',
                             'Payout', 'L/P', 'Percent', 'Support', 'Plan Sales Value'] if c in wcf_support.columns]
                st.dataframe(wcf_support[show_cols], use_container_width=True, height=500)
                st.download_button("📥 Download Support Table", convert_to_excel(wcf_support[show_cols], 'WCF Support'), "wonderchef_support.xlsx")

            with wcf_tab2:
                st.subheader("Units Sold per ASIN")
                st.dataframe(wcf_pivot, use_container_width=True, height=400)
                st.download_button("📥 Download Units Pivot", convert_to_excel(wcf_pivot, 'Units Pivot'), "wonderchef_pivot.xlsx")

            with wcf_tab3:
                st.subheader("Amazon Orders — Wonderchef Brand")
                st.caption(f"{len(wcf_brand_orders):,} rows")
                st.dataframe(wcf_brand_orders.head(500), use_container_width=True, height=400)
                if len(wcf_brand_orders) > 500:
                    st.info(f"Showing first 500 of {len(wcf_brand_orders):,} rows")
                st.download_button("📥 Download Orders", convert_to_excel(wcf_brand_orders, 'WCF Orders'), "wonderchef_orders.xlsx")

            # Combined Summary
            wcf_combined_df = pd.DataFrame({"Brand": ["Wonderchef (Secondary)"], "Wonderchef Sec Support": [wcf_total_support]})
            combined_results.append(wcf_combined_df)

        except Exception as e:
            st.error(f"❌ Error processing Wonderchef Secondary: {str(e)}")
    else:
        st.warning("Please upload Wonderchef GIF Support Excel, Orders TXT, and PM file.")

# ==========================================
# TAB 14: HAFELE SECONDARY
# ==========================================
with tabs[11]:
    st.header("🍴 Hafele Secondary Support")

    with st.expander("👁️ Preview Sample: Hafele Support Sheet (Hafele Jan Art Event Support Working 2026.xlsx)", expanded=False):
        _hafele_sample = pd.DataFrame([
            {"Amazon ASIN": "B0DH6DDR35", "SKU Code": "538.11.233", "Product Name": "Hafele Themis 60 Ceiling Recessed Cookerhood Chimney | 1100 m3/hr Suction | Filterfree Technology", "Sold Units": "", "Corrected NLC": 11520, "Event CSP": 14990, "Bau Commission": 0.08, "Event Commission": 0.08, "Amazon Referral Fee": 1199.2, "Total FBA Fee +GST": 724.8, "DB Margin": 749.5, "Payout": 12316.5, "L/P": 796.5, "Percent": 0.0531, "Support": 0, "Plan Sales Value": 0},
            {"Amazon ASIN": "B0DH689LKG", "SKU Code": "538.11.234", "Product Name": "Hafele Themis 90 Ceiling Recessed Cookerhood Chimney | 1100 m3/hr Suction | Filterfree Technology", "Sold Units": "", "Corrected NLC": 12480, "Event CSP": 15990, "Bau Commission": 0.08, "Event Commission": 0.08, "Amazon Referral Fee": 1279.2, "Total FBA Fee +GST": 811.2, "DB Margin": 799.5, "Payout": 13100.1, "L/P": 620.1, "Percent": 0.0388, "Support": 0, "Plan Sales Value": 0},
            {"Amazon ASIN": "B0DH6D6H2R", "SKU Code": "538.11.232", "Product Name": "Hafele Themis 60 Filterfree Technology Kitchen Chimney with High Suction", "Sold Units": "", "Corrected NLC": 13838, "Event CSP": 15990, "Bau Commission": 0.08, "Event Commission": 0.08, "Amazon Referral Fee": 1279.2, "Total FBA Fee +GST": 759.2, "DB Margin": 799.5, "Payout": 13152.1, "L/P": -685.9, "Percent": -0.0429, "Support": 0, "Plan Sales Value": 0},
        ])
        st.caption(f"📄 3 sample rows × {len(_hafele_sample.columns)} columns — read-only preview of expected Hafele Support format")
        st.dataframe(_hafele_sample, use_container_width=True, height=200)

    if hafele_support_file and hafele_orders_file and pm_file:
        try:
            with st.spinner("Processing Hafele Secondary data..."):
                # Load Hafele sheet
                haf_support = pd.read_excel(hafele_support_file, header=0)

                # Load orders (tab-separated TXT)
                haf_orders = pd.read_csv(hafele_orders_file, sep="\t", low_memory=False, dtype=str)
                haf_pm = pm_df.copy()

                # Clean ASINs
                haf_orders['asin'] = haf_orders['asin'].astype(str).str.strip()
                haf_pm['ASIN'] = haf_pm['ASIN'].astype(str).str.strip()

                # Merge Brand from PM
                haf_orders = haf_orders.merge(haf_pm[['ASIN', 'Brand']], left_on='asin', right_on='ASIN', how='left')
                haf_orders.drop(columns=['ASIN'], inplace=True, errors='ignore')

                # Convert numeric cols
                haf_orders['item-price'] = pd.to_numeric(haf_orders['item-price'], errors='coerce')
                haf_orders = haf_orders[haf_orders['item-price'].notna()]
                haf_orders['quantity'] = pd.to_numeric(haf_orders['quantity'], errors='coerce').fillna(0)

                # Filter Hafele brand
                haf_brand_orders = haf_orders[haf_orders['Brand'] == 'Hafele'].copy()

                # Pivot: sold units per ASIN
                haf_pivot = (
                    pd.pivot_table(haf_brand_orders, index='asin', values='quantity', aggfunc='sum')
                    .sort_values(by='quantity', ascending=False)
                    .reset_index()
                )
                haf_pivot.columns = ['Amazon ASIN', 'Sold Units']

                # Map sold units into support sheet
                haf_support['Amazon ASIN'] = haf_support['Amazon ASIN'].astype(str).str.strip()
                haf_pivot['Amazon ASIN'] = haf_pivot['Amazon ASIN'].astype(str).str.strip()
                asin_to_units = dict(zip(haf_pivot['Amazon ASIN'], haf_pivot['Sold Units']))
                haf_support['Sold Units'] = haf_support['Amazon ASIN'].map(asin_to_units).fillna(0)

                # Compute Support and Plan Sales Value
                haf_support['L/P'] = pd.to_numeric(haf_support['L/P'], errors='coerce').fillna(0)
                haf_support['Sold Units'] = pd.to_numeric(haf_support['Sold Units'], errors='coerce').fillna(0)
                haf_support['Support'] = haf_support['L/P'] * haf_support['Sold Units']
                haf_support['Plan Sales Value'] = haf_support['Sold Units'] * pd.to_numeric(haf_support['Event CSP'], errors='coerce').fillna(0)

                # Grand Total row
                haf_total_support = haf_support['Support'].sum()
                haf_total_psv = haf_support['Plan Sales Value'].sum()
                haf_total_units = haf_support['Sold Units'].sum()
                haf_support_pct = (haf_total_support / haf_total_psv * 100) if haf_total_psv != 0 else 0

                total_row = {col: None for col in haf_support.columns}
                total_row['Amazon ASIN'] = 'Grand Total'
                total_row['Sold Units'] = haf_total_units
                total_row['Support'] = haf_total_support
                total_row['Plan Sales Value'] = haf_total_psv
                haf_support = pd.concat([haf_support, pd.DataFrame([total_row])], ignore_index=True)

                pct_row = {col: None for col in haf_support.columns}
                pct_row['Amazon ASIN'] = 'Support %'
                pct_row['Support'] = haf_support_pct
                haf_support = pd.concat([haf_support, pd.DataFrame([pct_row])], ignore_index=True)

            st.success(f"✅ Hafele Secondary processed! Total Support: ₹{haf_total_support:,.2f} | Support %: {haf_support_pct:.2f}%")

            # KPI Cards
            data_rows_haf = haf_support[~haf_support['Amazon ASIN'].isin(['Grand Total', 'Support %'])]
            profitable = int((pd.to_numeric(data_rows_haf['L/P'], errors='coerce') > 0).sum())
            loss_making = int((pd.to_numeric(data_rows_haf['L/P'], errors='coerce') < 0).sum())

            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Total SKUs", len(data_rows_haf))
            k2.metric("Profitable", profitable)
            k3.metric("Loss-Making", loss_making)
            k4.metric("Total Support", f"₹{haf_total_support:,.0f}")
            k5.metric("Support %", f"{haf_support_pct:.2f}%")

            # Sub-tabs
            haf_tab1, haf_tab2, haf_tab3 = st.tabs(["📋 Final Support Table", "📦 Units Sold Pivot", "🔍 Hafele Orders"])

            with haf_tab1:
                st.subheader("Hafele Support Sheet (Final)")
                # Color code P/L or Support? Original code had color functions.
                # Let's use simple highlight for now as in other secondary tabs.
                st.dataframe(haf_support, use_container_width=True, height=500)
                st.download_button("📥 Download Support Table", convert_to_excel(haf_support, 'Hafele Support'), "hafele_support.xlsx")

            with haf_tab2:
                st.subheader("Units Sold per ASIN")
                st.dataframe(haf_pivot, use_container_width=True, height=400)
                st.download_button("📥 Download Units Pivot", convert_to_excel(haf_pivot, 'Units Pivot'), "hafele_pivot.xlsx")

            with haf_tab3:
                st.subheader("Amazon Orders — Hafele Brand")
                st.caption(f"{len(haf_brand_orders):,} rows")
                st.dataframe(haf_brand_orders.head(500), use_container_width=True, height=400)
                if len(haf_brand_orders) > 500:
                    st.info(f"Showing first 500 of {len(haf_brand_orders):,} rows")
                st.download_button("📥 Download Orders", convert_to_excel(haf_brand_orders, 'Hafele Orders'), "hafele_orders.xlsx")

            # Combined Summary
            haf_combined_df = pd.DataFrame({"Brand": ["Hafele (Secondary)"], "Hafele Sec Support": [haf_total_support]})
            combined_results.append(haf_combined_df)

        except Exception as e:
            st.error(f"❌ Error processing Hafele Secondary: {str(e)}")
    else:
        st.warning("Please upload Hafele Support Excel, Orders TXT, and PM file.")

# ==========================================
# TAB 15: PANASONIC SECONDARY
# ==========================================
with tabs[12]:
    st.header("📺 Panasonic Secondary Support")

    with st.expander("👁️ Preview Sample: Panasonic Support Sheet (Panasonicsupport.xlsx)", expanded=False):
        _pana_sample = pd.DataFrame([
            {"Amazon ASIN": "B0BT9DYKHN", "Sold Units": "", "SKU Code": 63153844, "Desc": "SKT PLUS PROFESSIONAL", "Corrected NLC": 3082.0, "Current CSP": 4499, "Current Commission": 0.045, "Amazon Referral Fee": 202.455, "Monthly Storage Fee": 35.07, "Fixed Closing Fee": 51, "Pick & Pack Fee": 14.5, "Weight Handling Fee": 227.5, "Return Fee": 44.99, "GST": 103.5927, "Total FBA Fee +GST": 679.1077, "DB Margin": 224.95, "Payout": 3594.9423, "L/P": 512.9423, "Percent": 0.114, "Support": 0, "Plan Sales Value": 0},
            {"Amazon ASIN": "B098P7STVY", "Sold Units": "", "SKU Code": 63153748, "Desc": "Wonderchef Nutri Blend Bolt FP 600W Black", "Corrected NLC": 3041.22, "Current CSP": 4299, "Current Commission": 0.045, "Amazon Referral Fee": 193.455, "Monthly Storage Fee": 28.94, "Fixed Closing Fee": 51, "Pick & Pack Fee": 14.5, "Weight Handling Fee": 188.0, "Return Fee": 42.99, "GST": 93.3993, "Total FBA Fee +GST": 612.2843, "DB Margin": 214.95, "Payout": 3471.7657, "L/P": 430.5457, "Percent": 0.1002, "Support": 0, "Plan Sales Value": 0},
            {"Amazon ASIN": "B01HXWI2P2", "Sold Units": "", "SKU Code": 63151935, "Desc": "Smoky Grill Electric Barbeque", "Corrected NLC": 2983.0, "Current CSP": 4499, "Current Commission": 0.125, "Amazon Referral Fee": 562.375, "Monthly Storage Fee": 60.43, "Fixed Closing Fee": 51, "Pick & Pack Fee": 14.5, "Weight Handling Fee": 256.04, "Return Fee": 44.99, "GST": 178.0803, "Total FBA Fee +GST": 1167.4153, "DB Margin": 224.95, "Payout": 3106.6347, "L/P": 123.6347, "Percent": 0.0275, "Support": 0, "Plan Sales Value": 0},
        ])
        st.caption(f"📄 3 sample rows × {len(_pana_sample.columns)} columns — read-only preview of expected Panasonic Support format")
        st.dataframe(_pana_sample, use_container_width=True, height=200)

    if panasonic_support_file and panasonic_orders_file and pm_file:
        try:
            with st.spinner("Processing Panasonic Secondary data..."):
                # Load Panasonic sheet (header at row 2 -> header=1)
                pana_support = pd.read_excel(panasonic_support_file, header=1)

                # Load orders (tab-separated TXT)
                pana_orders = pd.read_csv(panasonic_orders_file, sep="\t", low_memory=False, dtype=str)
                pana_pm = pm_df.copy()

                # Clean ASINs
                pana_orders['asin'] = pana_orders['asin'].astype(str).str.strip()
                pana_pm['ASIN'] = pana_pm['ASIN'].astype(str).str.strip()

                # Merge Brand from PM
                pana_orders = pana_orders.merge(pana_pm[['ASIN', 'Brand']], left_on='asin', right_on='ASIN', how='left')
                pana_orders.drop(columns=['ASIN'], inplace=True, errors='ignore')

                # Convert numeric cols
                pana_orders['item-price'] = pd.to_numeric(pana_orders['item-price'], errors='coerce')
                pana_orders = pana_orders[pana_orders['item-price'].notna()]
                pana_orders['quantity'] = pd.to_numeric(pana_orders['quantity'], errors='coerce').fillna(0)

                # Filter Panasonic brand
                pana_brand_orders = pana_orders[pana_orders['Brand'] == 'Panasonic'].copy()

                # Pivot: sold units per ASIN
                pana_pivot = (
                    pd.pivot_table(pana_brand_orders, index='asin', values='quantity', aggfunc='sum')
                    .sort_values(by='quantity', ascending=False)
                    .reset_index()
                )
                pana_pivot.columns = ['Amazon ASIN', 'Sold Units']

                # Map sold units into support sheet
                pana_support['Amazon ASIN'] = pana_support['Amazon ASIN'].astype(str).str.strip()
                pana_pivot['Amazon ASIN'] = pana_pivot['Amazon ASIN'].astype(str).str.strip()
                asin_to_units = dict(zip(pana_pivot['Amazon ASIN'], pana_pivot['Sold Units']))
                pana_support['Sold Units'] = pana_support['Amazon ASIN'].map(asin_to_units).fillna(0)

                # Compute Support and Plan Sales Value
                pana_support['L/P'] = pd.to_numeric(pana_support['L/P'], errors='coerce').fillna(0)
                pana_support['Sold Units'] = pd.to_numeric(pana_support['Sold Units'], errors='coerce').fillna(0)
                pana_support['Support'] = pana_support['L/P'] * pana_support['Sold Units']
                pana_support['Plan Sales Value'] = pana_support['Sold Units'] * pd.to_numeric(pana_support['Current CSP'], errors='coerce').fillna(0)

                # Grand Total row
                pana_total_support = pana_support['Support'].sum()
                pana_total_psv = pana_support['Plan Sales Value'].sum()
                pana_total_units = pana_support['Sold Units'].sum()
                pana_support_pct = (pana_total_support / pana_total_psv * 100) if pana_total_psv != 0 else 0

                total_row = {col: None for col in pana_support.columns}
                total_row['Amazon ASIN'] = 'Grand Total'
                total_row['Sold Units'] = pana_total_units
                total_row['Current CSP'] = pd.to_numeric(pana_support['Current CSP'], errors='coerce').sum()
                total_row['Support'] = pana_total_support
                total_row['Plan Sales Value'] = pana_total_psv
                pana_support = pd.concat([pana_support, pd.DataFrame([total_row])], ignore_index=True)

                pct_row = {col: None for col in pana_support.columns}
                pct_row['Amazon ASIN'] = 'Support %'
                pct_row['Support'] = pana_support_pct
                pana_support = pd.concat([pana_support, pd.DataFrame([pct_row])], ignore_index=True)

            st.success(f"✅ Panasonic Secondary processed! Total Support: ₹{pana_total_support:,.2f} | Support %: {pana_support_pct:.2f}%")

            # KPI Cards
            data_rows_pana = pana_support[~pana_support['Amazon ASIN'].isin(['Grand Total', 'Support %'])]
            profitable = int((pd.to_numeric(data_rows_pana['L/P'], errors='coerce') > 0).sum())
            loss_making = int((pd.to_numeric(data_rows_pana['L/P'], errors='coerce') < 0).sum())

            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Total SKUs", len(data_rows_pana))
            k2.metric("Profitable", profitable)
            k3.metric("Loss-Making", loss_making)
            k4.metric("Total Support", f"₹{pana_total_support:,.0f}")
            k5.metric("Support %", f"{pana_support_pct:.2f}%")

            # Sub-tabs
            pana_tab1, pana_tab2, pana_tab3 = st.tabs(["📋 Final Support Table", "📦 Units Sold Pivot", "🔍 Panasonic Orders"])

            with pana_tab1:
                st.subheader("Panasonic Support Sheet (Final)")
                st.dataframe(pana_support, use_container_width=True, height=500)
                st.download_button("📥 Download Support Table", convert_to_excel(pana_support, 'Panasonic Support'), "panasonic_support.xlsx")

            with pana_tab2:
                st.subheader("Units Sold per ASIN")
                st.dataframe(pana_pivot, use_container_width=True, height=400)
                st.download_button("📥 Download Units Pivot", convert_to_excel(pana_pivot, 'Units Pivot'), "panasonic_pivot.xlsx")

            with pana_tab3:
                st.subheader("Amazon Orders — Panasonic Brand")
                st.caption(f"{len(pana_brand_orders):,} rows")
                st.dataframe(pana_brand_orders.head(500), use_container_width=True, height=400)
                if len(pana_brand_orders) > 500:
                    st.info(f"Showing first 500 of {len(pana_brand_orders):,} rows")
                st.download_button("📥 Download Orders", convert_to_excel(pana_brand_orders, 'Panasonic Orders'), "panasonic_orders.xlsx")

            # Combined Summary
            pana_combined_df = pd.DataFrame({"Brand": ["Panasonic (Secondary)"], "Panasonic Sec Support": [pana_total_support]})
            combined_results.append(pana_combined_df)

        except Exception as e:
            st.error(f"❌ Error processing Panasonic Secondary: {str(e)}")
    else:
        st.warning("Please upload Panasonic Support Excel, Orders TXT, and PM file.")


# ==========================================
# TAB 16: INALSA SECONDARY
# ==========================================
with tabs[13]:
    st.header("📦 Inalsa Secondary Support")
    st.markdown("Generates the Credit Note summary for Inalsa/Taurus brands.")

    if inalsa_b2b_zips and inalsa_b2c_zips and pm_file and inalsa_unified_csv and inalsa_storage_csv:
        try:
            with st.spinner("Processing Inalsa Secondary data..."):
                def read_zip_single(uploaded_zip):
                    with zipfile.ZipFile(io.BytesIO(uploaded_zip.read()), "r") as z:
                        file_name = z.namelist()[0]
                        with z.open(file_name) as f:
                            if file_name.endswith(".csv"):
                                return pd.read_csv(f)
                            elif file_name.endswith((".xlsx", ".xls")):
                                return pd.read_excel(f)
                            else:
                                raise ValueError(f"Unsupported file format inside zip: {file_name}")

                def read_zips_list(zip_list):
                    frames = [read_zip_single(z) for z in zip_list]
                    return pd.concat(frames, ignore_index=True)

                # 1. Load B2B + B2C reports
                df_b2b = read_zips_list(inalsa_b2b_zips)
                df_b2c = read_zips_list(inalsa_b2c_zips)
                final_df = pd.concat([df_b2b, df_b2c], ignore_index=True)

                st.caption(f"Loaded {len(inalsa_b2b_zips)} B2B file(s) and {len(inalsa_b2c_zips)} B2C file(s) → {len(final_df):,} rows.")

                # 2. Merge Brand from PM
                inalsa_pm = pm_df.copy()
                pm_lookup = inalsa_pm[['ASIN', 'Brand']].copy()
                final_df = final_df.merge(pm_lookup, left_on="Asin", right_on="ASIN", how="left")
                final_df.drop(columns=["ASIN"], inplace=True)

                # 3. Filter brands
                final_df = final_df[final_df["Brand"].isin(["Inalsa", "Taurus"])]

                # 4. Remove Cancel transactions
                final_df = final_df[final_df["Transaction Type"] != "Cancel"]

                # 5. FreeReplacement → Quantity = 0
                final_df = final_df.copy()
                final_df["Transaction Type"] = final_df["Transaction Type"].astype(str).str.strip()
                final_df.loc[final_df["Transaction Type"].str.lower() == "freereplacement", "Quantity"] = 0

                # 6. Refund → Quantity negative
                final_df["Quantity"] = pd.to_numeric(final_df["Quantity"], errors="coerce")
                mask_refund = final_df["Transaction Type"].str.lower() == "refund"
                final_df.loc[mask_refund, "Quantity"] = -final_df.loc[mask_refund, "Quantity"].abs()

                # 7. Load Unified Transaction CSV
                inalsa_unified_csv.seek(0)
                df_uni = pd.read_csv(inalsa_unified_csv, encoding="utf-8", low_memory=False, header=11)
                df_uni = df_uni[df_uni["type"].isin(["Fulfilment Fee Refund", "Order", "Refund"])]

                numeric_cols_uni = [
                    "product sales", "shipping credits", "promotional rebates",
                    "selling fees", "fba fees", "other transaction fees",
                ]
                for col in numeric_cols_uni:
                    df_uni[col] = pd.to_numeric(df_uni[col], errors="coerce")

                pivot_df = df_uni.groupby("order id")[numeric_cols_uni].sum().reset_index()

                # 8. Select required columns from final_df
                required_columns_inalsa = [
                    "Seller Gstin", "Invoice Number", "Invoice Date", "Transaction Type",
                    "Order Id", "Shipment Id", "Shipment Date", "Order Date",
                    "Shipment Item Id", "Quantity", "Item Description", "Asin", "Brand",
                    "Invoice Amount", "Tax Exclusive Gross", "Total Tax Amount",
                ]
                available_cols = [c for c in required_columns_inalsa if c in final_df.columns]
                final_df = final_df[available_cols]

                # Merge pivot
                final_df = final_df.merge(pivot_df, left_on="Order Id", right_on="order id", how="left")
                final_df.drop(columns=["order id"], inplace=True, errors="ignore")

                # 9. Capture NaN product sales BEFORE removing them
                for col in numeric_cols_uni:
                    if col in final_df.columns:
                        final_df[col] = pd.to_numeric(final_df[col], errors="coerce")

                final_df["Amazon Fees"] = final_df[[c for c in numeric_cols_uni if c in final_df.columns]].sum(axis=1)

                nan_product_sales_df = final_df[final_df["product sales"].isna()].copy()
                final_df = final_df[final_df["product sales"].notna()]

                # 10. With GST Amount Fees
                final_df["With GST Amount Fees"] = (final_df["Amazon Fees"] / 1.18).round(2)

                # 11. Purchase Price & Purchase Cost
                pm_cp_lookup = inalsa_pm.iloc[:, [0, 9]].copy()
                pm_cp_lookup.columns = ["ASIN", "Purchase Price"]
                final_df = final_df.merge(pm_cp_lookup, left_on="Asin", right_on="ASIN", how="left")
                final_df.drop(columns=["ASIN"], inplace=True, errors="ignore")

                final_df["Purchase Price"] = pd.to_numeric(final_df["Purchase Price"], errors="coerce").fillna(0)
                final_df["Quantity"] = pd.to_numeric(final_df["Quantity"], errors="coerce").fillna(0)
                final_df["Purchase Cost"] = (final_df["Purchase Price"] * final_df["Quantity"]).round(2)
                final_df["Purchase Cost"] = pd.to_numeric(final_df["Purchase Cost"], errors="coerce").fillna(0)

                # 12. Base PM, Gross & Net Margin, Agreed Margin
                final_df["Base PM"] = (final_df["Purchase Cost"] * 1.18).round(2)
                final_df["Gross Margin"] = (final_df["Tax Exclusive Gross"] - final_df["Base PM"]).round(2)
                final_df["Net Margin"] = (final_df["Gross Margin"] + final_df["With GST Amount Fees"]).round(2)
                final_df["Agreed Margin"] = (final_df["Tax Exclusive Gross"] * 0.04).round(2)
                final_df["Amount of CN"] = (final_df["Agreed Margin"] - final_df["Net Margin"]).round(2)

                # 13. Grand Total row
                numeric_cols_final = final_df.select_dtypes(include="number").columns
                total_row_inalsa = {col: None for col in final_df.columns}
                total_row_inalsa["Seller Gstin"] = "Grand Total"
                for col in numeric_cols_final:
                    total_row_inalsa[col] = final_df[col].sum()
                final_with_total = pd.concat([final_df, pd.DataFrame([total_row_inalsa])], ignore_index=True)

                grand_total    = final_with_total.iloc[-1]
                net_sales      = grand_total["Tax Exclusive Gross"]
                minimum_margin = grand_total["Agreed Margin"]
                gross_margin   = grand_total["Gross Margin"]
                amazon_fees    = grand_total["With GST Amount Fees"]

                # 14. Storage fees
                inalsa_storage_csv.seek(0)
                storage_df = pd.read_csv(inalsa_storage_csv)

                storage_df = storage_df.merge(pm_lookup, left_on="asin", right_on="ASIN", how="left")
                storage_df.drop(columns=["ASIN"], inplace=True, errors="ignore")
                storage_df = storage_df[storage_df["Brand"].isin(["Inalsa", "Taurus"])]

                storage_df["estimated-monthly-storage-fee"] = pd.to_numeric(
                    storage_df["estimated-monthly-storage-fee"], errors="coerce"
                ).fillna(0)

                total_storage_fee = storage_df["estimated-monthly-storage-fee"].sum()
                total_storage_without_gst = -abs(round(total_storage_fee / 1.18, 2))
                storage_fees = total_storage_without_gst

                # 15. CN Summary
                total_abc = gross_margin + amazon_fees + storage_fees
                credit_note_amount = minimum_margin - total_abc

                cn_summary = pd.DataFrame({
                    "Particulars": [
                        "Net Sales", "Minimum Margin (4%)", "a. Gross Margin", "b. Amazon Fees (Without GST)",
                        "c. Storage Fees (Without GST)", "Total (a+b+c)", "Credit Note Amount",
                    ],
                    "Amount (₹)": [
                        round(float(net_sales), 2), round(float(minimum_margin), 2), round(float(gross_margin), 2),
                        round(float(amazon_fees), 2), round(float(storage_fees), 2), round(float(total_abc), 2),
                        round(float(credit_note_amount), 2),
                    ],
                })

                st.success(f"✅ Inalsa Secondary processed! Credit Note Amount: ₹{credit_note_amount:,.2f}")

                # Display Results
                st.subheader("📊 Summary")
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Net Sales", format_currency(float(net_sales)))
                c2.metric("Minimum Margin (4%)", format_currency(float(minimum_margin)))
                c3.metric("Total (a+b+c)", format_currency(float(total_abc)))
                c4.metric("💳 Credit Note", format_currency(float(credit_note_amount)))

                inalsa_tab1, inalsa_tab2, inalsa_tab3, inalsa_tab4 = st.tabs(["📋 CN Summary", "🗂️ Detailed Data", "🏭 Storage Fees", "⚠️ NaN Report"])

                with inalsa_tab1:
                    def highlight_cn(row):
                        if row["Particulars"] == "Credit Note Amount":
                            return ["background-color: #d4edda; font-weight: bold"] * len(row)
                        if row["Particulars"] == "Total (a+b+c)":
                            return ["background-color: #fff3cd"] * len(row)
                        return [""] * len(row)
                    st.dataframe(cn_summary.style.apply(highlight_cn, axis=1), use_container_width=True, hide_index=True)
                    st.download_button("📥 Download CN Summary", convert_to_excel(cn_summary, 'Summary'), "inalsa_cn_summary.xlsx")
                
                with inalsa_tab2:
                    display_cols = [c for c in [
                        "Seller Gstin", "Invoice Number", "Invoice Date", "Transaction Type",
                        "Order Id", "Asin", "Brand", "Quantity", "Invoice Amount",
                        "Tax Exclusive Gross", "Total Tax Amount", "Amazon Fees",
                        "With GST Amount Fees", "Purchase Cost", "Base PM",
                        "Gross Margin", "Net Margin", "Agreed Margin", "Amount of CN",
                    ] if c in final_with_total.columns]
                    st.dataframe(final_with_total[display_cols], use_container_width=True, height=400)
                    st.download_button("📥 Download Detailed Data", convert_to_excel(final_with_total[display_cols], 'Data'), "inalsa_detailed.xlsx")
                
                with inalsa_tab3:
                    st.markdown(f"- **Total Storage Fee (with GST):** {format_currency(total_storage_fee)}")
                    st.markdown(f"- **Total Storage Fee (without GST):** {format_currency(abs(float(total_storage_without_gst)))}")
                    storage_display_cols = [c for c in [
                        "asin", "fnsku", "product-name", "fulfillment-center",
                        "estimated-monthly-storage-fee", "Brand",
                    ] if c in storage_df.columns]
                    st.dataframe(storage_df[storage_display_cols], use_container_width=True, height=400)
                    st.download_button("📥 Download Storage Data", convert_to_excel(storage_df[storage_display_cols], 'Storage'), "inalsa_storage.xlsx")
                
                with inalsa_tab4:
                    st.markdown(f"**{len(nan_product_sales_df):,} rows** have no matching data in the Unified Transaction file (i.e. product sales is NaN). Excluded from calculation.")
                    nan_display_cols = [c for c in [
                        "Seller Gstin", "Invoice Number", "Invoice Date", "Transaction Type",
                        "Order Id", "Asin", "Brand", "Quantity", "Invoice Amount",
                        "Tax Exclusive Gross", "Total Tax Amount",
                    ] if c in nan_product_sales_df.columns]
                    st.dataframe(nan_product_sales_df[nan_display_cols], use_container_width=True, height=400)
                    st.download_button("📥 Download NaN Report", convert_to_excel(nan_product_sales_df[nan_display_cols], 'NaN Report'), "inalsa_nan_report.xlsx")

                # Combined Summary
                # Note: The combined summary looks for support as a positive metric. Since this is a "Credit Note Amount", we use it as the Support value.
                inalsa_combined_df = pd.DataFrame({"Brand": ["Inalsa/Taurus (Secondary)"], "Inalsa Sec CN Amount": [credit_note_amount]})
                combined_results.append(inalsa_combined_df)

        except Exception as e:
            st.error(f"❌ Error processing Inalsa Secondary: {str(e)}")
    else:
        st.warning("Please upload B2B/B2C ZIPs, PM file, Unified Transaction CSV, and Storage Fee CSV.")

# ==========================================
# TAB 17: VICTORINOX SECONDARY
# ==========================================
with tabs[14]:
    st.header("🔪 Victorinox Secondary Support")
    st.markdown("Upload Victorinox Support Excel (auto-detects sheets) + Amazon Orders TXT + Purchase Master.")

    if victorinox_support_file and victorinox_orders_file and pm_file:
        try:
            # Re-read support_file to get sheet names
            victorinox_support_file.seek(0)
            support_bytes = victorinox_support_file.read()
            xl_file = pd.ExcelFile(io.BytesIO(support_bytes))
            all_sheet_names = xl_file.sheet_names

            st.markdown("### 📋 Select Sheets to Process")
            
            # Using unique keys for multiselect
            if "vic_select_all" not in st.session_state:
                st.session_state.vic_select_all = False

            col_sel1, col_sel2 = st.columns([4, 1])
            with col_sel2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("✅ Select All Sheets" if not st.session_state.vic_select_all else "❌ Deselect All"):
                    st.session_state.vic_select_all = not st.session_state.vic_select_all
                    st.rerun()

            with col_sel1:
                default_sheets = all_sheet_names if st.session_state.vic_select_all else [all_sheet_names[0]]
                selected_sheets = st.multiselect(
                    f"Choose sheets to process ({len(all_sheet_names)} available):",
                    options=all_sheet_names,
                    default=default_sheets,
                    help="Select one or multiple sheets. Each sheet gets its own results section below."
                )

            if selected_sheets:
                with st.spinner("Loading Orders and Purchase Master..."):
                    victorinox_orders_file.seek(0)
                    df_orders = pd.read_csv(victorinox_orders_file, sep="\t", low_memory=False)
                    df_pm = pm_df.copy()

                    df_orders["asin"] = df_orders["asin"].astype(str).str.strip()
                    df_pm["ASIN"] = df_pm["ASIN"].astype(str).str.strip()

                    df_orders = df_orders.merge(
                        df_pm[["ASIN", "Brand"]],
                        left_on="asin", right_on="ASIN", how="left"
                    )
                    df_orders.drop(columns=["ASIN"], inplace=True, errors="ignore")

                def run_vic_pipeline(sheet_name, supp_bytes, df_orders_base):
                    # Load sheet (header on row 2 → header=1)
                    df = pd.read_excel(io.BytesIO(supp_bytes), header=1, sheet_name=sheet_name)
                    df["ASIN"] = df["ASIN"].astype(str).str.strip()

                    # Filter Victorinox brand first, then item-price
                    df_vic = df_orders_base[df_orders_base["Brand"] == "Victorinox"].copy()
                    df_vic["item-price"] = pd.to_numeric(df_vic["item-price"], errors="coerce")
                    df_vic = df_vic[df_vic["item-price"].notna()]

                    # Pivot: qty sold per ASIN
                    pivot = (
                        pd.pivot_table(df_vic, index="asin", values="quantity", aggfunc="sum")
                        .sort_values(by="quantity", ascending=False)
                        .reset_index()
                    )
                    pivot.columns = ["ASIN", "Qty Sold"]
                    pivot["ASIN"] = pivot["ASIN"].astype(str).str.strip()

                    # Map qty sold into support sheet
                    asin_map = dict(zip(pivot["ASIN"], pivot["Qty Sold"]))
                    df["Qty Sold"] = df["ASIN"].map(asin_map).fillna(0)

                    # Numeric coerce
                    df["NLC Diff"] = pd.to_numeric(df["NLC Diff"], errors="coerce").fillna(0)
                    df["Qty Sold"] = pd.to_numeric(df["Qty Sold"], errors="coerce").fillna(0)

                    # CN Value = NLC Diff × Qty Sold
                    df["CN Value"] = df["NLC Diff"] * df["Qty Sold"]

                    # Grand Total row
                    total_row = {col: None for col in df.columns}
                    total_row["ASIN"] = "Grand Total"
                    total_row["Qty Sold"] = df["Qty Sold"].sum()
                    total_row["NLC Diff"] = df["NLC Diff"].sum()
                    total_row["CN Value"] = df["CN Value"].sum()
                    df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

                    return df, df_vic, pivot

                results = {}
                pipeline_errors = []

                with st.spinner(f"Running pipeline on {len(selected_sheets)} sheet(s)..."):
                    for sheet in selected_sheets:
                        try:
                            vdf, df_vic, piv = run_vic_pipeline(sheet, support_bytes, df_orders)
                            results[sheet] = (vdf, df_vic, piv)
                        except Exception as e:
                            pipeline_errors.append((sheet, str(e)))

                if pipeline_errors:
                    for sheet, err in pipeline_errors:
                        st.error(f"❌ Error processing sheet **{sheet}**: {err}")

                if results:
                    st.success(f"✅ Pipeline complete for {len(results)} sheet(s)!")

                    def safe_fmt(fmt_str):
                        def _fmt(val):
                            if val == "" or val is None or pd.isna(val):
                                return ""
                            try:
                                return fmt_str.format(float(val))
                            except (ValueError, TypeError):
                                return str(val)
                        return _fmt

                    NUM_FMT = {
                        "MRP": safe_fmt("{:,.2f}"), "BAU Discount": safe_fmt("{:.1f}%"),
                        "Event Discount": safe_fmt("{:.1f}%"), "BAU    NLC": safe_fmt("{:,.4f}"),
                        "Event NLC": safe_fmt("{:,.4f}"), "NLC Diff": safe_fmt("{:,.4f}"),
                        "Qty Sold": safe_fmt("{:,.0f}"), "CN Value": safe_fmt("{:,.3f}"),
                    }
                    SHOW_COLS_ORDER = ["SKU", "Category", "ASIN", "Name", "MRP", "BAU Discount", "Event Discount", "BAU    NLC", "Event NLC", "NLC Diff", "Qty Sold", "CN Value"]

                    st.subheader(f"📂 Results ({len(results)} Sheets)")
                    sheet_tabs = st.tabs([f"📄 {name}" for name in results.keys()])

                    total_victorinox_cn = 0

                    for idx, (tab, (sheet_name, (vdf, df_vic, piv))) in enumerate(zip(sheet_tabs, results.items())):
                        with tab:
                            summary_mask = vdf["ASIN"] == "Grand Total"
                            summary_rows = vdf[summary_mask]
                            data_rows = vdf[~summary_mask].copy()

                            total_cn = float(vdf.iloc[-1]["CN Value"])
                            total_victorinox_cn += total_cn
                            total_skus = len(data_rows)
                            positive_nlc = int((pd.to_numeric(data_rows["NLC Diff"], errors="coerce") > 0).sum())
                            zero_nlc = int((pd.to_numeric(data_rows["NLC Diff"], errors="coerce") == 0).sum())
                            orders_total = int(df_vic["quantity"].sum()) if "quantity" in df_vic.columns else 0

                            k1, k2, k3, k4, k5 = st.columns(5)
                            k1.metric("Total SKUs", total_skus)
                            k2.metric("SKUs with NLC Diff", positive_nlc)
                            k3.metric("SKUs — Zero NLC Diff", zero_nlc)
                            k4.metric("Total Orders", f"{orders_total:,}")
                            k5.metric("Total CN Value", format_currency(total_cn))

                            t1, t2, t3, t4 = st.tabs(["📋 Final Table", "📊 Charts", "🔍 Orders Detail", "📦 Units Sold Pivot"])

                            with t1:
                                fl1, fl2 = st.columns([3, 2])
                                with fl1:
                                    search = st.text_input("🔍 Search Product Name", "", key=f"vic_search_{idx}")
                                with fl2:
                                    cat_options = ["All"] + sorted(data_rows["Category"].dropna().unique().tolist()) if "Category" in data_rows.columns else ["All"]
                                    cat_filter = st.selectbox("Filter by Category", cat_options, key=f"vic_cat_{idx}")

                                display = data_rows.copy()
                                if search:
                                    display = display[display["Name"].astype(str).str.contains(search, case=False, na=False)]
                                if cat_filter != "All" and "Category" in display.columns:
                                    display = display[display["Category"] == cat_filter]

                                show_cols = [c for c in SHOW_COLS_ORDER if c in display.columns]
                                sum_show = [c for c in show_cols if c in summary_rows.columns]
                                full_display = pd.concat([display[show_cols], summary_rows[sum_show]], ignore_index=True)

                                def highlight_rows(row):
                                    if row.get("ASIN") == "Grand Total":
                                        return ["background-color:#fff3cd;font-weight:bold;color:#856404"] * len(row)
                                    try:
                                        v = float(row.get("NLC Diff", 0))
                                        color = "background-color:#d4edda;color:#155724" if v > 0 else ""
                                        return [color if col == "NLC Diff" else "" for col in row.index]
                                    except:
                                        return ["" for _ in row.index]

                                styled = full_display.style.apply(highlight_rows, axis=1).format({k: v for k, v in NUM_FMT.items() if k in show_cols})
                                st.dataframe(styled, use_container_width=True, height=400)
                                st.download_button("⬇️ Download Table", display[show_cols].to_csv(index=False).encode("utf-8"), f"victorinox_table_{sheet_name}.csv", "text/csv", key=f"vic_dl_tbl_{idx}")

                            with t2:
                                chart_data = data_rows.copy()
                                for col in ["NLC Diff", "CN Value", "Qty Sold"]:
                                    if col in chart_data.columns:
                                        chart_data[col] = pd.to_numeric(chart_data[col], errors="coerce")

                                c1, c2 = st.columns(2)
                                with c1:
                                    st.markdown("**Top 20 – NLC Diff by SKU**")
                                    if "Name" in chart_data.columns and "NLC Diff" in chart_data.columns:
                                        top_nlc = chart_data[["Name", "NLC Diff"]].dropna().sort_values("NLC Diff", ascending=False).head(20).set_index("Name")
                                        st.bar_chart(top_nlc)
                                with c2:
                                    st.markdown("**CN Value by SKU (Top 20)**")
                                    if "Name" in chart_data.columns and "CN Value" in chart_data.columns:
                                        cn_chart = chart_data[["Name", "CN Value"]].dropna().sort_values("CN Value", ascending=False).head(20).set_index("Name")
                                        st.bar_chart(cn_chart)

                            with t3:
                                st.caption(f"{len(df_vic):,} rows after brand + item-price filter")
                                st.dataframe(df_vic.head(1000), use_container_width=True, height=400)
                                st.download_button("⬇️ Download Orders", df_vic.to_csv(index=False).encode("utf-8"), f"victorinox_orders_{sheet_name}.csv", "text/csv", key=f"vic_dl_ord_{idx}")

                            with t4:
                                st.dataframe(piv, use_container_width=True, height=400)
                                st.download_button("⬇️ Download Pivot", piv.to_csv(index=False).encode("utf-8"), f"victorinox_pivot_{sheet_name}.csv", "text/csv", key=f"vic_dl_piv_{idx}")

                    # Combined Summary
                    vic_combined_df = pd.DataFrame({"Brand": ["Victorinox (Secondary)"], "Victorinox Sec CN Value": [total_victorinox_cn]})
                    combined_results.append(vic_combined_df)
            else:
                st.warning("Please select at least one sheet to process from the Victorinox Support Excel.")

        except Exception as e:
            st.error(f"❌ Error initializing Victorinox Secondary data: {str(e)}")
# ==========================================
# TAB 18: CURRENT INVENTORY
# ==========================================
with tabs[15]:
    st.header("📦 Current Inventory Analysis")
    if current_inv_file and pm_file:
        try:
            # Load Data
            inv_df = pd.read_csv(current_inv_file)
            inv_df.columns = inv_df.columns.str.lower()
            
            pm_full = pm_df.copy()
            pm_full.columns = pm_full.columns.str.lower()
            
            # PM lookup — col 0=asin, col 4=brand manager, col 6=brand, col 9=cp
            # unified_support_app uses columns[2] for SKU, but current_inv needs ASIN (col 0)
            pm_lookup = pm_full.iloc[:, [0, 4, 6, 9]].copy()
            pm_lookup.columns = ["asin", "brand manager", "brand", "cp"]
            pm_lookup = pm_lookup.drop_duplicates(subset="asin")
            
            inv_proc = inv_df.merge(pm_lookup, on="asin", how="left")
            inv_proc["cp"] = pd.to_numeric(inv_proc["cp"], errors="coerce")
            inv_proc["afn-warehouse-quantity"] = pd.to_numeric(inv_proc["afn-warehouse-quantity"], errors="coerce")
            inv_proc["CP As Per Qty"] = inv_proc["cp"] * inv_proc["afn-warehouse-quantity"]
            
            # Filter out zero stock
            inv_proc = inv_proc[inv_proc["afn-warehouse-quantity"] != 0].copy()
            
            st.success(f"✅ Processed {len(inv_proc)} inventory records with non-zero stock.")
            
            # KPI Container
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total CP Value", format_currency(inv_proc["CP As Per Qty"].sum()))
            c2.metric("Total Warehouse Units", f"{inv_proc['afn-warehouse-quantity'].sum():,.0f}")
            c3.metric("Unique SKUs", inv_proc["sku"].nunique())
            c4.metric("Brands", inv_proc["brand"].nunique())
            
            i_tab1, i_tab2 = st.tabs(["📊 Brand Pivot", "📋 Full Detail"])
            
            with i_tab1:
                st.subheader("Brand-wise Inventory Value")
                i_pivot = inv_proc.groupby("brand")["CP As Per Qty"].sum().reset_index()
                i_pivot.columns = ["Brand", "CP Inventory Value"]
                
                i_summary = pd.DataFrame({"Brand": ["Grand Total"], "CP Inventory Value": [i_pivot["CP Inventory Value"].sum()]})
                i_pivot_disp = pd.concat([i_pivot, i_summary], ignore_index=True)
                
                st.dataframe(make_arrow_safe(i_pivot_disp), use_container_width=True)
                st.download_button("📥 Download Inventory Pivot", i_pivot_disp.to_csv(index=False).encode(), "inventory_brand_pivot.csv")
                
                # Combine result
                combined_results.append(i_pivot.rename(columns={"CP Inventory Value": "Current Inventory"}))
                
            with i_tab2:
                st.subheader("Full Inventory Detail")
                st.dataframe(make_arrow_safe(inv_proc), use_container_width=True)
                st.download_button("📥 Download Full Inventory", inv_proc.to_csv(index=False).encode(), "inventory_full_report.csv")
                
        except Exception as e:
            st.error(f"❌ Error processing Current Inventory: {e}")
    else:
        st.warning("Please upload Inventory CSV and PM file.")



# ==========================================
# TAB 19: REIMBURSEMENT FBA
# ==========================================
with tabs[16]:
    st.header("💰 Reimbursement - FBA Analysis")
    if reimb_fba_file and pm_file:
        try:
            r_fba = pd.read_csv(reimb_fba_file)
            r_fba = r_fba[r_fba["reason"].isin(["CustomerReturn", "CustomerServiceIssue"])].copy()
            r_fba.columns = r_fba.columns.str.lower()
            
            pm_full = pm_df.copy()
            pm_full.columns = pm_full.columns.str.lower()
            
            # PM lookup — col 0=asin, col 4=brand manager, col 6=brand, col 7=product name
            r_pm_lookup = pm_full.iloc[:, [0, 4, 6, 7]].copy()
            r_pm_lookup.columns = ["asin", "brand manager", "brand", "product name"]
            r_pm_lookup = r_pm_lookup.drop_duplicates(subset="asin")
            
            r_fba = r_fba.merge(r_pm_lookup, on="asin", how="left")
            r_fba["amount-total"] = pd.to_numeric(r_fba["amount-total"], errors="coerce")
            
            st.success(f"✅ Processed {len(r_fba)} FBA Reimbursement records.")
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Reimbursement", format_currency(r_fba["amount-total"].sum()))
            k2.metric("Unique Orders", r_fba["amazon-order-id"].nunique())
            k3.metric("Brands", r_fba["brand"].nunique())
            k4.metric("Total Units", f"{r_fba['quantity-reimbursed-total'].sum():,.0f}" if 'quantity-reimbursed-total' in r_fba.columns else "N/A")
            
            rf_tab1, rf_tab2 = st.tabs(["📊 Brand Pivot", "📋 Full Detail"])
            
            with rf_tab1:
                st.subheader("Brand-wise FBA Reimbursement")
                rf_piv = r_fba.groupby("brand")["amount-total"].sum().reset_index()
                rf_piv.columns = ["Brand", "FBA Reimbursement"]
                
                rf_sum = pd.DataFrame({"Brand": ["Grand Total"], "FBA Reimbursement": [rf_piv["FBA Reimbursement"].sum()]})
                rf_piv_disp = pd.concat([rf_piv, rf_sum], ignore_index=True)
                
                st.dataframe(make_arrow_safe(rf_piv_disp), use_container_width=True)
                st.download_button("📥 Download FBA Reimb Pivot", rf_piv_disp.to_csv(index=False).encode(), "reimbursement_fba_pivot.csv")
                
                combined_results.append(rf_piv.rename(columns={"FBA Reimbursement": "Reimbursement FBA"}))
                
            with rf_tab2:
                st.subheader("Full FBA Reimbursement Detail")
                st.dataframe(make_arrow_safe(r_fba), use_container_width=True)
                st.download_button("📥 Download Full FBA Reimb", r_fba.to_csv(index=False).encode(), "reimbursement_fba_full.csv")
                
        except Exception as e:
            st.error(f"❌ Error processing Reimbursement FBA: {e}")
    else:
        st.warning("Please upload Reimbursement (FBA) CSV and PM file.")

# ==========================================
# TAB 20: REIMBURSEMENT SELLER
# ==========================================
with tabs[17]:
    st.header("🛒 Reimbursement - Seller Analysis")
    if reimb_seller_file and pm_file:
        try:
            r_sel = pd.read_csv(reimb_seller_file, skiprows=11)
            r_sel = r_sel[r_sel["type"].isin(["SAFE-T Reimbursement", "Reimbursements"])].copy()
            
            pm_full = pm_df.copy()
            
            # Map Sku -> ASIN via Amazon Sku Name (col 2)
            # Unified app col indices: 0-ASIN, 2-Sku Name
            sku_to_asin = dict(zip(pm_full.iloc[:, 2].astype(str).str.strip(), pm_full.iloc[:, 0].astype(str).str.strip()))
            r_sel["ASIN"] = r_sel["Sku"].astype(str).str.strip().map(sku_to_asin)
            
            r_sel.columns = r_sel.columns.str.lower()
            pm_full.columns = pm_full.columns.str.lower()
            
            # PM lookup — col 0=asin, col 4=brand manager, col 6=brand, col 7=product name
            rs_pm_lookup = pm_full.iloc[:, [0, 4, 6, 7]].copy()
            rs_pm_lookup.columns = ["asin", "brand manager", "brand", "product name"]
            rs_pm_lookup = rs_pm_lookup.drop_duplicates(subset="asin")
            
            r_sel = r_sel.merge(rs_pm_lookup, on="asin", how="left")
            r_sel["total"] = pd.to_numeric(r_sel["total"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
            
            st.success(f"✅ Processed {len(r_sel)} Seller Reimbursement records.")
            
            ks1, ks2, ks3, ks4 = st.columns(4)
            ks1.metric("Total Reimbursement", format_currency(r_sel["total"].sum()))
            ks2.metric("Unique Orders", r_sel["order id"].nunique() if 'order id' in r_sel.columns else "N/A")
            ks3.metric("Brands", r_sel["brand"].nunique())
            ks4.metric("Total Records", len(r_sel))
            
            rs_tab1, rs_tab2 = st.tabs(["📊 Brand Pivot", "📋 Full Detail"])
            
            with rs_tab1:
                st.subheader("Brand-wise Seller Reimbursement")
                rs_piv = r_sel.groupby("brand")["total"].sum().reset_index()
                rs_piv.columns = ["Brand", "Seller Reimbursement"]
                
                rs_sum = pd.DataFrame({"Brand": ["Grand Total"], "Seller Reimbursement": [rs_piv["Seller Reimbursement"].sum()]})
                rs_piv_disp = pd.concat([rs_piv, rs_sum], ignore_index=True)
                
                st.dataframe(make_arrow_safe(rs_piv_disp), use_container_width=True)
                st.download_button("📥 Download Seller Reimb Pivot", rs_piv_disp.to_csv(index=False).encode(), "reimbursement_seller_pivot.csv")
                
                combined_results.append(rs_piv.rename(columns={"Seller Reimbursement": "Reimbursement Seller Flex (Safe T Claim)"}))
                
            with rs_tab2:
                st.subheader("Full Seller Reimbursement Detail")
                st.dataframe(make_arrow_safe(r_sel), use_container_width=True)
                st.download_button("📥 Download Full Seller Reimb", r_sel.to_csv(index=False).encode(), "reimbursement_seller_full.csv")
                
        except Exception as e:
            st.error(f"❌ Error processing Reimbursement Seller: {e}")
    else:
        st.warning("Please upload Reimbursement (Seller) CSV and PM file.")

# ==========================================
# TAB 21: AMAZON STORAGE
# ==========================================
with tabs[18]:
    st.header("🏭 Amazon Storage Analysis")
    if amazon_storage_file and pm_file:
        try:
            stor_df = pd.read_csv(amazon_storage_file)
            stor_df.columns = stor_df.columns.str.lower()
            
            pm_full = pm_df.copy()
            pm_full.columns = pm_full.columns.str.lower()
            
            # PM lookup — col 0=asin, col 4=brand manager, col 6=brand, col 7=product name
            s_pm_lookup = pm_full.iloc[:, [0, 4, 6, 7]].copy()
            s_pm_lookup.columns = ["asin", "brand manager", "brand", "product name"]
            s_pm_lookup = s_pm_lookup.drop_duplicates(subset="asin")
            
            stor_proc = stor_df.merge(s_pm_lookup, on="asin", how="left")
            stor_proc["estimated-monthly-storage-fee"] = pd.to_numeric(stor_proc["estimated-monthly-storage-fee"], errors="coerce")
            
            st.success(f"✅ Processed {len(stor_proc)} storage fee records.")
            
            s1, s2, s3, s4 = st.columns(4)
            s1.metric("Total Storage Fee", format_currency(stor_proc["estimated-monthly-storage-fee"].sum()))
            s2.metric("Unique ASINs", stor_proc["asin"].nunique())
            s3.metric("Brands", stor_proc["brand"].nunique())
            s4.metric("Avg Fee/ASIN", format_currency(stor_proc["estimated-monthly-storage-fee"].mean()))
            
            st_tab1, st_tab2 = st.tabs(["📊 Brand Pivot", "📋 Full Detail"])
            
            with st_tab1:
                st.subheader("Brand-wise Storage Fees")
                st_piv = stor_proc.groupby("brand")["estimated-monthly-storage-fee"].sum().reset_index()
                st_piv.columns = ["Brand", "Storage Fee"]
                
                st_sum = pd.DataFrame({"Brand": ["Grand Total"], "Storage Fee": [st_piv["Storage Fee"].sum()]})
                st_piv_disp = pd.concat([st_piv, st_sum], ignore_index=True)
                
                st.dataframe(make_arrow_safe(st_piv_disp), use_container_width=True)
                st.download_button("📥 Download Storage Pivot", st_piv_disp.to_csv(index=False).encode(), "storage_brand_pivot.csv")
                
                combined_results.append(st_piv.rename(columns={"Storage Fee": "Storage Charges"}))
                
            with st_tab2:
                st.subheader("Full Storage Detail")
                st.dataframe(make_arrow_safe(stor_proc), use_container_width=True)
                st.download_button("📥 Download Full Storage", stor_proc.to_csv(index=False).encode(), "storage_full_report.csv")
                
        except Exception as e:
            st.error(f"❌ Error processing Amazon Storage: {e}")
    else:
        st.warning("Please upload Storage Fees CSV and PM file.")

# ==========================================
# TAB 22: LOSS/DAMAGE FBA
# ==========================================
with tabs[19]:
    st.header("📉 Loss/Damage - FBA Analysis")
    if loss_damage_fba_file and pm_file:
        try:
            l_fba = pd.read_csv(loss_damage_fba_file)
            l_fba.columns = l_fba.columns.str.lower()
            
            # Filter non-sellable
            if "detailed-disposition" in l_fba.columns:
                l_fba = l_fba[l_fba["detailed-disposition"].str.upper() != "SELLABLE"].copy()
            
            pm_full = pm_df.copy()
            pm_full.columns = pm_full.columns.str.lower()
            
            # PM lookup — col 0=asin, col 4=brand manager, col 6=brand, col 9=cp
            lf_pm_lookup = pm_full.iloc[:, [0, 4, 6, 9]].copy()
            lf_pm_lookup.columns = ["asin", "brand manager", "brand", "cp"]
            lf_pm_lookup = lf_pm_lookup.drop_duplicates(subset="asin")
            
            l_fba = l_fba.merge(lf_pm_lookup, on="asin", how="left")
            l_fba["cp"] = pd.to_numeric(l_fba["cp"], errors="coerce")
            l_fba["CP as per QTY"] = l_fba["cp"] * l_fba["quantity"]
            
            st.success(f"✅ Processed {len(l_fba)} non-sellable FBA return records.")
            
            fl1, fl2, fl3, fl4 = st.columns(4)
            fl1.metric("Total CP Loss", format_currency(l_fba["CP as per QTY"].sum()))
            fl2.metric("Total Units", f"{l_fba['quantity'].sum():,.0f}")
            fl3.metric("Unique Orders", l_fba["order-id"].nunique())
            fl4.metric("Brands", l_fba["brand"].nunique())
            
            lf_tab1, lf_tab2 = st.tabs(["📊 Brand Pivot", "📋 Full Detail"])
            
            with lf_tab1:
                st.subheader("Brand-wise FBA Loss")
                lf_piv = l_fba.groupby("brand")["CP as per QTY"].sum().reset_index()
                lf_piv.columns = ["Brand", "FBA Loss"]
                
                lf_sum = pd.DataFrame({"Brand": ["Grand Total"], "FBA Loss": [lf_piv["FBA Loss"].sum()]})
                lf_piv_disp = pd.concat([lf_piv, lf_sum], ignore_index=True)
                
                st.dataframe(make_arrow_safe(lf_piv_disp), use_container_width=True)
                st.download_button("📥 Download FBA Loss Pivot", lf_piv_disp.to_csv(index=False).encode(), "loss_fba_pivot.csv")
                
                combined_results.append(lf_piv.rename(columns={"FBA Loss": "Loss in damages FBA"}))
                
            with lf_tab2:
                st.subheader("Full FBA Loss Detail")
                st.dataframe(make_arrow_safe(l_fba), use_container_width=True)
                st.download_button("📥 Download Full FBA Loss", l_fba.to_csv(index=False).encode(), "loss_fba_full.csv")
                
        except Exception as e:
            st.error(f"❌ Error processing Loss/Damage FBA: {e}")
    else:
        st.warning("Please upload FBA Returns CSV and PM file.")

# ==========================================
# TAB 23: LOSS/DAMAGE SELLER
# ==========================================
with tabs[20]:
    st.header("🏬 Loss/Damage - Seller Flex Analysis")
    if loss_damage_seller_file:
        try:
            xl = pd.ExcelFile(loss_damage_seller_file)
            sheet_names = xl.sheet_names
            default_sheet = "Amazon" if "Amazon" in sheet_names else sheet_names[0]
            sel_sheet = st.selectbox("Select Sheet", sheet_names, index=sheet_names.index(default_sheet), key="loss_sel_sheet")
            
            l_sel = pd.read_excel(loss_damage_seller_file, sheet_name=sel_sheet)
            l_sel["AS Per Qty"] = pd.to_numeric(l_sel["AS Per Qty"], errors="coerce")
            
            st.success(f"✅ Loaded sheet '{sel_sheet}' with {len(l_sel)} records.")
            
            sl1, sl2, sl3, sl4 = st.columns(4)
            sl1.metric("Total Loss", format_currency(l_sel["AS Per Qty"].sum()))
            sl2.metric("Total Units", f"{l_sel['Qty'].sum():,.0f}" if 'Qty' in l_sel.columns else "N/A")
            sl3.metric("Unique Orders", l_sel["Order ID"].nunique() if 'Order ID' in l_sel.columns else "N/A")
            sl4.metric("Brands", l_sel["Brand"].nunique() if 'Brand' in l_sel.columns else "N/A")
            
            ls_tab1, ls_tab2 = st.tabs(["📊 Brand Pivot", "📋 Full Detail"])
            
            with ls_tab1:
                st.subheader("Brand-wise Seller Loss")
                if "Brand" in l_sel.columns:
                    ls_piv = l_sel.groupby("Brand")["AS Per Qty"].sum().reset_index()
                    ls_piv.columns = ["Brand", "Seller Loss"]
                    
                    ls_sum = pd.DataFrame({"Brand": ["Grand Total"], "Seller Loss": [ls_piv["Seller Loss"].sum()]})
                    ls_piv_disp = pd.concat([ls_piv, ls_sum], ignore_index=True)
                    
                    st.dataframe(make_arrow_safe(ls_piv_disp), use_container_width=True)
                    st.download_button("📥 Download Seller Loss Pivot", ls_piv_disp.to_csv(index=False).encode(), "loss_seller_pivot.csv")
                    
                    combined_results.append(ls_piv.rename(columns={"Seller Loss": "Loss in damages Seller Flex"}))
                else:
                    st.warning("Column 'Brand' not found in selected sheet.")
                
            with ls_tab2:
                st.subheader("Full Seller Loss Detail")
                st.dataframe(make_arrow_safe(l_sel), use_container_width=True)
                st.download_button("📥 Download Full Seller Loss", l_sel.to_csv(index=False).encode(), "loss_seller_full.csv")
                
        except Exception as e:
            st.error(f"❌ Error processing Loss/Damage Seller: {e}")
    else:
        st.warning("Please upload Seller Flex Damage Excel.")



# ==========================================
# TAB 24: REVERSE LOGISTIC FBA
# ==========================================
with tabs[21]:
    st.header("📦 Reverse Logistic - FBA Analysis")
    if rev_fba_txn_file and rev_fba_ret_file and pm_file:
        try:
            with st.spinner("Processing FBA Reverse Logistics..."):
                # 1. Load Transaction CSV (skip 11 rows)
                txn_fba = pd.read_csv(rev_fba_txn_file, skiprows=11, thousands=",", low_memory=False)
                
                # 2. Orders Pivot
                orders_fba = txn_fba[(txn_fba["type"] == "Order") & (txn_fba["product sales"] != 0)].copy()
                orders_fba["order id"] = orders_fba["order id"].astype(str)
                orders_fba["Sku"] = orders_fba["Sku"].astype(str)
                orders_fba["Con"] = orders_fba["order id"] + orders_fba["Sku"]
                
                piv_orders = orders_fba.groupby("Con")["total"].sum().reset_index().rename(columns={"total": "Order Payment"})
                
                # 3. Refunds Pivot
                refunds_fba = txn_fba[(txn_fba["type"] == "Refund") & (txn_fba["product sales"] != 0)].copy()
                refunds_fba["order id"] = refunds_fba["order id"].astype(str)
                refunds_fba["Sku"] = refunds_fba["Sku"].astype(str)
                refunds_fba["Con"] = refunds_fba["order id"] + refunds_fba["Sku"]
                
                piv_refunds = refunds_fba.groupby("Con")["total"].sum().reset_index().rename(columns={"total": "Refund Payment"})
                
                # 4. Returns Report
                ret_fba = pd.read_csv(rev_fba_ret_file, low_memory=False)
                ret_fba["order-id"] = ret_fba["order-id"].astype(str)
                ret_fba["sku"] = ret_fba["sku"].astype(str)
                ret_fba["Con"] = ret_fba["order-id"] + ret_fba["sku"]
                
                # Merge payments
                ret_fba = ret_fba.merge(piv_orders, on="Con", how="left")
                ret_fba = ret_fba.merge(piv_refunds, on="Con", how="left")
                
                # Logic from standalone script
                ret_fba.loc[ret_fba["Order Payment"].isna(), ["Order Payment", "Refund Payment"]] = 0
                ret_fba.loc[ret_fba["Refund Payment"].isna(), ["Refund Payment", "Order Payment"]] = 0
                
                ret_fba["Order Payment"] = pd.to_numeric(ret_fba["Order Payment"], errors="coerce").fillna(0)
                ret_fba["Refund Payment"] = pd.to_numeric(ret_fba["Refund Payment"], errors="coerce").fillna(0)
                ret_fba["Reverse Logistic Charges"] = ret_fba["Order Payment"] + ret_fba["Refund Payment"]
                
                # Scale positive charges by 25%
                mask = ret_fba["Reverse Logistic Charges"] > 0
                ret_fba.loc[mask, "Reverse Logistic Charges"] *= 0.25
                
                # PM lookup for Brand
                pm_local = pm_df.copy()
                pm_local.columns = pm_local.columns.str.lower()
                pm_lookup = pm_local.iloc[:, [0, 4, 6]].copy() # asin, brand manager, brand
                pm_lookup.columns = ["asin", "brand manager", "brand"]
                pm_lookup = pm_lookup.drop_duplicates(subset="asin")
                
                ret_fba = ret_fba.merge(pm_lookup, on="asin", how="left")
                
                st.success(f"✅ Processed {len(ret_fba)} FBA return records.")
                
                # KPIs
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total RLC", format_currency(ret_fba["Reverse Logistic Charges"].sum()))
                c2.metric("Total Order Payment", format_currency(ret_fba["Order Payment"].sum()))
                c3.metric("Total Refund Payment", format_currency(ret_fba["Refund Payment"].sum()))
                c4.metric("Brands Affected", ret_fba["brand"].nunique())
                
                rf_tab1, rf_tab2 = st.tabs(["📊 Brand Pivot", "📋 Full Detail"])
                
                with rf_tab1:
                    st.subheader("Brand-wise Reverse Logistic Charges")
                    rf_piv = ret_fba.groupby("brand")["Reverse Logistic Charges"].sum().reset_index()
                    rf_piv.columns = ["Brand", "Reverse logistics FBA"]
                    
                    rf_sum = pd.DataFrame({"Brand": ["Grand Total"], "Reverse logistics FBA": [rf_piv["Reverse logistics FBA"].sum()]})
                    rf_piv_disp = pd.concat([rf_piv, rf_sum], ignore_index=True)
                    
                    st.dataframe(make_arrow_safe(rf_piv_disp), use_container_width=True)
                    st.download_button("📥 Download RLC Pivot", rf_piv_disp.to_csv(index=False).encode(), "rlc_fba_pivot.csv")
                    
                    combined_results.append(rf_piv)
                    
                with rf_tab2:
                    st.subheader("Full Reverse Logistic Detail")
                    st.dataframe(make_arrow_safe(ret_fba), use_container_width=True)
                    st.download_button("📥 Download Full RLC", ret_fba.to_csv(index=False).encode(), "rlc_fba_full.csv")
                    
        except Exception as e:
            st.error(f"❌ Error processing Reverse Logistic FBA: {e}")
            import traceback
            st.code(traceback.format_exc())
    else:
        st.warning("Please upload FBA Transaction CSV, FBA Returns CSV, and PM file.")

# ==========================================
# TAB 25: REVERSE LOGISTIC SELLER
# ==========================================
with tabs[22]:
    st.header("🏬 Reverse Logistic - Seller Flex Analysis")
    if rev_sel_txn_file and rev_sel_ret_file and pm_file and rev_sel_ws_file:
        try:
            with st.spinner("Processing Seller Reverse Logistics..."):
                # 1. Load Transaction CSV (skip 11)
                txn_sel = pd.read_csv(rev_sel_txn_file, skiprows=11, thousands=",", low_memory=False)
                
                # 2. Orders Pivot
                orders_sel = txn_sel[(txn_sel["type"] == "Order") & (txn_sel["product sales"] != 0)].copy()
                orders_sel["order id"] = orders_sel["order id"].astype(str)
                orders_sel["Sku"] = orders_sel["Sku"].astype(str)
                orders_sel["Con"] = orders_sel["order id"] + orders_sel["Sku"]
                piv_orders_s = orders_sel.groupby("Con")["total"].sum().reset_index().rename(columns={"total": "Order Payment"})
                
                # 3. Refunds Pivot
                refunds_sel = txn_sel[(txn_sel["type"] == "Refund") & (txn_sel["product sales"] != 0)].copy()
                refunds_sel["order id"] = refunds_sel["order id"].astype(str)
                refunds_sel["Sku"] = refunds_sel["Sku"].astype(str)
                refunds_sel["Con"] = refunds_sel["order id"] + refunds_sel["Sku"]
                piv_refunds_s = refunds_sel.groupby("Con")["total"].sum().reset_index().rename(columns={"total": "Refund Payment"})
                
                # 4. Returns Reconciliation
                ret_sel = pd.read_csv(rev_sel_ret_file, low_memory=False)
                ret_sel = ret_sel[["Customer Order ID", "Shipment ID", "mSKU", "Units", "ASIN"]].copy()
                ret_sel = ret_sel.drop_duplicates(subset=["Customer Order ID", "Shipment ID", "mSKU", "Units"]).reset_index(drop=True)
                ret_sel = ret_sel.rename(columns={"mSKU": "Sku", "Customer Order ID": "order-id", "ASIN": "asin"})
                ret_sel["order-id"] = ret_sel["order-id"].astype(str)
                ret_sel["Sku"] = ret_sel["Sku"].astype(str)
                ret_sel["Con"] = ret_sel["order-id"] + ret_sel["Sku"]
                
                # 6. Working Sheet - Return Type
                ws = pd.read_excel(rev_sel_ws_file, sheet_name="Bluk Return Upload Snaphire")
                ws_lookup = ws.iloc[:, [0, 6]].copy()
                ws_lookup.columns = ["order-id", "Return Type"]
                ws_lookup["order-id"] = ws_lookup["order-id"].astype(str)
                ws_lookup = ws_lookup.drop_duplicates(subset="order-id")
                
                ret_sel = ret_sel.merge(ws_lookup, on="order-id", how="left")
                
                # Merge payments
                ret_sel = ret_sel.merge(piv_orders_s, on="Con", how="left")
                ret_sel = ret_sel.merge(piv_refunds_s, on="Con", how="left")
                
                # Logic
                ret_sel.loc[ret_sel["Order Payment"].isna(), ["Order Payment", "Refund Payment"]] = 0
                ret_sel.loc[ret_sel["Refund Payment"].isna(), ["Refund Payment", "Order Payment"]] = 0
                
                ret_sel["Order Payment"] = pd.to_numeric(ret_sel["Order Payment"], errors="coerce").fillna(0)
                ret_sel["Refund Payment"] = pd.to_numeric(ret_sel["Refund Payment"], errors="coerce").fillna(0)
                ret_sel["Reverse Logistic Charges"] = ret_sel["Order Payment"] + ret_sel["Refund Payment"]
                
                # Scale positive charges by 25%
                mask_s = ret_sel["Reverse Logistic Charges"] > 0
                ret_sel.loc[mask_s, "Reverse Logistic Charges"] *= 0.25
                
                # PM lookup
                pm_local_s = pm_df.copy()
                pm_local_s.columns = pm_local_s.columns.str.lower()
                pm_lookup_s = pm_local_s.iloc[:, [0, 4, 6]].copy()
                pm_lookup_s.columns = ["asin", "brand manager", "brand"]
                pm_lookup_s = pm_lookup_s.drop_duplicates(subset="asin")
                
                ret_sel = ret_sel.merge(pm_lookup_s, on="asin", how="left")
                
                st.success(f"✅ Processed {len(ret_sel)} Seller Flex return records.")
                
                sc1, sc2, sc3, sc4 = st.columns(4)
                sc1.metric("Total RLC", format_currency(ret_sel["Reverse Logistic Charges"].sum()))
                sc2.metric("Total Order Payment", format_currency(ret_sel["Order Payment"].sum()))
                sc3.metric("Total Refund Payment", format_currency(ret_sel["Refund Payment"].sum()))
                sc4.metric("Brands Affected", ret_sel["brand"].nunique())
                
                rs_tab1, rs_tab2 = st.tabs(["📊 Brand Pivot", "📋 Full Detail"])
                
                with rs_tab1:
                    st.subheader("Brand-wise Reverse Logistic Charges (Seller)")
                    rs_piv = ret_sel.groupby("brand")["Reverse Logistic Charges"].sum().reset_index()
                    rs_piv.columns = ["Brand", "Reverse logistics Seller Flex Reverse"]
                    
                    rs_sum = pd.DataFrame({"Brand": ["Grand Total"], "Reverse logistics Seller Flex Reverse": [rs_piv["Reverse logistics Seller Flex Reverse"].sum()]})
                    rs_piv_disp = pd.concat([rs_piv, rs_sum], ignore_index=True)
                    
                    st.dataframe(make_arrow_safe(rs_piv_disp), use_container_width=True)
                    st.download_button("📥 Download Seller RLC Pivot", rs_piv_disp.to_csv(index=False).encode(), "rlc_seller_pivot.csv")
                    
                    combined_results.append(rs_piv)
                    
                with rs_tab2:
                    st.subheader("Full Seller Reverse Logistic Detail")
                    st.dataframe(make_arrow_safe(ret_sel), use_container_width=True)
                    st.download_button("📥 Download Full Seller RLC", ret_sel.to_csv(index=False).encode(), "rlc_seller_full.csv")
                    
        except Exception as e:
            st.error(f"❌ Error processing Reverse Logistic Seller: {e}")
            import traceback
            st.code(traceback.format_exc())
    else:
        st.warning("Please upload Seller Transaction CSV, QWTT Returns CSV, PM, and Working Sheet 2.")



# ==========================================
# TAB 26: NET SALE ANALYZER
# ==========================================
with tabs[23]:
    st.header("📊 Net Sale Analyzer")
    if net_sale_txn_file and pm_file and net_sale_refund_file:
        try:
            with st.spinner("Processing Net Sale data via optimized pipeline..."):
                csv_bytes    = net_sale_txn_file.getvalue()
                pm_bytes     = pm_file.getvalue()
                refund_bytes = net_sale_refund_file.getvalue()

                # Determine if PM is CSV or Excel
                is_pm_csv = pm_file.name.lower().endswith(".csv")

                netsale, netsale_refund, netsale_refund_nan, refunded, brand_pivot = run_net_pipeline(
                    csv_bytes, pm_bytes, refund_bytes, is_pm_csv=is_pm_csv
                )

                st.success(
                    f"✅ {len(netsale):,} orders | "
                    f"{len(netsale_refund):,} refund rows | "
                    f"{len(netsale_refund_nan):,} clean orders"
                )

                # ── KPI Cards ─────────────────────────────────────────────────────────────
                grand = brand_pivot[brand_pivot["brand"]=="Grand Total"].iloc[0]
                c1,c2,c3,c4,c5 = st.columns(5)
                c1.metric("Total Quantity",    f"{int(grand['quantity']):,}")
                c2.metric("Sales (Turn Over)", fmt_net(grand["Sales Amount (Turn Over)"]))
                c3.metric("Transferred Price", fmt_net(grand["total"]))
                c4.metric("CP as per Qty",     fmt_net(grand["cp as per qty"]))
                c5.metric("P&L",               fmt_net(grand["profit"]))
                st.divider()

                # STYLE THRESHOLD: only apply Styler when rows are small
                STYLE_ROW_LIMIT = 5_000

                def show_table_net(df, fmt_dict=None, profit_col=None, height=450, key_prefix=""):
                    """Display df — with styling only if small, raw dataframe if large."""
                    if len(df) <= STYLE_ROW_LIMIT and fmt_dict:
                        s = df.style.format(fmt_dict, na_rep="—")
                        if profit_col and profit_col in df.columns:
                            s = s.map(
                                lambda v: f"background-color: {'#d4edda' if v>=0 else '#f8d7da'}",
                                subset=[profit_col]
                            )
                        st.dataframe(s, use_container_width=True, height=height, hide_index=True)
                    else:
                        if len(df) > STYLE_ROW_LIMIT and fmt_dict:
                            st.caption(f"ℹ️ Styling skipped for speed ({len(df):,} rows).")
                        st.dataframe(df, use_container_width=True, height=height, hide_index=True)

                MONEY = "₹{:,.2f}"

                nt_tab1,nt_tab2,nt_tab3,nt_tab4,nt_tab5,nt_tab6 = st.tabs([
                    "📊 Brand Summary","🔍 Order Detail","↩️ Refund Orders",
                    "📋 netsale","🔄 netsale_refund","✅ netsale_refund_nan",
                ])

                # ── Tab 1: Brand Summary ──────────────────────────────────────────────────
                with nt_tab1:
                    st.subheader("Brand-wise Summary (excluding refunded orders)")
                    col_f1, col_f2 = st.columns([3,1])
                    with col_f1:
                        brands_list = brand_pivot[brand_pivot["brand"]!="Grand Total"]["brand"].tolist()
                        sel_brands  = st.multiselect("Filter by Brand", brands_list, default=brands_list, key="ns_filt_br")
                    with col_f2:
                        show_grand = st.checkbox("Show Grand Total", value=True, key="ns_show_grand")

                    disp = brand_pivot[
                        brand_pivot["brand"].isin(sel_brands) |
                        ((brand_pivot["brand"]=="Grand Total") & show_grand)
                    ].rename(columns={
                        "brand":"Brand","quantity":"Quantity","total":"Transferred Price",
                        "cp as per qty":"CP as per Qty","profit":"P&L"
                    })[["Brand","Quantity","Sales Amount (Turn Over)","Transferred Price","CP as per Qty","P&L"]]

                    show_table_net(disp,
                        fmt_dict={"Sales Amount (Turn Over)":MONEY,"Transferred Price":MONEY,
                                  "CP as per Qty":MONEY,"P&L":MONEY,"Quantity":"{:,}"},
                        profit_col="P&L", height=600)

                    st.download_button("⬇️ Download Brand Summary (Excel)",
                        data=to_excel_bytes_net(disp), file_name="brand_summary.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_brand_v2")
                    
                    # Also append to combined summary with standardized names
                    bp_for_combined = brand_pivot[brand_pivot["brand"] != "Grand Total"].copy()
                    bp_for_combined["Gross PnL Level 1"] = bp_for_combined["total"] - bp_for_combined["cp as per qty"]
                    combined_results.append(bp_for_combined.rename(columns={
                        "brand": "Brand",
                        "quantity": "Net Sales",
                        "Sales Amount (Turn Over)": "Turn Over",
                        "total": "Payout",
                        "cp as per qty": "Cost of goods sold",
                        "profit": "Net PnL"
                    }))

                # ── Tab 2: Order Detail ───────────────────────────────────────────────────
                with nt_tab2:
                    st.subheader("Order-level Detail (pivot, no refunds)")
                    col_s1,col_s2,col_s3 = st.columns(3)
                    with col_s1:
                        bf = st.multiselect("Brand",
                            options=sorted(netsale_refund_nan["brand"].dropna().unique()), key="det_brand_v2")
                    with col_s2:
                        sf = st.text_input("SKU contains",  key="det_sku_v2")
                    with col_s3:
                        af = st.text_input("ASIN contains", key="det_asin_v2")

                    det = netsale_refund_nan
                    if bf: det = det[det["brand"].isin(bf)]
                    if sf: det = det[det["sku"].str.contains(sf, case=False, na=False)]
                    if af: det = det[det["asin"].str.contains(af, case=False, na=False)]

                    # Identity + Requested Metrics
                    base_cols = ["sku", "order id", "asin", "brand", "quantity"]
                    
                    # Rename as per request
                    rename_map = {
                        "total": "Transferred Price- total",
                        "cp as per qty": "CP As Per Qty",
                        "profit": "P&L - profit"
                    }
                    det = det.rename(columns=rename_map)

                    ordered_metrics = [
                        "Sales Amount (Turn Over)",
                        "Amazon Total Deducation",
                        "Amazon Total Deducation %",
                        "Transferred Price- total",
                        "CP As Per Qty",
                        "P&L - profit"
                    ]
                    
                    final_cols = [c for c in base_cols if c in det.columns] + [c for c in ordered_metrics if c in det.columns]
                    det = det[final_cols]

                    show_table_net(det,
                        fmt_dict={
                            "Sales Amount (Turn Over)": MONEY,
                            "Amazon Total Deducation": MONEY,
                            "Amazon Total Deducation %": "{:.2f}%",
                            "Transferred Price- total": MONEY,
                            "CP As Per Qty": MONEY,
                            "P&L - profit": MONEY
                        },
                        profit_col="P&L - profit")
                    st.caption(f"{len(det):,} rows")
                    st.download_button("⬇️ Download Filtered Orders (Excel)",
                        data=to_excel_bytes_net(det), file_name="order_detail.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_order_v2")

                # ── Tab 3: Refund Orders ──────────────────────────────────────────────────
                with nt_tab3:
                    st.subheader("Orders with Refunds")
                    ref = refunded[["sku","order id","asin","brand","quantity","total","profit"]].copy()
                    show_table_net(ref, fmt_dict={"total":MONEY,"profit":MONEY}, profit_col="profit", height=400)
                    st.caption(f"{len(ref):,} refunded orders")
                    st.download_button("⬇️ Download Refund Orders (Excel)",
                        data=to_excel_bytes_net(ref), file_name="refund_orders.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_refund_v2")

                # ── Tab 4: netsale ────────────────────────────────────────────────────────
                with nt_tab4:
                    st.subheader("netsale — Enriched Order rows")
                    st.caption(f"{len(netsale):,} rows × {netsale.shape[1]} cols")
                    ns_q = st.text_input("Search SKU or Order ID", key="ns_q_v2")
                    ns_v = netsale
                    if ns_q:
                        ns_v = netsale[
                            netsale["sku"].str.contains(ns_q,case=False,na=False) |
                            netsale["order id"].astype(str).str.contains(ns_q,case=False,na=False)
                        ]
                    show_table_net(ns_v,
                        fmt_dict={"product sales":MONEY,"total sales tax liable(gst before adjusting tcs)":MONEY,
                                  "total":MONEY,"cp":MONEY,"cp as per qty":MONEY})
                    st.caption(f"Showing {len(ns_v):,} rows")
                    st.download_button("⬇️ Download netsale (Excel)",
                        data=to_excel_bytes_net(netsale), file_name="netsale_report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_ns_v2")

                # ── Tab 5: netsale_refund ─────────────────────────────────────────────────
                with nt_tab5:
                    st.subheader("netsale_refund — Refund-type rows from Refund CSV")
                    st.caption(f"{len(netsale_refund):,} rows × {netsale_refund.shape[1]} cols")
                    nr_q = st.text_input("Search SKU or Order ID", key="nr_q_v2")
                    nr_v = netsale_refund
                    if nr_q:
                        # Handle varied casing in raw CSV
                        sc = "sku" if "sku" in nr_v.columns else ("Sku" if "Sku" in nr_v.columns else nr_v.columns[0])
                        oc = "order id" if "order id" in nr_v.columns else ("Order Id" if "Order Id" in nr_v.columns else nr_v.columns[1])
                        nr_v = netsale_refund[
                            netsale_refund[sc].astype(str).str.contains(nr_q,case=False,na=False) |
                            netsale_refund[oc].astype(str).str.contains(nr_q,case=False,na=False)
                        ]
                    show_table_net(nr_v)
                    st.caption(f"Showing {len(nr_v):,} rows")
                    st.download_button("⬇️ Download netsale_refund (Excel)",
                        data=to_excel_bytes_net(netsale_refund), file_name="netsale_refund_report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_nr_v2")

                # ── Tab 6: netsale_refund_nan ─────────────────────────────────────────────
                with nt_tab6:
                    st.subheader("netsale_refund_nan — Pivot rows excluding refunded Order IDs")
                    st.caption(f"{len(netsale_refund_nan):,} rows × {netsale_refund_nan.shape[1]} cols")
                    col_n1,col_n2,col_n3 = st.columns(3)
                    with col_n1:
                        nb = st.multiselect("Brand",
                            options=sorted(netsale_refund_nan["brand"].dropna().unique()), key="nan_b_v2")
                    with col_n2:
                        ns2 = st.text_input("SKU contains",  key="nan_s_v2")
                    with col_n3:
                        na2 = st.text_input("ASIN contains", key="nan_a_v2")

                    nv = netsale_refund_nan
                    if nb:  nv = nv[nv["brand"].isin(nb)]
                    if ns2: nv = nv[nv["sku"].str.contains(ns2,case=False,na=False)]
                    if na2: nv = nv[nv["asin"].str.contains(na2,case=False,na=False)]

                    show_table_net(nv,
                        fmt_dict={"product sales":MONEY,"total sales tax liable(gst before adjusting tcs)":MONEY,
                                  "total":MONEY,"cp":MONEY,"cp as per qty":MONEY,
                                  "Sales Amount (Turn Over)":MONEY,"Amazon Total Deducation":MONEY,
                                  "Amazon Total Deducation %":"{:.2f}%","profit":MONEY},
                        profit_col="profit")
                    st.caption(f"Showing {len(nv):,} rows")
                    st.download_button("⬇️ Download netsale_refund_nan (Excel)",
                        data=to_excel_bytes_net(netsale_refund_nan), file_name="netsale_refund_nan_report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_nan_v2")
                    
        except Exception as e:
            st.error(f"❌ Error processing Net Sale Analyzer: {e}")
            import traceback
            st.code(traceback.format_exc())
    else:
        st.warning("Please upload Net Sale Transaction CSV (Orders), Refund CSV, and PM file.")


# ==========================================
# TAB 27: CURRENT DAMAGE
# ==========================================
with tabs[24]:
    st.header("🏥 Current Damage")
    st.markdown("Brand-wise cost summary based on Inventory Report and Product Attributes.")

    if inv_rep_file and prod_attr_file:
        try:
            with st.spinner("Processing Current Damage data..."):
                # Load data
                inventory_cd = pd.read_csv(inv_rep_file)
                product_cd   = pd.read_csv(prod_attr_file)

                with st.expander("📋 Raw Inventory — all rows", expanded=False):
                    st.write(f"Shape: **{inventory_cd.shape[0]:,} rows × {inventory_cd.shape[1]} columns**")
                    st.dataframe(inventory_cd, use_container_width=True)

                # Filter: keep only items with stock
                if "old_quantity" in inventory_cd.columns:
                    inventory_cd = inventory_cd[inventory_cd["old_quantity"] != 0].copy()
                else:
                    st.error("Inventory Report CSV must contain column **old_quantity**.")
                    st.stop()

                with st.expander("✅ Filtered Inventory — items with stock", expanded=False):
                    st.write(f"Shape after filter: **{inventory_cd.shape[0]:,} rows × {inventory_cd.shape[1]} columns**")
                    st.dataframe(inventory_cd, use_container_width=True)

                # Merge to bring in COST
                if "SKU" not in product_cd.columns or "COST" not in product_cd.columns:
                    st.error("Product Attributes CSV must contain columns **SKU** and **COST**.")
                else:
                    inventory_cd = inventory_cd.merge(
                        product_cd[["SKU", "COST"]], 
                        left_on="sku", right_on="SKU", 
                        how="left"
                    ).drop(columns="SKU").rename(columns={"COST": "CP"})

                    # Compute CP as Per Qty
                    inventory_cd["CP"] = pd.to_numeric(inventory_cd["CP"].astype(str).str.replace(",","",regex=False), errors="coerce")
                    inventory_cd["CP as Per Qty"] = inventory_cd["CP"].fillna(0) * inventory_cd["old_quantity"]
                    
                    # Standardize Brand
                    if "Brand" in inventory_cd.columns:
                        inventory_cd["Brand"] = inventory_cd["Brand"].astype(str).str.upper()
                    
                    with st.expander("🔗 Enriched Inventory — with Cost and CP as Per Qty", expanded=False):
                        st.write(f"Shape: **{inventory_cd.shape[0]:,} rows × {inventory_cd.shape[1]} columns**")
                        st.dataframe(inventory_cd, use_container_width=True)

                    # Pivot: brand-wise cost summary
                    cd_pivot = inventory_cd.groupby("Brand", dropna=False)["CP as Per Qty"].sum().reset_index()
                    grand_total_cd = cd_pivot["CP as Per Qty"].sum()
                    
                    # KPIs
                    k1, k2, k3, k4 = st.columns(4)
                    k1.metric("Total SKUs with Stock", f"{len(inventory_cd):,}")
                    k2.metric("Total Brands", f"{len(cd_pivot):,}")
                    k3.metric("Grand Total CP (₹)", f"₹ {grand_total_cd:,.2f}")
                    k4.metric("SKUs Missing CP", f"{inventory_cd['CP'].isna().sum():,}")

                    st.divider()

                    # Styled Pivot Table
                    pivot_display_cd = cd_pivot.copy()
                    
                    # Add Grand Total row for display
                    grand_row_cd = pd.DataFrame([{"Brand": "GRAND TOTAL", "CP as Per Qty": grand_total_cd}])
                    pivot_display_cd = pd.concat([pivot_display_cd, grand_row_cd], ignore_index=True)

                    def highlight_grand_cd_style(row):
                        if str(row["Brand"]).upper() == "GRAND TOTAL":
                            return ["background-color: #1f4e79; color: white; font-weight: bold"] * len(row)
                        return [""] * len(row)

                    st.subheader("📊 Brand-wise Cost Summary")
                    st.dataframe(
                        pivot_display_cd.style.format({"CP as Per Qty": "₹{:,.2f}"}).apply(highlight_grand_cd_style, axis=1),
                        use_container_width=True, height=500
                    )

                    # Bar Chart
                    st.subheader("📈 CP by Brand")
                    chart_data_cd = cd_pivot.set_index("Brand").sort_values("CP as Per Qty", ascending=False)
                    st.bar_chart(chart_data_cd)

                    # Download
                    st.download_button("⬇️ Download Current Damage Summary (CSV)",
                                       data=cd_pivot.to_csv(index=False),
                                       file_name="current_damage_summary.csv",
                                       mime="text/csv", key="cd_dl_btn")

                    # Append to combined summary
                    cd_for_combined = cd_pivot.copy()
                    combined_results.append(cd_for_combined.rename(columns={
                        "Brand": "Brand",
                        "CP as Per Qty": "Current damages"
                    }))

        except Exception as e:
            st.error(f"❌ Error processing Current Damage: {e}")
            import traceback
            st.code(traceback.format_exc())
    else:
        st.info("Please upload both Inventory Report and Product Attributes CSV files.")

# ==========================================

# FINAL COMBINED REPORT POPULATION

# ==========================================

with tabs[0]:
    if combined_results:
        # Normalize brands to Title Case for robust merging
        final_df = combined_results[0].copy()
        if "Brand" in final_df.columns:
            final_df["Brand"] = final_df["Brand"].astype(str).str.strip().str.title()
            
        for next_df in combined_results[1:]:
            if "Brand" in next_df.columns:
                next_df["Brand"] = next_df["Brand"].astype(str).str.strip().str.title()
            final_df = pd.merge(final_df, next_df, on="Brand", how="outer")
        
        final_df["Brand"] = final_df["Brand"].fillna("Unknown/Unmapped")
        
        # List of columns to ensure presence and order
        requested_cols = [
            "Net Sales", "Turn Over", "Payout", "Cost of goods sold", "Gross PnL Level 1",
            "Price Support", "Coupon Support", "NCEMI Support", "Freebies Support", 
            "Exchange support", "Advertising support", "Gross PnL level 2",
            "Reimbursement FBA", "Reimbursement Seller Flex (Safe T Claim)", "Total Reimbursement",
            "Reverse logistics FBA", "Reverse logistics Seller Flex Reverse", "Total Reverse",
            "Replacement charges", "Storage Charges", "Admin @1%", 
            "Gross PnL level 3", "Interest %", "Interest", "Loss in damages FBA", "Loss in damages Seller Flex", 
            "Loss in damages Total", "Damage Resolve %", "Actual Loss of Damage", "Net PnL", "Current Inventory", "Cost Of Interest Rate On Good", "Current damages", "Profit in %"
        ]
        
        # Align naming for Exchange Support (fix case)
        if "Exchange Support" in final_df.columns:
            final_df.rename(columns={"Exchange Support": "Exchange support"}, inplace=True)

        for col in requested_cols:
            if col not in final_df.columns:
                final_df[col] = 0.0
        
        final_df = final_df.fillna(0)
        
        # Calculations
        final_df["Total Reimbursement"] = final_df["Reimbursement FBA"] + final_df["Reimbursement Seller Flex (Safe T Claim)"]
        final_df["Total Reverse"] = final_df["Reverse logistics FBA"] + final_df["Reverse logistics Seller Flex Reverse"]
        
        final_df["Gross PnL level 2"] = (
            final_df["Gross PnL Level 1"] + 
            final_df["Price Support"] + 
            final_df["Coupon Support"] + 
            final_df["NCEMI Support"] + 
            final_df["Freebies Support"] + 
            final_df["Exchange support"] + 
            final_df["Advertising support"]
        )
        
        final_df["Gross PnL level 3"] = (
            final_df["Gross PnL level 2"] + 
            final_df["Total Reimbursement"] - 
            final_df["Total Reverse"] - 
            final_df["Replacement charges"] - 
            final_df["Storage Charges"]
        )
        
        final_df["Loss in damages Total"] = final_df["Loss in damages FBA"] + final_df["Loss in damages Seller Flex"]
        
        final_df["Damage Resolve %"] = 0.0
        mask_damage = final_df["Loss in damages Total"] != 0
        final_df.loc[mask_damage, "Damage Resolve %"] = (final_df.loc[mask_damage, "Total Reimbursement"] / final_df.loc[mask_damage, "Loss in damages Total"]) * 100
        
        final_df["Actual Loss of Damage"] = final_df["Loss in damages Total"] - (final_df["Loss in damages Total"] * final_df["Damage Resolve %"] / 100)
        
        # Overlay uploaded Interest & Damage Resolve file using vlookup logic (C3 matching Brand)
        if interest_damage_file:
            try:
                # Assuming Column 1 is Brand, Column 2 is Interest, Column 3 is Damage Resolve %
                id_df = pd.read_excel(interest_damage_file.getvalue(), usecols=[0, 1, 2])
                id_df.columns = ["Brand_lookup", "Interest_from_file", "Damage_from_file"]
                
                def parse_pct(v):
                    if pd.isna(v): return np.nan
                    if isinstance(v, str): return float(v.replace("%", "").replace(",", "").strip())
                    return float(v) * 100 if float(v) > 0 and float(v) <= 1.0 else float(v)
                
                def parse_num(v):
                    if pd.isna(v): return np.nan
                    if isinstance(v, str): return float(v.replace(",", "").strip())
                    return float(v)
                
                # In the file, Interest is actually a percentage.
                id_df["Interest_from_file"] = id_df["Interest_from_file"].apply(parse_pct)
                id_df["Damage_from_file"] = id_df["Damage_from_file"].apply(parse_pct)
                
                id_df["Brand_lookup"] = id_df["Brand_lookup"].astype(str).str.strip().str.lower()
                final_df["Brand_str"] = final_df["Brand"].astype(str).str.strip().str.lower()
                final_df = final_df.merge(id_df, left_on="Brand_str", right_on="Brand_lookup", how="left")
                
                # Update Interest % and Interest Rupee Value if available in the file
                # If "Interest" was 0 before, we will now calculate it: Interest (Rs) = Turn Over * Interest %
                mask_interest = final_df["Interest_from_file"].notna()
                if not mask_interest.empty and mask_interest.any():
                    final_df.loc[mask_interest, "Interest %"] = final_df.loc[mask_interest, "Interest_from_file"]
                    final_df.loc[mask_interest, "Interest"] = final_df.loc[mask_interest, "Turn Over"] * (final_df.loc[mask_interest, "Interest %"] / 100)
                
                # Update Damage Resolve % if available in the file
                mask_damage_file = final_df["Damage_from_file"].notna()
                if not mask_damage_file.empty and mask_damage_file.any():
                    final_df.loc[mask_damage_file, "Damage Resolve %"] = final_df.loc[mask_damage_file, "Damage_from_file"]
                    
                    # Recalculate Actual Loss of Damage based on the overridden Damage Resolve %
                    final_df.loc[mask_damage_file, "Actual Loss of Damage"] = (
                        final_df.loc[mask_damage_file, "Loss in damages Total"] - 
                        (final_df.loc[mask_damage_file, "Loss in damages Total"] * final_df.loc[mask_damage_file, "Damage Resolve %"] / 100)
                    )
                
                final_df = final_df.drop(columns=["Brand_lookup", "Interest_from_file", "Damage_from_file", "Brand_str"], errors="ignore")
            except Exception as e:
                st.error(f"Error reading Interest & Damage Resolve file for vlookup: {e}")
        
        # Ensure Interest % exists for ones not updated by file
        if "Interest %" not in final_df.columns:
            final_df["Interest %"] = 0.0
        final_df["Interest %"] = final_df["Interest %"].fillna(0)
        
        final_df["Net PnL"] = final_df["Gross PnL level 3"] - final_df["Interest"] - final_df["Loss in damages Total"]
        
        final_df["Cost Of Interest Rate On Good"] = final_df["Current Inventory"] * (final_df["Interest %"] / 100)
        
        # Profit %
        mask_sales = final_df["Net Sales"] != 0
        final_df.loc[mask_sales, "Profit in %"] = (final_df.loc[mask_sales, "Net PnL"] / final_df.loc[mask_sales, "Net Sales"]) * 100
        
        # Select and Reorder
        final_df = final_df[["Brand"] + requested_cols]
        
        # Sort by Net PnL or Brand? User didn't specify, but Gross PnL Level 2/3 is key.
        # Let's keep Brand sort or desc PnL.
        final_df = final_df.sort_values("Net PnL", ascending=False)
        
        # Show Metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Brands", len(final_df[final_df['Brand'] != "Unknown/Unmapped"]))
        m2.metric("Total Net PnL", format_currency(final_df["Net PnL"].sum()))
        m3.metric("Max Profit Brand", final_df.iloc[0]["Brand"] if not final_df.empty else "N/A")
        m4.metric("Avg Profit/Brand", format_currency(final_df["Net PnL"].mean()))
        
        st.markdown("---")
        
        # Add Summary Row
        summary_cols = requested_cols.copy()
        if "Profit in %" in summary_cols: summary_cols.remove("Profit in %")
        if "Damage Resolve %" in summary_cols: summary_cols.remove("Damage Resolve %")
        if "Interest %" in summary_cols: summary_cols.remove("Interest %")
        
        summary_row = final_df[summary_cols].sum().to_frame().T
        summary_row["Brand"] = "TOTAL"
        
        # Recalculate profit % for total row
        total_net_pnl = summary_row["Net PnL"].iloc[0]
        total_net_sales = summary_row["Net Sales"].iloc[0]
        summary_row["Profit in %"] = (total_net_pnl / total_net_sales * 100) if total_net_sales != 0 else 0
        
        total_reimb = summary_row["Total Reimbursement"].iloc[0]
        total_loss = summary_row["Loss in damages Total"].iloc[0]
        summary_row["Damage Resolve %"] = (total_reimb / total_loss * 100) if total_loss != 0 else 0
        
        # Recalculate Interest % for total row based on Total Interest / Total Turn Over
        total_interest = summary_row["Interest"].iloc[0]
        total_turnover = summary_row["Turn Over"].iloc[0]
        summary_row["Interest %"] = (total_interest / total_turnover * 100) if total_turnover != 0 else 0
        
        final_df = pd.concat([final_df, summary_row], ignore_index=True)
        
        # Display with dynamic coloring
        format_dict = {c: format_currency for c in requested_cols if c not in ["Profit in %", "Damage Resolve %", "Interest %"]}
        format_dict["Profit in %"] = "{:.2f}%"
        format_dict["Damage Resolve %"] = "{:.2f}%"
        format_dict["Interest %"] = "{:.2f}%"
        
        st.dataframe(
            final_df.style.format(format_dict)
            .background_gradient(subset=["Net PnL"], cmap="RdYlGn"),
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
        st.subheader("📊 Net PnL Distribution by Brand")
        chart_data = final_df[final_df["Brand"] != "TOTAL"].copy()
        st.bar_chart(chart_data.set_index("Brand")["Net PnL"])

    else:
        st.info("Upload files to see the combined brand-wise summary.")

# Footer
st.markdown("---")
st.caption(f"Amazon Support Unified App | Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}")
