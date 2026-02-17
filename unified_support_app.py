import streamlit as st
import pandas as pd
import io
import re
import zipfile
import pypdf
import pdfplumber
from datetime import datetime

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
# SIDEBAR - GLOBAL UPLOADS
# ==========================================
st.sidebar.title("📤 Data Upload Center")

st.sidebar.subheader("💎 Essential Master Data")
pm_file = st.sidebar.file_uploader("Product Master (PM)", type=["xlsx", "xls"], key="pm_global")
portfolio_file = st.sidebar.file_uploader("Portfolio Report (Ads Mapping)", type=["xlsx", "xls"], key="portfolio_global")

st.sidebar.markdown("---")

st.sidebar.subheader("📊 Support Report Files")
coupon_file = st.sidebar.file_uploader("Coupon Orders (TXT)", type=["txt"], key="coupon_up")
exchange_file = st.sidebar.file_uploader("Exchange Data (Excel)", type=["xlsx", "xls"], key="exchange_up")
freebies_file = st.sidebar.file_uploader("Freebies Orders (TXT)", type=["txt"], key="freebies_up")
ncemi_payment_file = st.sidebar.file_uploader("NCEMI Payment (CSV)", type=["csv"], key="ncemi_pay_up")
ncemi_support_files = st.sidebar.file_uploader("NCEMI B2B/B2C Files", type=["csv", "zip"], accept_multiple_files=True, key="ncemi_sup_up")
adv_files = st.sidebar.file_uploader("Advertisement Invoices (PDF)", type=["pdf"], accept_multiple_files=True, key="adv_up")

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

if not (pm_file or coupon_file or exchange_file or freebies_file or ncemi_payment_file or adv_files):
    st.info("👋 Welcome! Please upload your data files in the sidebar to generate reports.")
    st.markdown("""
    ### 📂 Expected Files:
    - **Product Master (PM)**: Excel with `ASIN` and `Brand` columns.
    - **Coupon/Freebies**: Tab-separated TXT order reports.
    - **Exchange**: Excel with `brand` and `seller funding`.
    - **NCEMI**: Payment CSV + B2B/B2C order reports for SKU mapping.
    - **Advertisement**: PDF Invoices and Portfolio Excel for campaign mapping.
    """)
    st.stop()

tabs = st.tabs(["🏠 Combined Summary", "🏷️ Coupon", "🔄 Exchange", "🎁 Freebies", "💳 NCEMI", "📢 Advertisement"])

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
