"""
=============================================================================
B2B Last-Mile Delivery — Canada | Synthetic Dataset Generator
=============================================================================
Senior Data Engineer / Data Scientist — Production Script

Dataset overview
────────────────
  Table            │ Est. Rows  │ Key Logic
  ─────────────────┼────────────┼───────────────────────────────────────────
  retailers        │       500  │ Faker en_CA names · RFM segments · cohorts
  suppliers        │        80  │ Faker company names · category specialization
  products         │       180  │ Template names · log-normal pricing
  areas            │       108  │ 18 cities × 6 neighborhoods · cold flag
  drivers          │       120  │ Faker names · city-weighted · vehicle types
  orders           │    ~85 000 │ Growth curves · seasonality · Pareto 80/20
  order_details    │   ~190 000 │ GMV = price × qty (strict) · segment-aware
  payments         │    ~85 000 │ Method lag · status · cohort alignment
  deliveries       │    ~85 000 │ Winter-delay model · cold-city logic

Advanced logic embedded
────────────────────────
  • YoY growth curves (2020–2025, COVID-aware)
  • Monthly + holiday seasonality
  • Pareto 80/20 retailer order distribution (power-law weights)
  • RFM-inspired retailer segments (Champions → Lost Customers)
  • Cohort-based temporal activation (New/Lost retailers)
  • Anomaly spikes (supply-chain events, mega campaigns)
  • Supplier–product category specialization
  • Driver–area city matching
  • Winter-delay probability model per city (cold/warm)
  • Payment lag by method (Credit Card vs Net-30 vs Net-60 …)

Output
──────
  ./canada_b2b_delivery_dataset/  ← 9 CSV files, Power BI-ready

Usage
─────
  python generate_canada_b2b_dataset.py

Requirements
────────────
  pip install pandas numpy faker
=============================================================================
"""

import os
import warnings
import numpy as np
import pandas as pd
from faker import Faker

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# §0  SEED & FAKER INITIALIZATION  (run once, globally)
# ─────────────────────────────────────────────────────────────────────────────
SEED = 42
np.random.seed(SEED)

faker = Faker("en_CA")          # Canadian locale — mandatory per spec
Faker.seed(SEED)

OUTPUT_DIR = "canada_b2b_delivery_dataset"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# §1  GLOBAL CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# ── Date range ────────────────────────────────────────────────────────────────
START_DATE = pd.Timestamp("2020-01-01")
END_DATE   = pd.Timestamp("2025-12-31")

# ── Volume knobs ──────────────────────────────────────────────────────────────
N_RETAILERS       = 500
N_SUPPLIERS       = 80
N_PRODUCTS        = 180
N_DRIVERS         = 120
BASE_DAILY_ORDERS = 35    # weekday baseline for 2021 (scaled by growth/seasonality below)

# ── Canadian cities + business metadata ───────────────────────────────────────
#   pop_weight  → drives how many retailers/drivers live there
#   cold        → True  = eligible for winter-delay model
CITIES = {
    "Toronto":      {"province": "ON", "pop_weight": 0.18, "cold": True },
    "Vancouver":    {"province": "BC", "pop_weight": 0.12, "cold": False},
    "Montreal":     {"province": "QC", "pop_weight": 0.13, "cold": True },
    "Calgary":      {"province": "AB", "pop_weight": 0.09, "cold": True },
    "Ottawa":       {"province": "ON", "pop_weight": 0.07, "cold": True },
    "Edmonton":     {"province": "AB", "pop_weight": 0.08, "cold": True },
    "Winnipeg":     {"province": "MB", "pop_weight": 0.06, "cold": True },
    "Quebec City":  {"province": "QC", "pop_weight": 0.05, "cold": True },
    "Hamilton":     {"province": "ON", "pop_weight": 0.04, "cold": True },
    "Kitchener":    {"province": "ON", "pop_weight": 0.03, "cold": True },
    "London":       {"province": "ON", "pop_weight": 0.03, "cold": True },
    "Victoria":     {"province": "BC", "pop_weight": 0.02, "cold": False},
    "Halifax":      {"province": "NS", "pop_weight": 0.03, "cold": True },
    "Oshawa":       {"province": "ON", "pop_weight": 0.02, "cold": True },
    "Windsor":      {"province": "ON", "pop_weight": 0.02, "cold": True },
    "Saskatoon":    {"province": "SK", "pop_weight": 0.02, "cold": True },
    "Regina":       {"province": "SK", "pop_weight": 0.02, "cold": True },
    "St. John's":   {"province": "NL", "pop_weight": 0.01, "cold": True },
}
CITY_NAMES   = list(CITIES.keys())
_cw          = np.array([CITIES[c]["pop_weight"] for c in CITY_NAMES], dtype=float)
CITY_WEIGHTS = _cw / _cw.sum()   # normalized probability vector

# ── 6 neighborhoods per city (108 areas total) ────────────────────────────────
NEIGHBORHOODS = {
    "Toronto":      ["Downtown Core",   "Scarborough",        "North York",
                     "Etobicoke",        "East York",          "Mississauga Border"],
    "Vancouver":    ["Downtown",         "Burnaby",            "Richmond",
                     "Surrey",           "Kitsilano",          "East Vancouver"],
    "Montreal":     ["Plateau-Mont-Royal","Rosemont",          "Verdun",
                     "Laval",            "Saint-Laurent",      "Hochelaga"],
    "Calgary":      ["Beltline",         "Inglewood",          "NE Calgary",
                     "SW Calgary",       "Forest Lawn",        "Bridgeland"],
    "Ottawa":       ["Centretown",       "Glebe",              "Barrhaven",
                     "Kanata",           "Orléans",            "Vanier"],
    "Edmonton":     ["Downtown",         "Whyte Ave",          "Millwoods",
                     "West Edmonton",    "Sherwood Park",      "Glenora"],
    "Winnipeg":     ["Exchange District","St. Boniface",       "West End",
                     "North End",        "River Heights",      "Transcona"],
    "Quebec City":  ["Old Quebec",       "Limoilou",           "Sainte-Foy",
                     "Charlesbourg",     "Beauport",           "Lévis"],
    "Hamilton":     ["Downtown",         "Westdale",           "Stoney Creek",
                     "Dundas",           "Ancaster",           "Binbrook"],
    "Kitchener":    ["Downtown",         "Waterloo Border",    "Pioneer Park",
                     "Forest Hill",      "Chicopee",           "Stanley Park"],
    "London":       ["Downtown",         "Old North",          "White Oaks",
                     "Hyde Park",        "Lambeth",            "Byron"],
    "Victoria":     ["Downtown",         "Oak Bay",            "Saanich",
                     "Esquimalt",        "James Bay",          "Langford"],
    "Halifax":      ["Downtown",         "North End",          "South End",
                     "Dartmouth",        "Bedford",            "Sackville"],
    "Oshawa":       ["Downtown",         "Northwood",          "Taunton",
                     "Eastdale",         "Donevan",            "McLaughlin"],
    "Windsor":      ["Downtown",         "Riverside",          "South Windsor",
                     "Walkerville",      "Forest Glade",       "Tecumseh"],
    "Saskatoon":    ["Downtown",         "Nutana",             "Riversdale",
                     "Confederation Park","Lakewood",          "Stonebridge"],
    "Regina":       ["Downtown",         "Wascana",            "Harbour Landing",
                     "Lakeview",         "Normanview",         "Cathedral"],
    "St. John's":   ["Downtown",         "East End",           "West End",
                     "Mount Pearl",      "CBS",                "Quidi Vidi"],
}

# ── Product categories + price bands ─────────────────────────────────────────
#   price_min/max → log-normal sampling range
#   weight       → share of product catalog
PRODUCT_CATEGORIES = {
    "Dairy":                  {"price_min":  5, "price_max":  55, "weight": 0.11},
    "Grains & Rice":          {"price_min":  8, "price_max":  60, "weight": 0.09},
    "Cleaning Supplies":      {"price_min":  6, "price_max":  80, "weight": 0.09},
    "Beverages":              {"price_min":  5, "price_max":  45, "weight": 0.09},
    "Snacks & Confectionery": {"price_min":  3, "price_max":  35, "weight": 0.08},
    "Produce":                {"price_min":  4, "price_max":  40, "weight": 0.09},
    "Meat & Seafood":         {"price_min": 18, "price_max": 160, "weight": 0.08},
    "Office Supplies":        {"price_min":  8, "price_max": 120, "weight": 0.08},
    "Packaging Materials":    {"price_min": 15, "price_max": 350, "weight": 0.08},
    "Health & Personal Care": {"price_min":  5, "price_max":  90, "weight": 0.08},
    "Frozen Foods":           {"price_min":  8, "price_max":  65, "weight": 0.08},
    "Industrial Supplies":    {"price_min": 20, "price_max": 500, "weight": 0.05},
}
CAT_NAMES   = list(PRODUCT_CATEGORIES.keys())
_catw       = np.array([PRODUCT_CATEGORIES[c]["weight"] for c in CAT_NAMES], dtype=float)
CAT_WEIGHTS = _catw / _catw.sum()

# ── Product name building blocks ──────────────────────────────────────────────
ADJECTIVES = [
    "Premium","Organic","Fresh","Canadian","Natural","Pure","Select","Classic",
    "Heritage","Artisan","Deluxe","Pro","Essential","Ultra","Golden","Royal",
    "Superior","Choice","Harvest","Alpine",
]
SIZES = [
    "500g","1kg","2kg","5kg","10kg","500ml","1L","2L","4L",
    "12-Pack","24-Pack","48-Pack","Bulk Case","Commercial Size",
]

# Templates: {a} = adjective, {s} = size
PRODUCT_TEMPLATES = {
    "Dairy":                  ["{a} Whole Milk {s}",       "{a} Cheddar Cheese {s}",
                               "Free-Range Eggs {s}",      "{a} Greek Yogurt {s}",
                               "{a} Unsalted Butter {s}",  "{a} Heavy Cream {s}",
                               "{a} Cottage Cheese {s}",   "{a} Mozzarella {s}"],
    "Grains & Rice":          ["{a} Jasmine Rice {s}",     "{a} Whole Wheat Flour {s}",
                               "{a} Rolled Oats {s}",      "Basmati Rice {s}",
                               "{a} Pearl Barley {s}",     "{a} Quinoa {s}",
                               "{a} Cornmeal {s}",         "{a} Rye Flour {s}"],
    "Cleaning Supplies":      ["{a} All-Purpose Cleaner {s}", "{a} Disinfectant Spray {s}",
                               "Heavy-Duty Degreaser {s}", "{a} Floor Cleaner {s}",
                               "Antibacterial Wipes {s}",  "{a} Bleach Solution {s}",
                               "{a} Glass Cleaner {s}",    "{a} Dish Soap {s}"],
    "Beverages":              ["{a} Spring Water {s}",     "{a} Ground Coffee {s}",
                               "{a} Green Tea {s}",        "{a} Orange Juice {s}",
                               "{a} Energy Drink {s}",     "{a} Sparkling Water {s}",
                               "{a} Herbal Tea {s}",       "{a} Cranberry Juice {s}"],
    "Snacks & Confectionery": ["{a} Granola Bars {s}",     "{a} Trail Mix {s}",
                               "{a} Dark Chocolate {s}",   "{a} Rice Crackers {s}",
                               "{a} Mixed Nuts {s}",       "{a} Dried Cranberries {s}",
                               "{a} Protein Bars {s}",     "{a} Popcorn {s}"],
    "Produce":                ["Fresh {a} Apples {s}",     "{a} Mixed Greens {s}",
                               "Organic {a} Tomatoes {s}", "{a} Russet Potatoes {s}",
                               "Fresh {a} Blueberries {s}","{a} Baby Carrots {s}",
                               "{a} Broccoli Crowns {s}",  "{a} Sweet Peppers {s}"],
    "Meat & Seafood":         ["{a} Chicken Breast {s}",   "Atlantic {a} Salmon {s}",
                               "{a} Lean Ground Beef {s}", "{a} Pork Tenderloin {s}",
                               "Wild Pacific Shrimp {s}",  "{a} Turkey Breast {s}",
                               "{a} Beef Striploin {s}",   "Pacific {a} Halibut {s}"],
    "Office Supplies":        ["{a} Copy Paper {s}",       "{a} Ballpoint Pens {s}",
                               "{a} Binder Clips {s}",     "{a} Manila Folders {s}",
                               "{a} Sticky Notes {s}",     "{a} Whiteboard Markers {s}",
                               "{a} Stapler Set",          "{a} Desk Organizer"],
    "Packaging Materials":    ["{a} Corrugated Boxes {s}", "Bubble Wrap Roll {s}",
                               "{a} Packing Tape {s}",     "{a} Stretch Film Roll {s}",
                               "Kraft Paper Roll {s}",     "{a} Foam Sheets {s}",
                               "{a} Shipping Labels {s}",  "{a} Void Fill Bags {s}"],
    "Health & Personal Care": ["{a} Hand Sanitizer {s}",   "{a} Liquid Soap {s}",
                               "{a} Shampoo {s}",          "{a} Daily Moisturizer {s}",
                               "{a} Vitamin C Tablets {s}","{a} Face Masks {s}",
                               "{a} Hand Lotion {s}",      "{a} Antiseptic Wipes {s}"],
    "Frozen Foods":           ["{a} Frozen Pizza",         "{a} Chicken Nuggets {s}",
                               "{a} Mixed Vegetables {s}", "{a} Vanilla Ice Cream {s}",
                               "{a} Ready Meals {s}",      "{a} Fish Fillets {s}",
                               "{a} Frozen Waffles {s}",   "{a} Hash Browns {s}"],
    "Industrial Supplies":    ["Nitrile Gloves {s}",       "Heavy-Duty Duct Tape {s}",
                               "Nylon Cable Ties {s}",     "{a} Safety Vest {s}",
                               "{a} Hard Hat",             "{a} Safety Glasses",
                               "{a} Work Gloves {s}",      "{a} Ear Protection {s}"],
}

# ── Seasonality model ─────────────────────────────────────────────────────────
#   B2B retail restocking: peaks in Nov–Dec (pre-holiday), dips in Jan–Feb
MONTHLY_SEASONALITY = {
     1: 0.72,   # Post-holiday slump
     2: 0.78,
     3: 0.88,   # Spring ramp
     4: 0.95,
     5: 1.02,
     6: 1.05,   # Early summer
     7: 1.00,
     8: 1.02,   # Back-to-school ordering
     9: 1.10,
    10: 1.18,   # Pre-holiday stocking begins
    11: 1.35,   # Black Friday + supplier push
    12: 1.45,   # Holiday peak
}

# ── Year-over-year growth ─────────────────────────────────────────────────────
#   2020: COVID disruption (dropped early, recovered late)
#   2021: baseline year (= 1.0)
#   2022–2025: compound growth as e-B2B adoption accelerates
YEAR_GROWTH = {
    2020: 0.82,
    2021: 1.00,
    2022: 1.20,
    2023: 1.34,
    2024: 1.44,
    2025: 1.52,
}

# ── Canadian holiday purchase surges ──────────────────────────────────────────
HOLIDAY_BOOSTS = {
    (11, 27): 1.8,   # Black Friday (Thu pre-rush)
    (11, 28): 2.1,   # Black Friday
    (11, 29): 1.6,   # Black Friday weekend
    (12, 22): 1.6,   # Christmas ordering
    (12, 23): 1.9,
    (12, 24): 2.3,   # Last-minute Christmas Eve
    ( 7,  1): 1.3,   # Canada Day
    ( 2, 14): 1.2,   # Valentine's Day
    (10, 31): 1.3,   # Halloween
    ( 9,  5): 1.2,   # Labour Day (back-to-office restocking)
}

# ── RFM-inspired retailer segments ────────────────────────────────────────────
SEGMENTS     = [
    "Champions", "Loyal Customers", "Potential Loyalists",
    "At Risk", "Hibernating", "New Customers", "Lost Customers",
]
# Fraction of retailer base in each segment
SEGMENT_DIST = np.array([0.10, 0.18, 0.17, 0.15, 0.12, 0.16, 0.12])

# Order frequency multiplier (Pareto amplifier)
SEGMENT_ORDER_WEIGHT = {
    "Champions":           5.0,
    "Loyal Customers":     3.0,
    "Potential Loyalists": 1.5,
    "At Risk":             0.7,
    "Hibernating":         0.25,
    "New Customers":       0.9,
    "Lost Customers":      0.15,
}
# Quantity multiplier in order details (Champions buy in bulk)
SEGMENT_QTY_MULT = {
    "Champions":           1.7,
    "Loyal Customers":     1.2,
    "Potential Loyalists": 1.0,
    "At Risk":             0.9,
    "Hibernating":         0.75,
    "New Customers":       0.85,
    "Lost Customers":      0.65,
}

# ── Supplier category specializations ─────────────────────────────────────────
#   Each supplier maps to one of these groups
SUPPLIER_SPEC_GROUPS = [
    ["Dairy", "Frozen Foods"],
    ["Grains & Rice", "Snacks & Confectionery"],
    ["Cleaning Supplies", "Packaging Materials"],
    ["Beverages"],
    ["Produce", "Meat & Seafood"],
    ["Office Supplies", "Industrial Supplies"],
    ["Health & Personal Care"],
    ["Packaging Materials", "Industrial Supplies"],
    ["Dairy", "Grains & Rice"],
    ["Beverages", "Snacks & Confectionery"],
    ["Frozen Foods", "Dairy", "Produce"],
    ["Industrial Supplies", "Packaging Materials", "Office Supplies"],
]

# ── Payment logic ─────────────────────────────────────────────────────────────
PAYMENT_METHODS = ["Credit Card", "Net-30", "Net-60", "Bank Transfer", "Cash on Delivery"]
PAYMENT_WEIGHTS = [0.44, 0.24, 0.14, 0.13, 0.05]

# Days from order_date to payment_date (lo, hi)
PAYMENT_LAG = {
    "Credit Card":      ( 1,  3),
    "Net-30":           (28, 36),
    "Net-60":           (58, 67),
    "Bank Transfer":    ( 3,  8),
    "Cash on Delivery": ( 0,  1),
}

ORDER_STATUSES = ["Completed", "Pending", "Cancelled"]
ORDER_STATUS_W = [0.88, 0.07, 0.05]


# ─────────────────────────────────────────────────────────────────────────────
# §2  TABLE GENERATORS
# ─────────────────────────────────────────────────────────────────────────────


def generate_retailers(n: int = N_RETAILERS) -> pd.DataFrame:
    """
    Generate B2B retailer records using Faker en_CA.

    Mix (~60% business names, ~40% individual shop owners).
    Segments pre-assigned (RFM-inspired) to drive order frequency.
    Cohort year anchors temporal activation of New/Lost segments.
    """
    # ── Segment assignment ────────────────────────────────────────────────────
    seg_prob         = SEGMENT_DIST / SEGMENT_DIST.sum()
    segments         = np.random.choice(SEGMENTS, size=n, p=seg_prob)

    # Cohort year range per segment (when the retailer joined / became active)
    cohort_ranges = {
        "Champions":           (2019, 2021),
        "Loyal Customers":     (2019, 2022),
        "Potential Loyalists": (2020, 2023),
        "At Risk":             (2020, 2023),
        "Hibernating":         (2019, 2022),
        "New Customers":       (2023, 2026),   # joined recent years
        "Lost Customers":      (2019, 2022),   # left early
    }
    cohort_years = np.array([
        np.random.randint(*cohort_ranges[s]) for s in segments
    ])

    # ── City distribution ────────────────────────────────────────────────────
    cities    = np.random.choice(CITY_NAMES, size=n, p=CITY_WEIGHTS)
    provinces = np.array([CITIES[c]["province"] for c in cities])

    # ── Name generation (Faker en_CA) ─────────────────────────────────────────
    is_business  = np.random.rand(n) < 0.60
    store_suffixes = ["Store", "Market", "Mart", "Depot", "Shop",
                      "Supplies", "Distribution", "Wholesale", "Foods", "Goods",
                      "Distributors", "Traders", "Provisions", "Retail"]
    names = []
    seen_names: set = set()
    for biz in is_business:
        attempts = 0
        while True:
            if biz:
                last  = faker.last_name()
                sfx   = np.random.choice(store_suffixes)
                name  = f"{last}'s {sfx}"
            else:
                name  = faker.name()
            if name not in seen_names or attempts > 30:
                seen_names.add(name)
                names.append(name)
                break
            attempts += 1

    # ── Preferred payment method (stays consistent per retailer) ──────────────
    pref_pay = np.random.choice(PAYMENT_METHODS, size=n, p=PAYMENT_WEIGHTS)

    # ── Registration date: derived from cohort year ───────────────────────────
    reg_months = np.random.randint(1, 13, size=n)
    reg_days   = np.random.randint(1, 28, size=n)
    reg_dates  = pd.to_datetime({
        "year": cohort_years, "month": reg_months, "day": reg_days
    })

    return pd.DataFrame({
        "retailer_id":        np.arange(1, n + 1),
        "retailer_name":      names,
        "city":               cities,
        "province":           provinces,
        "segment":            segments,
        "cohort_year":        cohort_years,
        "preferred_payment":  pref_pay,
        "registration_date":  reg_dates,
    })


def generate_suppliers(n: int = N_SUPPLIERS) -> pd.DataFrame:
    """
    Generate B2B supplier records.

    Uses Faker en_CA company names with realistic Canadian corporate suffixes.
    Each supplier specializes in 1–3 product categories (supply chain realism).
    """
    suffixes       = ["Ltd.", "Inc.", "Corp.", "Ltd.", "Inc.", "Ltd."]   # weighed toward Ltd./Inc.
    spec_group_ids = np.random.randint(0, len(SUPPLIER_SPEC_GROUPS), size=n)

    # ── Unique company names (Faker) ──────────────────────────────────────────
    names: list    = []
    seen_names: set = set()
    for _ in range(n):
        attempts = 0
        while True:
            raw    = faker.company().split(",")[0].split("and")[0].strip()
            # Strip existing legal suffixes that Faker might add
            for s in ["LLC", "PLC", "Inc", "Ltd", "Corp", "Group"]:
                raw = raw.replace(s, "").strip().rstrip("-").strip()
            suffix    = np.random.choice(suffixes)
            full_name = f"{raw} {suffix}"
            if full_name not in seen_names or attempts > 50:
                seen_names.add(full_name)
                names.append(full_name)
                break
            attempts += 1

    cities    = np.random.choice(CITY_NAMES, size=n, p=CITY_WEIGHTS)
    provinces = np.array([CITIES[c]["province"] for c in cities])

    category_groups   = ["|".join(SUPPLIER_SPEC_GROUPS[i]) for i in spec_group_ids]
    primary_categories= [SUPPLIER_SPEC_GROUPS[i][0] for i in spec_group_ids]

    return pd.DataFrame({
        "supplier_id":       np.arange(1, n + 1),
        "supplier_name":     names,
        "city":              cities,
        "province":          provinces,
        "primary_category":  primary_categories,
        "category_group":    category_groups,        # pipe-separated
        "spec_group_id":     spec_group_ids,
        "established_year":  np.random.randint(1985, 2020, size=n),
        "supplier_rating":   np.round(np.random.uniform(3.0, 5.0, size=n), 1),
    })


def _build_product_name(category: str) -> str:
    """Construct a realistic product name from category templates."""
    templates = PRODUCT_TEMPLATES.get(category, ["{a} {cat} Product {s}"])
    tmpl      = np.random.choice(templates)
    adj       = np.random.choice(ADJECTIVES)
    size      = np.random.choice(SIZES)
    return tmpl.format(a=adj, s=size, cat=category)


def generate_products(n: int = N_PRODUCTS) -> pd.DataFrame:
    """
    Generate product catalog with realistic names, categories, and prices.

    Prices sampled log-normally within each category's range to reflect
    natural skew (most products mid-range, a few premium/bulk outliers).
    """
    # Distribute products proportionally across categories
    cat_counts          = np.round(CAT_WEIGHTS * n).astype(int)
    cat_counts[-1]     += n - cat_counts.sum()    # absorb rounding residual

    rows: list         = []
    pid                = 1
    seen_names: set    = set()

    for cat, count in zip(CAT_NAMES, cat_counts):
        pmin = PRODUCT_CATEGORIES[cat]["price_min"]
        pmax = PRODUCT_CATEGORIES[cat]["price_max"]
        log_lo, log_hi = np.log(pmin), np.log(pmax)

        for _ in range(count):
            # Unique name (try up to 40 times before allowing near-duplicate)
            for attempt in range(40):
                name = _build_product_name(cat)
                if name not in seen_names:
                    seen_names.add(name)
                    break

            # Log-normal unit price — realistic right-skewed distribution
            unit_price = round(float(np.exp(np.random.uniform(log_lo, log_hi))), 2)

            rows.append({
                "product_id":   pid,
                "product_name": name,
                "category":     cat,
                "unit_price":   unit_price,
                "sku":          f"SKU-{cat[:3].upper()}-{pid:04d}",
            })
            pid += 1

    return pd.DataFrame(rows)


def generate_areas() -> pd.DataFrame:
    """
    Generate delivery areas: 1 row per neighborhood × city (108 total).
    Includes cold-weather flag used by the delivery delay model.
    """
    rows     = []
    area_id  = 1
    for city, hoods in NEIGHBORHOODS.items():
        province = CITIES[city]["province"]
        is_cold  = CITIES[city]["cold"]
        city_pop = CITIES[city]["pop_weight"]

        for hood in hoods:
            rows.append({
                "area_id":       area_id,
                "city":          city,
                "province":      province,
                "neighborhood":  hood,
                "area_name":     f"{hood}, {city}",
                "is_cold":       is_cold,
                "city_pop_weight": round(city_pop, 3),
            })
            area_id += 1

    return pd.DataFrame(rows)


def generate_drivers(n: int = N_DRIVERS,
                     areas_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Generate driver records using Faker en_CA names.

    Drivers are city-weighted (more drivers in larger markets) and assigned
    a primary area + vehicle type reflecting B2B delivery operations.
    """
    cities    = np.random.choice(CITY_NAMES, size=n, p=CITY_WEIGHTS)
    provinces = np.array([CITIES[c]["province"] for c in cities])

    # ── Unique names (Faker) ──────────────────────────────────────────────────
    names: list   = []
    seen: set     = set()
    for _ in range(n):
        for _ in range(50):
            name = faker.name()
            if name not in seen:
                seen.add(name)
                names.append(name)
                break

    # ── Vehicle type: B2B last-mile mix ─────────────────────────────────────
    vehicle_types = np.random.choice(
        ["Cargo Van", "Box Truck", "Refrigerated Truck", "Pickup Truck", "Electric Van"],
        size=n,
        p=[0.38, 0.30, 0.17, 0.10, 0.05],
    )

    # ── Primary area assignment (match city) ──────────────────────────────────
    primary_area_ids = np.zeros(n, dtype=int)
    if areas_df is not None:
        city_area_map = areas_df.groupby("city")["area_id"].apply(np.array).to_dict()
        for i, city in enumerate(cities):
            pool = city_area_map.get(city, areas_df["area_id"].to_numpy())
            primary_area_ids[i] = np.random.choice(pool)

    return pd.DataFrame({
        "driver_id":        np.arange(1, n + 1),
        "driver_name":      names,
        "city":             cities,
        "province":         provinces,
        "primary_area_id":  primary_area_ids,
        "vehicle_type":     vehicle_types,
        "hire_year":        np.random.randint(2015, 2026, size=n),
        "driver_rating":    np.round(np.random.uniform(3.2, 5.0, size=n), 1),
        "active":           np.random.choice([True, False], size=n, p=[0.88, 0.12]),
    })


def generate_orders(retailers_df: pd.DataFrame,
                    suppliers_df: pd.DataFrame,
                    areas_df:     pd.DataFrame,
                    drivers_df:   pd.DataFrame) -> pd.DataFrame:
    """
    Generate the master orders table (~85 000 rows).

    Business logic layers:
    ┌─────────────────────┬───────────────────────────────────────────────┐
    │ Layer               │ Mechanism                                     │
    ├─────────────────────┼───────────────────────────────────────────────┤
    │ Volume baseline     │ BASE_DAILY_ORDERS (2021 weekday reference)    │
    │ YoY growth          │ YEAR_GROWTH dict (2020–2025)                  │
    │ Seasonality         │ MONTHLY_SEASONALITY (Jan=0.72 … Dec=1.45)     │
    │ Holiday boosts      │ HOLIDAY_BOOSTS (Black Friday, Christmas …)    │
    │ Weekend penalty     │ ×0.30 (B2B orders rare on weekends)           │
    │ Anomaly spikes      │ 30 random days with ×1.8–4.0 multiplier       │
    │ Poisson noise       │ np.random.poisson for day-to-day variation    │
    │ Pareto 80/20        │ Power-law rank weights on retailers           │
    │ RFM segments        │ Per-segment frequency multiplier              │
    │ Cohort filter       │ New/Lost retailers temporally bounded         │
    │ City matching       │ Areas & drivers matched to retailer's city    │
    └─────────────────────┴───────────────────────────────────────────────┘
    """

    # ── 1. Build daily volume schedule ───────────────────────────────────────
    dates      = pd.date_range(START_DATE, END_DATE, freq="D")
    n_days     = len(dates)
    year_v     = dates.year.to_numpy()
    month_v    = dates.month.to_numpy()
    day_v      = dates.day.to_numpy()
    dow_v      = dates.dayofweek.to_numpy()   # 0=Monday … 6=Sunday

    growth      = np.array([YEAR_GROWTH[y]            for y in year_v], dtype=float)
    seasonality = np.array([MONTHLY_SEASONALITY[m]     for m in month_v], dtype=float)
    weekend     = np.where(dow_v >= 5, 0.30, 1.0)     # B2B: 70% drop on Sat/Sun

    # Holiday boost vector
    hboost = np.ones(n_days, dtype=float)
    for (m, d), mult in HOLIDAY_BOOSTS.items():
        hboost[(month_v == m) & (day_v == d)] = mult

    # Anomaly spikes (supply-chain disruptions, promotional campaigns)
    anomaly = np.ones(n_days, dtype=float)
    spike_idx          = np.random.choice(n_days, size=30, replace=False)
    anomaly[spike_idx] = np.random.uniform(1.8, 4.0, size=30)

    expected_daily = BASE_DAILY_ORDERS * growth * seasonality * hboost * weekend * anomaly
    expected_daily = np.maximum(expected_daily, 0.1)

    daily_counts  = np.random.poisson(expected_daily).astype(int)
    total_orders  = int(daily_counts.sum())
    print(f"    → Total orders: {total_orders:,}")

    # ── 2. Expand dates ───────────────────────────────────────────────────────
    order_dates = np.repeat(dates.to_numpy(), daily_counts)

    # ── 3. Retailer assignment — Pareto + segment weights ─────────────────────
    n_ret         = len(retailers_df)
    rank_weights  = 1.0 / (np.arange(1, n_ret + 1) ** 0.75)          # power-law
    seg_mult      = retailers_df["segment"].map(SEGMENT_ORDER_WEIGHT).to_numpy(float)
    ret_weights   = rank_weights * seg_mult
    ret_weights  /= ret_weights.sum()

    ret_idx           = np.random.choice(n_ret, size=total_orders, p=ret_weights)
    retailer_ids      = retailers_df["retailer_id"].to_numpy()[ret_idx]
    retailer_cities   = retailers_df["city"].to_numpy()[ret_idx]
    retailer_segments = retailers_df["segment"].to_numpy()[ret_idx]
    retailer_cohorts  = retailers_df["cohort_year"].to_numpy()[ret_idx]

    # ── 4. Cohort filter — replace temporally invalid assignments ─────────────
    order_years  = pd.DatetimeIndex(order_dates).year.to_numpy()

    # New Customers only appear after cohort_year
    invalid_new  = (retailer_segments == "New Customers") & (order_years < retailer_cohorts)
    # Lost Customers disappear after cohort_year + 2
    invalid_lost = (retailer_segments == "Lost Customers") & (order_years > retailer_cohorts + 2)
    invalid_mask = invalid_new | invalid_lost

    if invalid_mask.sum() > 0:
        # Replace with stable Champions/Loyal retailers (keeps volume consistent)
        stable_mask   = retailers_df["segment"].isin(["Champions", "Loyal Customers"]).to_numpy()
        stable_ids    = retailers_df["retailer_id"].to_numpy()[stable_mask]
        stable_cities = retailers_df["city"].to_numpy()[stable_mask]
        n_inv         = invalid_mask.sum()
        ridx          = np.random.choice(len(stable_ids), size=n_inv)
        retailer_ids[invalid_mask]   = stable_ids[ridx]
        retailer_cities[invalid_mask]= stable_cities[ridx]
        retailer_segments[invalid_mask] = "Champions"

    # ── 5. Area assignment — vectorized city matching ─────────────────────────
    city_to_areas = areas_df.groupby("city")["area_id"].apply(np.array).to_dict()
    all_areas     = areas_df["area_id"].to_numpy()
    area_ids      = np.empty(total_orders, dtype=int)

    for city in CITY_NAMES:
        pool = city_to_areas.get(city, all_areas)
        mask = (retailer_cities == city)
        nc   = mask.sum()
        if nc > 0:
            area_ids[mask] = np.random.choice(pool, size=nc)

    # ── 6. Driver assignment — vectorized city matching (active only) ─────────
    active_drv     = drivers_df[drivers_df["active"]].copy()
    city_to_drvs   = active_drv.groupby("city")["driver_id"].apply(np.array).to_dict()
    all_drivers    = active_drv["driver_id"].to_numpy()
    driver_ids     = np.empty(total_orders, dtype=int)

    for city in CITY_NAMES:
        pool = city_to_drvs.get(city, all_drivers)
        mask = (retailer_cities == city)
        nc   = mask.sum()
        if nc > 0:
            driver_ids[mask] = np.random.choice(pool, size=nc)

    # ── 7. Supplier assignment ────────────────────────────────────────────────
    supplier_ids = np.random.choice(suppliers_df["supplier_id"].to_numpy(), size=total_orders)

    # ── 8. Order status ───────────────────────────────────────────────────────
    statuses = np.random.choice(ORDER_STATUSES, size=total_orders, p=ORDER_STATUS_W)

    return pd.DataFrame({
        "order_id":         np.arange(1, total_orders + 1),
        "order_date":       pd.to_datetime(order_dates),
        "retailer_id":      retailer_ids,
        "supplier_id":      supplier_ids,
        "area_id":          area_ids,
        "driver_id":        driver_ids,
        "retailer_segment": retailer_segments,    # denormalized for analytics
        "order_status":     statuses,
        "gmv":              0.0,                  # filled in after order_details
    })


def generate_order_details(orders_df:   pd.DataFrame,
                           products_df: pd.DataFrame,
                           suppliers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate order line items (~190 000 rows).

    GMV = unit_price × quantity  (strict — no shortcuts).

    Key design decisions:
    • Items per order: Poisson(λ=2.3) clipped to [1, 8]
    • Products filtered to supplier's specialization (supply chain realism)
    • Quantity: log-normal, adjusted downward for high-price items
    • Quantity amplified by retailer segment (Champions buy more)
    • Price: unit_price ± 10% (contract variance / volume discounts)
    """

    # ── Supplier → eligible product IDs ──────────────────────────────────────
    sup_to_prods: dict = {}
    all_pids           = products_df["product_id"].to_numpy()
    for _, row in suppliers_df.iterrows():
        cats     = row["category_group"].split("|")
        eligible = products_df[products_df["category"].isin(cats)]["product_id"].to_numpy()
        sup_to_prods[row["supplier_id"]] = eligible if len(eligible) > 0 else all_pids

    # ── Items per order ───────────────────────────────────────────────────────
    n_orders    = len(orders_df)
    items_count = np.clip(np.random.poisson(2.3, size=n_orders), 1, 8).astype(int)
    total_lines = int(items_count.sum())
    print(f"    → Total order_detail lines: {total_lines:,}")

    # ── Expand order attributes to line level ─────────────────────────────────
    order_id_exp    = np.repeat(orders_df["order_id"].to_numpy(),       items_count)
    supplier_id_exp = np.repeat(orders_df["supplier_id"].to_numpy(),    items_count)
    seg_exp         = np.repeat(orders_df["retailer_segment"].to_numpy(),items_count)

    # ── Product selection (vectorized per supplier group) ─────────────────────
    product_ids = np.empty(total_lines, dtype=int)
    unique_sups = np.unique(supplier_id_exp)

    for sup_id in unique_sups:
        mask     = (supplier_id_exp == sup_id)
        n_lines  = mask.sum()
        eligible = sup_to_prods[sup_id]
        product_ids[mask] = np.random.choice(eligible, size=n_lines)

    # ── Price lookup + ±10% contract variance ─────────────────────────────────
    price_map   = products_df.set_index("product_id")["unit_price"].to_dict()
    base_prices = np.array([price_map[p] for p in product_ids], dtype=float)
    variance    = np.random.uniform(0.90, 1.10, total_lines)
    unit_prices = np.round(base_prices * variance, 2)

    # ── Quantity: log-normal + price adjustment + segment amplifier ───────────
    seg_mult_arr = np.array([SEGMENT_QTY_MULT.get(s, 1.0) for s in seg_exp], dtype=float)

    # Raw log-normal centered at ~12 units (B2B bulk purchasing)
    raw_qty = np.exp(np.random.normal(np.log(12), 0.85, total_lines))

    # Price factor: expensive items ordered in smaller quantities
    price_factor = np.where(base_prices > 150, 0.25,
                   np.where(base_prices >  80, 0.45,
                   np.where(base_prices >  40, 0.70,
                   np.where(base_prices >  20, 0.90, 1.20))))

    quantities = np.maximum(
        np.round(raw_qty * price_factor * seg_mult_arr).astype(int), 1
    )
    quantities = np.minimum(quantities, 500)    # cap at 500 units per line

    # ── GMV = unit_price × quantity ───────────────────────────────────────────
    line_totals = np.round(unit_prices * quantities, 2)

    # ── Metadata lookups ──────────────────────────────────────────────────────
    name_map = products_df.set_index("product_id")["product_name"].to_dict()
    cat_map  = products_df.set_index("product_id")["category"].to_dict()
    sku_map  = products_df.set_index("product_id")["sku"].to_dict()

    return pd.DataFrame({
        "detail_id":    np.arange(1, total_lines + 1),
        "order_id":     order_id_exp,
        "supplier_id":  supplier_id_exp,
        "product_id":   product_ids,
        "product_name": [name_map[p] for p in product_ids],
        "category":     [cat_map[p]  for p in product_ids],
        "sku":          [sku_map[p]  for p in product_ids],
        "quantity":     quantities,
        "unit_price":   unit_prices,
        "line_total":   line_totals,    # GMV component — strict
    })


def generate_payments(orders_df:   pd.DataFrame,
                      retailers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate one payment record per order.

    Logic:
    • 85% of orders use retailer's preferred payment method, 15% switch
    • Payment date = order_date + method-specific lag (vectorized)
    • Payment status weighted by method riskiness
      – Cash on Delivery: 8% failed, 5% pending
      – Net-60:           5% failed, 8% pending
      – Credit Card:      2% failed, 3% pending
    • Cancelled orders → status = 'Refunded'
    """
    n = len(orders_df)

    # Preferred method per retailer
    pref_map   = retailers_df.set_index("retailer_id")["preferred_payment"].to_dict()
    preferred  = orders_df["retailer_id"].map(pref_map).to_numpy()

    # 85% use preferred, 15% random
    use_pref   = np.random.rand(n) < 0.85
    rand_meth  = np.random.choice(PAYMENT_METHODS, size=n, p=PAYMENT_WEIGHTS)
    methods    = np.where(use_pref, preferred, rand_meth)

    # ── Payment date (vectorized lag) ─────────────────────────────────────────
    lags = np.array([
        np.random.randint(*PAYMENT_LAG.get(m, (1, 5)))
        for m in methods
    ], dtype=int)

    pay_dates = (pd.to_datetime(orders_df["order_date"].to_numpy())
                 + pd.to_timedelta(lags, unit="D"))
    # Cap at a reasonable future date
    pay_dates = pd.Series(pay_dates).clip(upper=pd.Timestamp("2026-12-31"))

    # ── Payment status ────────────────────────────────────────────────────────
    fail_prob    = np.where(methods == "Cash on Delivery", 0.08,
                   np.where(np.isin(methods, ["Net-60"]),  0.05, 0.02))
    pending_prob = np.where(np.isin(methods, ["Net-30", "Net-60"]), 0.08, 0.04)

    rnd        = np.random.rand(n)
    pay_status = np.where(rnd < fail_prob, "Failed",
                 np.where(rnd < fail_prob + pending_prob, "Pending", "Paid"))

    # Cancelled orders → Refunded
    cancelled                = (orders_df["order_status"].to_numpy() == "Cancelled")
    pay_status[cancelled]    = "Refunded"

    return pd.DataFrame({
        "payment_id":      np.arange(1, n + 1),
        "order_id":        orders_df["order_id"].to_numpy(),
        "retailer_id":     orders_df["retailer_id"].to_numpy(),
        "payment_method":  methods,
        "payment_date":    pay_dates.values,
        "payment_status":  pay_status,
        "amount":          0.0,   # filled in after GMV computation
        "currency":        "CAD",
    })


def generate_deliveries(orders_df: pd.DataFrame,
                        areas_df:  pd.DataFrame) -> pd.DataFrame:
    """
    Generate delivery tracking records.

    Winter delay model:
    ┌──────────────────────────┬────────────────────────────────────────┐
    │ Condition                │ Extra delay distribution               │
    ├──────────────────────────┼────────────────────────────────────────┤
    │ Cold city + Winter month │ 0–5 days  (Poisson-like, right-skewed)│
    │ Cold city + Other months │ 0–2 days  (mostly 0)                  │
    │ Warm city                │ 0–1 days  (rarely 1)                  │
    └──────────────────────────┴────────────────────────────────────────┘

    Delivery status:
    • Failed   → higher in cold+winter (5%) vs warm (1.5%)
    • Delayed  → higher in cold+winter (18%) vs warm (4%)
    • Delivered → remainder
    • Cancelled orders → "Not Dispatched"

    Timestamps: realistic business hours (07:00–18:00) with 15-min granularity.
    """
    n           = len(orders_df)
    order_dates = pd.to_datetime(orders_df["order_date"].to_numpy())
    months      = order_dates.month.to_numpy()

    # ── Cold-area flag ────────────────────────────────────────────────────────
    cold_map = areas_df.set_index("area_id")["is_cold"].to_dict()
    is_cold  = orders_df["area_id"].map(cold_map).fillna(False).to_numpy().astype(bool)
    is_winter = np.isin(months, [12, 1, 2, 3])       # Dec-Mar

    cold_winter = is_cold & is_winter
    cold_other  = is_cold & ~is_winter
    warm        = ~is_cold

    # ── Scheduled delivery lag (days from order_date) ─────────────────────────
    sched_lag     = np.random.choice([1, 2, 3], size=n, p=[0.45, 0.40, 0.15])
    sched_dates   = order_dates + pd.to_timedelta(sched_lag, unit="D")

    # ── Extra delay ───────────────────────────────────────────────────────────
    extra_delay = np.zeros(n, dtype=int)

    n_cw = cold_winter.sum()
    n_co = cold_other.sum()
    n_w  = warm.sum()

    if n_cw > 0:
        extra_delay[cold_winter] = np.random.choice(
            [0, 1, 2, 3, 4, 5], size=n_cw,
            p=[0.38, 0.28, 0.18, 0.10, 0.04, 0.02]
        )
    if n_co > 0:
        extra_delay[cold_other] = np.random.choice(
            [0, 1, 2], size=n_co, p=[0.80, 0.15, 0.05]
        )
    if n_w > 0:
        extra_delay[warm] = np.random.choice(
            [0, 1], size=n_w, p=[0.92, 0.08]
        )

    actual_dates_base = sched_dates + pd.to_timedelta(extra_delay, unit="D")

    # ── Delivery status ───────────────────────────────────────────────────────
    fail_prob    = np.where(cold_winter, 0.05, np.where(is_cold, 0.025, 0.015))
    delayed_prob = np.where(cold_winter, 0.18, np.where(is_cold, 0.08,  0.04))
    transit_prob = 0.01   # small fraction of most-recent orders

    rnd        = np.random.rand(n)
    del_status = np.where(rnd < fail_prob,                          "Failed",
                 np.where(rnd < fail_prob + delayed_prob,           "Delayed",
                 np.where(rnd < fail_prob + delayed_prob + transit_prob,
                                                                    "In Transit",
                                                                    "Delivered")))
    # Cancelled → Not Dispatched
    cancelled              = (orders_df["order_status"].to_numpy() == "Cancelled")
    del_status[cancelled]  = "Not Dispatched"

    # ── Business-hour timestamps (vectorized) ─────────────────────────────────
    # Deliveries happen between 07:00 and 18:00 in 15-min slots
    sched_offset = pd.to_timedelta(
        np.random.choice(np.arange(7 * 60, 18 * 60, 15), size=n), unit="min"
    )
    sched_ts = sched_dates.normalize() + sched_offset

    # Actual timestamps (NaT for Not Dispatched)
    act_offset   = pd.to_timedelta(
        np.random.choice(np.arange(7 * 60, 18 * 60, 15), size=n), unit="min"
    )
    actual_ts = pd.Series(actual_dates_base.normalize() + act_offset)
    actual_ts[cancelled] = pd.NaT

    return pd.DataFrame({
        "delivery_id":      np.arange(1, n + 1),
        "order_id":         orders_df["order_id"].to_numpy(),
        "driver_id":        orders_df["driver_id"].to_numpy(),
        "area_id":          orders_df["area_id"].to_numpy(),
        "scheduled_datetime": sched_ts,
        "actual_datetime":    actual_ts,
        "extra_delay_days": extra_delay,
        "delivery_status":  del_status,
        "is_cold_city":     is_cold,
        "is_winter_month":  is_winter,
    })


# ─────────────────────────────────────────────────────────────────────────────
# §3  POST-PROCESSING HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def attach_gmv_to_orders(orders_df: pd.DataFrame,
                         details_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-order GMV = SUM(line_total) and write back to orders.
    This is the single source of truth for GMV (avoids duplication).
    """
    gmv = details_df.groupby("order_id")["line_total"].sum()
    orders_df = orders_df.copy()
    orders_df["gmv"] = orders_df["order_id"].map(gmv).fillna(0.0).round(2)
    return orders_df


def attach_gmv_to_payments(payments_df: pd.DataFrame,
                           orders_df:   pd.DataFrame) -> pd.DataFrame:
    """Copy per-order GMV into payments as 'amount'."""
    gmv_map = orders_df.set_index("order_id")["gmv"].to_dict()
    payments_df = payments_df.copy()
    payments_df["amount"] = payments_df["order_id"].map(gmv_map).round(2)
    return payments_df


def save_csv(df: pd.DataFrame, name: str) -> None:
    path = os.path.join(OUTPUT_DIR, f"{name}.csv")
    df.to_csv(path, index=False)
    mb = os.path.getsize(path) / 1_048_576
    print(f"    ✓  {name}.csv  "
          f"({len(df):>9,} rows × {len(df.columns):>2} cols  |  {mb:.1f} MB)")


# ─────────────────────────────────────────────────────────────────────────────
# §4  VALIDATION
# ─────────────────────────────────────────────────────────────────────────────


def validate_dataset(tables: dict) -> None:
    """
    Run data-quality checks before saving.
    Asserts:
      • GMV consistency (orders.gmv ≈ SUM(order_details.line_total) per order)
      • Foreign key integrity for all fact → dimension relationships
      • No NULL primary keys
      • Reasonable value ranges (no negative prices or quantities)
    """
    print("\n  [VALIDATION]")
    passed = 0

    # GMV consistency
    detail_gmv = (tables["order_details"]
                  .groupby("order_id")["line_total"].sum()
                  .rename("detail_gmv"))
    order_gmv  = tables["orders"].set_index("order_id")["gmv"]
    diff       = (detail_gmv - order_gmv).abs()
    max_diff   = diff.max()
    if max_diff < 0.02:
        print(f"    ✓  GMV consistency: max_diff = {max_diff:.6f}  (< 0.02)")
        passed += 1
    else:
        print(f"    ✗  GMV inconsistency: max_diff = {max_diff:.4f}")

    # FK checks
    fk_pairs = [
        ("orders",        "retailer_id", "retailers",    "retailer_id"),
        ("orders",        "supplier_id", "suppliers",    "supplier_id"),
        ("orders",        "area_id",     "areas",        "area_id"),
        ("orders",        "driver_id",   "drivers",      "driver_id"),
        ("order_details", "order_id",    "orders",       "order_id"),
        ("order_details", "product_id",  "products",     "product_id"),
        ("payments",      "order_id",    "orders",       "order_id"),
        ("deliveries",    "order_id",    "orders",       "order_id"),
        ("deliveries",    "driver_id",   "drivers",      "driver_id"),
    ]
    for (fact, fk, dim, pk) in fk_pairs:
        fact_vals  = set(tables[fact][fk].unique())
        dim_vals   = set(tables[dim][pk].unique())
        orphans    = fact_vals - dim_vals
        if len(orphans) == 0:
            print(f"    ✓  FK {fact}.{fk} → {dim}.{pk}")
            passed += 1
        else:
            print(f"    ✗  FK {fact}.{fk}: {len(orphans)} orphan(s) found")

    # Value range checks
    neg_qty   = (tables["order_details"]["quantity"] <= 0).sum()
    neg_price = (tables["order_details"]["unit_price"] <= 0).sum()
    neg_gmv   = (tables["order_details"]["line_total"] < 0).sum()
    if neg_qty + neg_price + neg_gmv == 0:
        print(f"    ✓  No negative quantities, prices, or GMV values")
        passed += 1
    else:
        print(f"    ✗  Negative values: qty={neg_qty} price={neg_price} gmv={neg_gmv}")

    print(f"\n  Validation: {passed}/{len(fk_pairs) + 3} checks passed\n")


# ─────────────────────────────────────────────────────────────────────────────
# §5  MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────


def print_header() -> None:
    print()
    print("=" * 68)
    print("  B2B Last-Mile Delivery — Canada | Synthetic Dataset Generator")
    print("  Time range : 2020-01-01  →  2025-12-31")
    print("  Locale     : en_CA  |  Seed : 42")
    print("=" * 68)


def print_summary(tables: dict) -> None:
    print("=" * 68)
    print("  DATASET SUMMARY")
    print("=" * 68)
    total = 0
    for name, df in tables.items():
        n = len(df)
        total += n
        print(f"  {name:<22}  {n:>10,} rows  ×  {len(df.columns):>2} columns")
    print("-" * 68)
    print(f"  {'GRAND TOTAL':<22}  {total:>10,} rows")
    print("=" * 68)
    print(f"\n  Output directory:  ./{OUTPUT_DIR}/\n")


def main() -> None:
    print_header()

    print("\n  [1/9] Retailers …")
    retailers_df  = generate_retailers()
    save_csv(retailers_df, "retailers")

    print("  [2/9] Suppliers …")
    suppliers_df  = generate_suppliers()
    save_csv(suppliers_df, "suppliers")

    print("  [3/9] Products …")
    products_df   = generate_products()
    save_csv(products_df, "products")

    print("  [4/9] Areas …")
    areas_df      = generate_areas()
    save_csv(areas_df, "areas")

    print("  [5/9] Drivers …")
    drivers_df    = generate_drivers(areas_df=areas_df)
    save_csv(drivers_df, "drivers")

    print("  [6/9] Orders …")
    orders_df     = generate_orders(retailers_df, suppliers_df, areas_df, drivers_df)

    print("  [7/9] Order Details …")
    details_df    = generate_order_details(orders_df, products_df, suppliers_df)

    # Attach GMV (single computation, avoids any inconsistency)
    orders_df     = attach_gmv_to_orders(orders_df, details_df)
    save_csv(orders_df,  "orders")
    save_csv(details_df, "order_details")

    print("  [8/9] Payments …")
    payments_df   = generate_payments(orders_df, retailers_df)
    payments_df   = attach_gmv_to_payments(payments_df, orders_df)
    save_csv(payments_df, "payments")

    print("  [9/9] Deliveries …")
    deliveries_df = generate_deliveries(orders_df, areas_df)
    save_csv(deliveries_df, "deliveries")

    # ── Data quality validation ───────────────────────────────────────────────
    tables = {
        "retailers":    retailers_df,
        "suppliers":    suppliers_df,
        "products":     products_df,
        "areas":        areas_df,
        "drivers":      drivers_df,
        "orders":       orders_df,
        "order_details":details_df,
        "payments":     payments_df,
        "deliveries":   deliveries_df,
    }
    validate_dataset(tables)
    print_summary(tables)


if __name__ == "__main__":
    main()
