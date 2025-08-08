import pandas as pd
import numpy as np
import unicodedata, re, difflib
from sqlalchemy import create_engine

def clean_and_engineer(filepath):
    try:
        if filepath.lower().endswith(".csv"):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
    except Exception as e:
        raise ValueError(f"Failed to load file: {e}")
    try:
        df.columns = df.columns.astype(str).str.strip()
        first_row_str = " ".join(df.iloc[0].astype(str).tolist()).lower()
        if any(x in first_row_str for x in ["pp gads", "gads", "pp mēnesis", "pilsēta"]):
            df = df.iloc[1:].reset_index(drop=True)
            df.columns = df.columns.astype(str).str.strip()
    except Exception as e:
        raise ValueError(f"Failed during initial cleaning: {e}")
    def _norm(s: str) -> str:
        s = unicodedata.normalize("NFKD", str(s))
        s = "".join(c for c in s if not unicodedata.combining(c))  
        s = s.lower()
        s = re.sub(r"[^a-z0-9]+", "", s)  
        return s

    canonical_map = {
        "pp gads": "Report_Year",
        "gads": "Report_Year",

        "pp mēnesis": "Report_Month",
        "mēnesis": "Report_Month",

        "pilsēta, novads": "City_Municipality",
        "pilsēta, novads, pagasts": "City_Municipality",

        "atvk kods": "Administrative_Code_ATVK",

        "oblig. kopā, skaits": "Total_Insured_Persons_Employees_Self_Employed",
        "oblig. kopā, alga": "Average_Insurable_Salary_Total",

        "oblig. siev., skaits": "Insured_Women_Count",
        "oblig. siev., alga": "Insured_Women_Average_Salary",

        "oblig. vīr., skaits": "Insured_Men_Count",
        "oblig. vīr., alga": "Insured_Men_Average_Salary",

        "darba ņēm. kopā, skaits": "Employees_Count",
        "darba ņēm. kopā, alga": "Employees_Average_Salary",

        "darba ņēm. siev., skaits": "Female_Employees_Count",
        "darba ņēm. siev., alga": "Female_Employees_Average_Salary",

        "darba ņēm. vīr., skaits": "Male_Employees_Count",
        "darba ņēm. vīr., alga": "Male_Employees_Average_Salary",

        "pašnodarb. kopā, skaits": "Self_Employed_Count",
        "pašnodarb. kopā, alga": "Self_Employed_Average_Salary",

        "pašnodarb. siev., skaits": "Female_Self_Employed_Count",
        "pašnodarb. siev., alga": "Female_Self_Employed_Average_Salary",

        "pašnodarb. vīr., skaits": "Male_Self_Employed_Count",
        "pašnodarb. vīr., alga": "Male_Self_Employed_Average_Salary",

        "algu līmenis": "Region_Salary_Level",
    }
    norm_key_to_target = {_norm(k): v for k, v in canonical_map.items()}

    def build_fuzzy_rename_map(cols, threshold=0.80):
        rename = {}
        norm_keys = list(norm_key_to_target.keys())
        for c in cols:
            n = _norm(c)
            if n in norm_key_to_target:
                rename[c] = norm_key_to_target[n]
                continue
            best_key, best_ratio = None, 0.0
            for k in norm_keys:
                r = difflib.SequenceMatcher(None, n, k).ratio()
                if r > best_ratio:
                    best_ratio, best_key = r, k
            if best_key is not None and best_ratio >= threshold:
                rename[c] = norm_key_to_target[best_key]
        return rename

    try:
        rename_map = build_fuzzy_rename_map(df.columns)
        df.rename(columns=rename_map, inplace=True)
    except Exception as e:
        raise ValueError(f"Failed during column rename: {e}")

    if "Report_Month" in df.columns:
        df.drop(columns=["Report_Month"], inplace=True)

    for colname in df.columns:
        try:
            if colname == "Administrative_Code_ATVK":
                df[colname] = (
                    df[colname].astype(str)
                    .str.replace(r"\.0$", "", regex=True)
                    .str.zfill(7)
                )
            elif colname not in ["Report_Year", "City_Municipality", "Region_Salary_Level"]:
                df[colname] = pd.to_numeric(df[colname], errors="coerce")
        except Exception as col_err:
            raise ValueError(f"Failed converting column '{colname}': {col_err}")

    def classify_salary_level(avg_salary):
        if pd.isna(avg_salary):
            return "Unknown"
        elif avg_salary < 1000:
            return "Low"
        elif avg_salary < 1400:
            return "Medium"
        else:
            return "High"

    if "Region_Salary_Level" not in df.columns:
        salary_source = None
        if "Employees_Average_Salary" in df.columns:
            salary_source = "Employees_Average_Salary"
        elif "Average_Insurable_Salary_Total" in df.columns:
            salary_source = "Average_Insurable_Salary_Total"

        if salary_source is not None:
            df["Region_Salary_Level"] = df[salary_source].apply(classify_salary_level)
        else:
            df["Region_Salary_Level"] = "Unknown"
            raise ValueError(
                "Could not find a salary column to derive Region_Salary_Level. "
                "Expected one of: Employees_Average_Salary or Average_Insurable_Salary_Total."
            )

    if {"Male_Employees_Average_Salary", "Female_Employees_Average_Salary"}.issubset(df.columns):
        df["Wage_Gap_Male_Female"] = (
            df["Male_Employees_Average_Salary"] - df["Female_Employees_Average_Salary"]
        )
    else:
        df["Wage_Gap_Male_Female"] = np.nan

    if {"Male_Employees_Count", "Female_Employees_Count"}.issubset(df.columns):
        denom = df["Female_Employees_Count"].replace({0: np.nan})
        df["Male_Female_Employee_Ratio"] = df["Male_Employees_Count"] / denom
    else:
        df["Male_Female_Employee_Ratio"] = np.nan
    df.drop_duplicates(inplace=True)

    return df

def append_to_database(df, server, database, table_name):
    """
    Appends a cleaned DataFrame to SQL Server via Windows auth.
    """
    driver = "ODBC+Driver+17+for+SQL+Server"
    conn_str = f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes"
    engine = create_engine(conn_str, fast_executemany=True)

    expected_order = [
        "Report_Year", "City_Municipality", "Administrative_Code_ATVK",
        "Total_Insured_Persons_Employees_Self_Employed", "Average_Insurable_Salary_Total",
        "Insured_Women_Count", "Insured_Women_Average_Salary",
        "Insured_Men_Count", "Insured_Men_Average_Salary",
        "Employees_Count", "Employees_Average_Salary",
        "Female_Employees_Count", "Female_Employees_Average_Salary",
        "Male_Employees_Count", "Male_Employees_Average_Salary",
        "Self_Employed_Count", "Self_Employed_Average_Salary",
        "Female_Self_Employed_Count", "Female_Self_Employed_Average_Salary",
        "Male_Self_Employed_Count", "Male_Self_Employed_Average_Salary",
        "Region_Salary_Level", "Wage_Gap_Male_Female", "Male_Female_Employee_Ratio",
    ]
    missing = [c for c in expected_order if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns for insert: {missing}")
    df = df[expected_order]

    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    return True
