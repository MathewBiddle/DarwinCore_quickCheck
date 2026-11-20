"""
0. Loads three CSV files (`event_bd.csv`, `occurrence_bd.csv`, and `emof_bd.csv`) into pandas
DataFrames.
1. Checks for the presence of required columns;
2. Verifies data completeness by looking for null values in critical fields;
3. Validates the geographic coordinates (latitude and longitude) to ensure they are within valid
   ranges;
4. Checks for depth information, making sure that `minimumDepthInMeters` is not greater than
   `maximumDepthInMeters` and that depth values are numeric;
5. Checks with the World Register of Marine Species (WoRMS) API to validate scientific names,
   identifying any unaccepted or unfound taxa.
6. Then attempts to merge these DataFrames.
   a. First, `df_event` is merged with `df_occurrence` on the `eventID` column, with a validation
      to ensure it's a one-to-many relationship. The event file should have a unique list of
      `eventID` which match to `eventID` in the occurrence file.
   b. Next, the resulting DataFrame (`df_event_occur`) is merged with df_emof on occurrenceID,
      again with a one-to-many validation. The event and occurrence data files should have unique
      `occurrenceID` values which map to the `occurrenceID` in the emof file.
7. Finally, it checks if the number of rows in the final merged DataFrame (df_event_occur_emof)
   is equal to the number of rows in the df_emof DataFrame, indicating a successful arrangement
   of event files. Since we are merging all of the data together, there should be the same number
   of rows in the final dataset as there are in the emof file, but with more columns from the
   event and occurrence files.

"""

##-----------------------------------##
##             Libraries             ##
##-----------------------------------##
import functools
from js import document
from io import StringIO
import stamina

import pandas as pd
import requests
from pathlib import Path

import numpy as np

##-----------------------------------##
##          Create Functions         ##
##-----------------------------------##

# -------- load_csv --------
async def load_csv(event):
    global dfs

    output = document.querySelector("#output")
    status = document.querySelector("#status")

    output.innerHTML = ""

    files = [
        ("Event", "event_bd"),
        ("Occurrence", "occurrence_bd"),
        ("EMOF", "emof_bd")
    ]

    loaded = 0
    dfs = {}
    for name, file_id in files:
        file_input = document.querySelector(f"#{file_id}")
        if len(file_input.files) == 0:
            continue

        file = file_input.files.item(0)
        try:
            content = await file.text()

            # the browser swaps bad characters for "�" instead of raising an error
            if "\ufffd" in content:
                output.innerHTML += f"<p>❌ Could not read {name}: encoding error. Re-save the file as CSV UTF-8.</p>"
                continue

            df = pd.read_csv(StringIO(content), dtype=str, keep_default_na=False, na_values=[""])
            k = name.lower()
            dfs.update({k: df})
            loaded += 1

            output.innerHTML += f"""
                <hr class="section-divider" />
                <div class="dataset">
                    <h3>{name}</h3>
                    <p>
                        Rows: {df.shape[0]} |
                        Columns: {df.shape[1]}
                    </p>
                </div>
            """

            html_table = df.describe(include='all').T.to_html(
                index=True,
                border=0
            )

            output.innerHTML += html_table

            print(f"\n{name}")
            print(df.info())

        except pd.errors.ParserError:
            output.innerHTML += (
                f"<p>❌ Could not read {name}: some rows have more values than there are columns. "
                "Check for commas inside text that aren't in quotes.</p>"
            )
        except Exception as e:
            print(f"Error loading {name}: {e}")


    status.innerText = (
        f"{loaded} CSV file(s) loaded successfully. Processing checks..."
    )


    if loaded == 3:
        execute_checks()
    else:
        status.innerText += " Please upload all three CSV files."

# -------- required_event_columns --------

def required_event_columns(df):
    """
    check event core columns against the required DwC terms

    @param df        A pandas dataframe of event core
 
    @return          A list of missing required DwC columns
    """
    # set the required event core columns
    required_event_columns = [
    "eventID",
    "eventDate",
    "decimalLatitude",
    "decimalLongitude",
    "countryCode",
    "geodeticDatum",
]
    # compare to the event df and print what required columns are missing
    missing_cols = list(set(required_event_columns).difference(df.columns))
    res = True
    msg = "✅ Passed required columns!"
    if missing_cols:
        msg = f"❌ Failed! Missing required DwC {missing_cols} columns."
        res = False
    return res, msg

# -------- required_occ_columns --------

def required_occ_columns(df):
    """
    check occurrence extension columns against the required DwC terms

    @param df        A pandas dataframe of occurrence extension
 
    @return          A list of missing required DwC columns
    """

    # set required occurrence columns
    required_occ_columns = [
    "occurrenceID",
    "scientificName",
    "basisOfRecord",
    "occurrenceStatus",
]
    # compare to occurrence df and print a list of missing required occurrence columns
    missing_cols = list(set(required_occ_columns).difference(df.columns))
    res = True
    msg = "✅ Passed required columns!"
    if missing_cols:
        msg = f"❌ Failed! Missing required DwC {missing_cols} columns."
        res = False
    return res, msg

# -------- required_event_columns --------

def required_emof_columns(df):
    """
    check emof columns against the required DwC terms

    @param df        A pandas dataframe of emof
 
    @return          A list of missing required DwC columns
    """

    # set required emof columns
    required_emof_columns = [
    "eventID",
    "occurrenceID",
    "measurementValue",
    "measurementType",
    "measurementUnit",
]
    # compare to the emof df and print missing required emof columns
    missing_cols = list(set(required_emof_columns).difference(df.columns))
    res = True
    msg = "✅ Passed required columns!"
    if missing_cols:
        msg = f"❌ Failed! Missing required DwC {missing_cols} columns."
        res = False
    return res, msg

# -------- check_occ_dupes --------
def check_occ_dupes(df):
    """
    Check if occurrenceID has any duplicate values in the occurrence dataframe.

    @param df        A pandas dataframe of the occurrence extension
    """
    ids = df["occurrenceID"]
    occ_dupes = ids[ids.duplicated()].unique().tolist()

    if occ_dupes:
        msg = f"❌ Duplicate occurrenceIDs found: {occ_dupes}"
        res = True
        print(f"❌ Duplicate occurrenceIDs found: {occ_dupes}")
    else:
        msg = "✅ No duplicate occurrenceIDs!"
        res = False
        print("✅ No duplicate occurrenceIDs!")
    return res, msg

# -------- check_event_dupes --------
def check_event_dupes(df):
    """
    Check if eventID has any duplicate values in the event dataframe.

    @param df        A pandas dataframe of the event core
    """
    ids = df["eventID"]
    event_dupes = ids[ids.duplicated()].unique().tolist()

    if event_dupes:
        msg = f"❌ Duplicate eventIDs found: {event_dupes}"
        res = True
        print(f"❌ Duplicate eventIDs found: {event_dupes}")
    else:
        res = False
        msg = "✅ No duplicate eventIDs!"
        print("✅ No duplicate eventIDs!")
    return res, msg

# -------- check_merge_tables --------

def check_merge_tables(df_event, df_occurrence, df_emof):
    """
    Merge event -> occurrence -> emof, validating one-to-many at each step.

    @param df_event        A pandas dataframe of event core
    @param df_occurrence   A pandas dataframe of occurrence extension
    @param df_emof         A pandas dataframe of emof

    @return merged table or errors from merge
    """

    try:
        df_event_occur = df_event.merge(
            df_occurrence, on="eventID", validate="one_to_many", suffixes=(None, "_occ")
        )
        if df_event_occur.empty:
            return None, "❌ Merge event/occurrence tables failed: no matching eventIDs."

        # Treat empty or whitespace-only occurrenceIDs as missing.
        if "occurrenceID" in df_emof.columns:
            df_emof = df_emof.assign(
                occurrenceID=df_emof["occurrenceID"].replace(r"^\s*$", np.nan, regex=True)
            )

        # Use occurrenceID if emof has any values in it; otherwise fall back to eventID.
        if "occurrenceID" in df_emof.columns and df_emof["occurrenceID"].notna().any():
            key = "occurrenceID"
        else:
            key = "eventID"
            df_emof = df_emof.drop(columns="occurrenceID", errors="ignore")
            df_event_occur = df_event_occur.drop_duplicates("eventID")

        df = df_event_occur.merge(
            df_emof, on=key, validate="one_to_many", suffixes=(None, "_emof")
        )
        if df.empty:
            return None, f"❌ Merge emof table failed: no matching {key} values."

        msg = f"✅ Merge tables passed (emof joined on {key})!"
        if len(df) != len(df_emof):
            msg += f"\n⚠️  Merged rows ({len(df)}) != emof rows ({len(df_emof)})."
        return df, msg
    except pd.errors.MergeError as err:
        return None, f"❌ Merge tables failed.\n{err}"

# # -------- check_dwc_columns --------
# # load in the list of terms for event, occurrence, and emof
# TDWG_BASE = "https://raw.githubusercontent.com/tdwg/dwc/master/dist/dwc-a/"
# TERM_LISTS = {
#     "event": f"{TDWG_BASE}dwc_event_core_vertical.csv",
#     "occurrence": f"{TDWG_BASE}dwc_occurrence_core_vertical.csv",
#     "emof": f"{TDWG_BASE}measurements_or_facts_extension_vertical.csv",
# }

# # download all three lists and combine them into one set of valid terms
# ALL_TERMS = set()
# for url in TERM_LISTS.values():
#     ALL_TERMS |= set(pd.read_csv(url, header=None)[0])

# # function
# def check_all_columns(df):
#     """
#     Check every column name against the combined TDWG term lists.

#     @param df        A pandas dataframe

#     @return          (res, msg): True/False for pass/fail, and a message
#     """
#     unknown = [col for col in df.columns if col not in ALL_TERMS]

#     res = True
#     msg = "✅ All column names are valid DwC terms"
#     if unknown:
#         res = False
#         msg = f"⚠️  Columns not in the DwC term list: {unknown}"
#     return res, msg


# -------- check_null_values --------

def check_null_values(df):
    """
    check the merged dataframe for null values accross all columns

    @param df        A pandas dataframe of all merged tables (output of check_merge_tables)    
 
    @return          A list of columns that contain null values
    """

    missing = df.columns[df.isna().any()].to_list()
    res = True
    msg = "✅ Passed null values check!"
    if missing:
        msg = f"⚠️  Columns {missing} have missing values."
        res = False
    return res, msg

# -------- check_latitude --------

def check_latitude(df):
    """
    check to make sure that latitude is within bounds

    @param df        A pandas dataframe that contains decimalLatitude    
 
    @return          A list of invalid decimalLatitude values
    """
    
    res = True
    msg = "✅ Passed `decimalLatitude` bounds!"
    if "decimalLatitude" not in df.columns:
        return False, "⚠️  Cannot find `decimalLatitude` column."
    lat = pd.to_numeric(df["decimalLatitude"], errors="coerce")
    invalid_lat = df[
        lat.isna()
        | (lat <= -90)
        | (lat >= 90)
    ]
    if not invalid_lat.empty:
        msg = f"❌ Invalid `decimalLatitude` values detected. {invalid_lat.index.tolist()}"
        res = False
    return res, msg

# -------- check_longitude --------

def check_longitude(df):
    """
    check to make sure that longitude is within bounds

    @param df        A pandas dataframe that contains decimalLongitude
 
    @return          A list of invalid decimalLongitude values
    """
    res = True
    msg = "✅ Passed `decimalLongitude` bounds!"
    if "decimalLongitude" not in df.columns:
        return False, "⚠️  Cannot find `decimalLongitude` column."
    lon = pd.to_numeric(df["decimalLongitude"], errors="coerce")
    invalid_lon = df[
        lon.isna()
        | (lon <= -180)
        | (lon >= 180)
    ]
    if not invalid_lon.empty:
        msg = f" Invalid decimalLongitude values detected. {invalid_lon.index.tolist()}"
        res = False
    return res, msg
    
# -------- check_depth_consistency --------

def check_depth_consistency(df):
    """
    Check that depth values are numeric and that minimum depth is not
    greater than maximum depth.

    @param df        A pandas dataframe that contains minimumDepthInMeters
                     and maximumDepthInMeters

    @return          (res, msg): True/False for pass/fail, and a message
    """
    if not {"minimumDepthInMeters", "maximumDepthInMeters"}.issubset(df.columns):
        return False, "⚠️  No depth information found (minimumDepthInMeters/maximumDepthInMeters)."

    # convert to numbers; anything that isn't a number becomes NaN
    min_depth = pd.to_numeric(df["minimumDepthInMeters"], errors="coerce")
    max_depth = pd.to_numeric(df["maximumDepthInMeters"], errors="coerce")

    # non-numeric = had a value in the file, but couldn't be converted to a number
    bad_min = df["minimumDepthInMeters"].notna() & min_depth.isna()
    bad_max = df["maximumDepthInMeters"].notna() & max_depth.isna()

    # min should not be greater than max
    illogical = min_depth > max_depth

    res = True
    msg = "✅ Passed depth consistency test!"
    problems = []
    if bad_min.any():
        problems.append(f"⚠️  Non-numeric values in minimumDepthInMeters, rows {df.index[bad_min].tolist()}")
    if bad_max.any():
        problems.append(f"⚠️  Non-numeric values in maximumDepthInMeters, rows {df.index[bad_max].tolist()}")
    if illogical.any():
        problems.append(f"❌ minimumDepthInMeters > maximumDepthInMeters, rows {df.index[illogical].tolist()}")
    if problems:
        res = False
        msg = "\n".join(problems)
    return res, msg

# -------- check_scientific_names --------

def check_scientific_names(df):
    """
    check scientific names against WoRMS

    @param df        A pandas dataframe that contains species information   
 
    @return          A list of inconsistent scientific naming against WoRMS
    """
    
    if "scientificName" not in df.columns:
        return [None, "⚠️  Missing the `scientificName` column!"]

    names = list(set(df["scientificName"]))

    results = [check_scientific_name(name) for name in names]
    return [msg for res, msg in results if not res]


@functools.lru_cache(maxsize=128)
def check_scientific_name(name):
    response = _check_scientific_name(name)

    # Bail early to avoid unnecessary retries.
    if response.status_code == 204 or response.status_code == 400:
        return False, f"⚠️  {response.status_code=} for {name=}."

    if response.status_code == 200:
        results = response.json()
    else:
        msg = f"⚠️  WoRMS API Error: {response.status_code} for {name=}"
        return False, msg

    # The API returns a list of lists (one list per name queried).
    is_unique = f"Found 1 match for {name=}"
    if len(results) > 1:
        is_unique = f"⚠️  Found more than 1 match for {name=}, selecting the first one"

    # Take first match.
    result = results[0]
    if result["status"] != "accepted":
        msg = f"{is_unique}\n⚠️  Taxon {result['status']=}. Accepted name: {result['valid_name']=}, {result['url']=}"
        return False, msg
    return True, f"{is_unique}."


@stamina.retry(on=requests.exceptions.HTTPError, attempts=3)
def _check_scientific_name(name):
    url = f"https://www.marinespecies.org/rest/AphiaRecordsByName/{name}?like=true&marine_only=true"
    return requests.get(url, timeout=60)

# -------- execute_checks --------
# -------- execute_checks --------

def execute_checks():
    """
    Run every check on the uploaded tables and print the results.
    """
    # the three uploaded tables (loaded by read_dwc)
    df_event = dfs["event"]
    df_occurrence = dfs["occurrence"]
    df_emof = dfs["emof"]
    status = document.querySelector("#status")

    # -------- event core checks --------
    print("\n----- Event core -----")
    res, msg = required_event_columns(df_event)
    df_out = pd.DataFrame({"Check": ["Required columns"], "Result": [msg]})
    #status.innerText += msg
    print(msg)
    res, msg = check_event_dupes(df_event)
    df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Duplicate eventIDs"], "Result": [msg]})], ignore_index=True)
    #status.innerText += msg

    # -------- occurrence extension checks --------
    print("\n----- Occurrence extension -----")
    res, msg = required_occ_columns(df_occurrence)
    df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Required occur columns"], "Result": [msg]})], ignore_index=True)
    #status.innerText += msg
    print(msg)

    res, msg = check_occ_dupes(df_occurrence)
    df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Duplicate occurrenceIDs"], "Result": [msg]})], ignore_index=True)
    #status.innerText += msg

    # -------- emof checks --------
    print("\n----- eMoF -----")
    res, msg = required_emof_columns(df_emof)
    df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Required emof columns"], "Result": [msg]})], ignore_index=True)
    print(msg)
    #status.innerText += msg

    # -------- merge the tables --------
    # Change this to check keys function
    # print("\n----- Merge -----")
    # df, msg = check_merge_tables(
    #     df_event=df_event, df_occurrence=df_occurrence, df_emof=df_emof
    # )
    # df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Merge tables"], "Result": [msg]})], ignore_index=True)
    # print(msg)
    #status.innerText += msg

    # -------- merged table checks --------
    #if df is not None:
    print("\n----- Merged table -----")
    res, msg = check_null_values(df_event)
    df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Check event null values"], "Result": [msg]})], ignore_index=True)
    
    res, msg = check_null_values(df_occurrence)
    df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Check occurrence null values"], "Result": [msg]})], ignore_index=True)
    
    res, msg = check_null_values(df_emof)
    df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Check emof null values"], "Result": [msg]})], ignore_index=True)
    #status.innerText += msg
    print(msg)
    res, msg = check_latitude(df_event)
    df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Check latitude"], "Result": [msg]})], ignore_index=True)
    #status.innerText += msg
    print(msg)
    res, msg = check_longitude(df_event)
    df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Check longitude"], "Result": [msg]})], ignore_index=True)
    #status.innerText += msg
    print(msg)
    res, msg = check_depth_consistency(df_event)
    df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Check depth consistency"], "Result": [msg]})], ignore_index=True)
    #status.innerText += msg
    print(msg)
    results = check_scientific_names(df_occurrence)
    df_out = pd.concat([df_out, pd.DataFrame({"Check": ["Check scientific names"], "Result": [results if results else "✅ All scientific names are valid"]})], ignore_index=True)
    #[print(msg) for msg in results]
    status.innerHTML = df_out.set_index("Check").rename_axis(None).to_html(index=True, border=0)