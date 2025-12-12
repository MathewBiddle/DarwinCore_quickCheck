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

# TODO: Make every test a pass/fail for the HTML report.
# Some of the second phase tests are blocked if the data does not align, but can run them
# anyway to return a more meaningful report?

import functools

import stamina

import pandas as pd
import requests
from pathlib import Path

required_occurrence_columns = [
    "occurrenceID",
    "scientificName",
    "eventDate",
    "decimalLatitude",
    "decimalLongitude",
    "basisOfRecord",
    "occurrenceStatus",
]

required_emof_columns = [
    "eventID",
    "occurrenceID",
    "measurementValue",
    "measurementType",
    "measurementUnit",
]

required_event_columns = [
    "eventID",
    "eventDate",
    "decimalLatitude",
    "decimalLongitude",
    "countryCode",
    "geodeticDatum",
]


def check_required_columns(df, df_name, columns):
    missing_cols = list(set(columns).difference(df.columns))
    res = True
    msg = "✅"
    if missing_cols:
        msg = (
            f"❌ Missing required DwC columns: {', '.join(missing_cols)} in {df_name}."
        )
        res = False
    return res, msg


def check_null_values(df, columns):
    print("🔍 Checking completeness...")
    missing = df.columns[df.isna().any()].to_list()
    if missing:
        print(f"WARNING! Columns {missing} have missing values.")
        return False
    return True


def check_coordinates(df):
    print("🔍 Checking coordinates...")
    res = True
    if "decimalLatitude" in df.columns:
        invalid_lat = df[
            pd.to_numeric(df["decimalLatitude"], errors="coerce").isna()
            | (df["decimalLatitude"] <= -90)
            | (df["decimalLatitude"] >= 90)
        ]
        if not invalid_lat.empty:
            print(
                f"CRITICAL! Invalid decimalLatitude values detected. {invalid_lat.index.tolist()}"
            )
            res = False

    if "decimalLongitude" in df.columns:
        invalid_lon = df[
            pd.to_numeric(df["decimalLongitude"], errors="coerce").isna()
            | (df["decimalLongitude"] <= -180)
            | (df["decimalLongitude"] >= 180)
        ]
        if not invalid_lon.empty:
            print(
                f"CRITICAL! Invalid decimalLongitude values detected. {invalid_lon.index.tolist()}"
            )
            res = False
    return res


def check_depth_consistency(df):
    print("🌊 Checking aquatic depth logic...")
    res = True
    if ("minimumDepthInMeters", "maximumDepthInMeters") not in df.columns:
        print(
            "WARNING! No depth information found (minimumDepthInMeters/maximumDepthInMeters)."
        )
        # We return here b/c we cannot run the tests below without these columns.
        return False

    min_depth = pd.to_numeric(df["minimumDepthInMeters"], errors="coerce")
    if not min_depth.isna().empty:
        print(
            f"WARNING! Non-numeric values in minimumDepthInMeters {min_depth.index.tolist()}"
        )
        res = False

    max_depth = pd.to_numeric(df["minimumDepthInMeters"], errors="coerce")
    if not max_depth.isna().empty:
        print(
            f"WARNING! Non-numeric values in minimumDepthInMeters {max_depth.index.tolist()}"
        )
        res = False

    # Check logic: Min should not be greater than Max
    illogical = all(min_depth >= max_depth)

    if not illogical.empty:
        print(
            f"CRITICAL! minimumDepthInMeters is greater than maximumDepthInMeters {illogical.tolist()}"
        )
        res = False
    return res


def check_scientific_names(df):
    if "scientificName" not in df.columns:
        print("Missing scientificName.")
        return

    print("🐠 Verifying taxonomy with WoRMS API (this may take a moment)...")
    names = df["scientificName"].to_list()

    # TODO: Gemini thinks this is matlab!?
    return [check_scientific_name(name) for name in names]


@functools.lru_cache(maxsize=128)
def check_scientific_name(name):
    response = _check_scientific_name(name)

    # Bail early to avoid unnecessary retries.
    if response.status_code == 204 or response.status_code == 400:
        return False

    if response.status_code == 200:
        results = response.json()
    else:
        print(f"WARNING! WoRMS API Error: {response.status_code}")
        return False

    # The API returns a list of lists (one list per name queried).
    if len(results) > 1:
        print("WARNING! Found more than 1 match!")

    # Take first match.
    result = results[0]
    if result["status"] != "accepted":
        print(
            f"WARNING! Taxon {name} is {result['status']}. Accepted name: {result['valid_name']}, {result['url']}"
        )
        return False
    return True


@stamina.retry(on=requests.exceptions.HTTPError, attempts=3)
def _check_scientific_name(name):
    url = f"http://www.marinespecies.org/rest/AphiaRecordsByName/{name}?like=true&marine_only=true"
    return requests.get(url, timeout=60)


def check_merge_tables(df_event, df_occurrence, df_emof):
    """Using `one_to_many` to check if merge keys are unique in left dataset."""
    df_event_occur_emof = None
    print(f"event: {df_event.shape}")
    print(f"occurrence: {df_occurrence.shape}")
    print(f"emof: {df_emof.shape}")
    try:
        df_event_occur = df_event.merge(
            df_occurrence, on="eventID", validate="one_to_many"
        )
        df_event_occur_emof = df_event_occur.merge(
            df_emof, on="occurrenceID", validate="one_to_many", suffixes=(None, None)
        )
    except pd.errors.MergeError as err:
        print(f"Failed Merging DataFrames. \n{err}")
    return df_event_occur_emof


if __name__ == "__main__":
    # TODO: Web version add 3 boxes for each upload.
    # test_dir = Path("bad_data")
    test_dir = Path("good_data")
    df_event = pd.read_csv(test_dir.joinpath("event_bd.csv"))
    df_occurrence = pd.read_csv(test_dir.joinpath("occurrence_bd.csv"))
    df_emof = pd.read_csv(test_dir.joinpath("emof_bd.csv"))

    dfs = {
        "event": df_event,
        "occurrence": df_occurrence,
        "emof": df_emof,
    }

    for name, df, cols in zip(
        dfs.keys(),
        dfs.values(),
        [required_event_columns, required_occurrence_columns, required_emof_columns],
    ):
        # TODO: When using namedtuple we can skip tests that won't pass, like checking for depth in dfs that should not have it.
        res, msg = check_required_columns(df, name, columns=cols)
        print(f"Passing: {res}\n{msg}\n")
    #     res = [
    #         check_null_values(df, columns=cols),
    #         check_coordinates(df),
    #         check_depth_consistency(df),
    #     ]
    #     print(res)
    #     print(f"Finished {name}.")

    #     check_scientific_names(df)

    # check_merge_tables(df_event=df_event, df_occurrence=df_occurrence, df_emof=df_emof)
