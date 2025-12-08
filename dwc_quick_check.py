# FF: Make every test a pass/fail for the HTML report.
# Some of the second phase tests are blocked if the data does not align, but can run them
# anyway to return a more meaningful report?

"""
Matt's home grown code for checking

1. Loads three CSV files (`event_bd.csv`, `occurrence_bd.csv`, and `emof_bd.csv`) into pandas
DataFrames.

1. Then attempts to merge these DataFrames.
   1. First, `df_event` is merged with `df_occurrence` on the `eventID` column, with a validation
      to ensure it's a one-to-many relationship. The event file should have a unique list of
      `eventID` which match to `eventID` in the occurrence file.
   1. Next, the resulting DataFrame (`df_event_occur`) is merged with df_emof on occurrenceID,
      again with a one-to-many validation. The event and occurrence data files should have unique
      `occurrenceID` values which map to the `occurrenceID` in the emof file.
1. Finally, it checks if the number of rows in the final merged DataFrame (df_event_occur_emof)
   is equal to the number of rows in the df_emof DataFrame, indicating a successful arrangement
   of event files. Since we are merging all of the data together, there should be the same number
   of rows in the final dataset as there are in the emof file, but with more columns from the
   event and occurrence files.

If errors appear from this section, there are problems with the source data files that should be
addressed.

Doing validation checks for an EventCore package

Using some of the code from Gemini, we can build a simple checker.

Using the merged Darwin Core dataset (`df_event_occur_emof`).
This performs the following checks:

1. Checks for the presence of required columns;
1. Verifies data completeness by looking for null values in critical fields;
1. Validates the geographic coordinates (latitude and longitude) to ensure they are within valid
   ranges;
1. Checks for depth information, making sure that `minimumDepthInMeters` is not greater than
   `maximumDepthInMeters` and that depth values are numeric;
1. Checks with the World Register of Marine Species (WoRMS) API to validate scientific names,
   identifying any unaccepted or unfound taxa.
"""

import pandas as pd
import requests
import time

from pathlib import Path

test_dir = Path("bad_data")
test_dir = Path("good_data")

df_event = pd.read_csv(test_dir.joinpath("event_bd.csv"))
df_occurrence = pd.read_csv(test_dir.joinpath("occurrence_bd.csv"))
df_emof = pd.read_csv(test_dir.joinpath("emof_bd.csv"))

# FF: Before merging we can be more explicitly and not rely on pandas API in case that change.
assert len(set(df_event["eventID"])) == len(df_event)
assert sorted(set(df_event["eventID"])) == sorted(set(df_occurrence["eventID"]))


# First check: Merge tables
def test_merge_tables(df, other, on, validate="one_to_many"):
    # “one_to_many” or “1:m”: check if merge keys are unique in left dataset.
    df_out = None
    try:
        df_out = df.merge(other, on=on, validate="one_to_many")
    except pd.errors.MergeError as err:
        print(f"Failed Merging DataFrames {df} and {other}.\n{err}")
    return df_out


df_event_occur = test_merge_tables(df=df_event, other=df_occurrence, on="eventID")
if df_event_occur is not None:
    # suffixes=(None, None)
    df_event_occur_emof = test_merge_tables(
        df=df_event_occur, other=df_emof, on="occurrenceID"
    )

# We should report these shapes in the HTML.
print(f"event: {df_event.shape}")
print(f"occurrence: {df_occurrence.shape}")
print(f"emof: {df_emof.shape}")


# FF: Are this always the same fix or should we just report and leave it to the user?
# # Here are some hacks to make it work.
# df_event_occur = df_event.drop_duplicates(subset="eventID", keep="last").merge(
#     df_occurrence,
#     on="eventID",
#     validate="one_to_many",
# )


# df_event_occur_emof = df_event_occur.drop_duplicates(
#     subset="occurrenceID", keep="first"
# ).merge(
#     df_emof.drop(columns=["eventID"]),
#     on="occurrenceID",
#     validate="one_to_many",
#     suffixes=(None, None),
# )

# if df_event_occur_emof.shape[0] == df_emof.shape[0]:
#     print(
#         f"Successfully arranged event files!\nevent + occurrence + emof = {df_event_occur_emof.shape}"
#     )
# else:
#     print("Problems matching event, occurrence, and emof")


# FF: Unused
# required_occ_terms = [
#     "occurrenceID",
#     "scientificName",
#     "eventDate",
#     "decimalLatitude",
#     "decimalLongitude",
#     "basisOfRecord",
#     "occurrenceStatus",
# ]

required_emof_terms = [
    "eventID",
    "occurrenceID",
    "measurementValue",
    "measurementType",
    "measurementUnit",
]

required_event_terms = [
    "eventID",
    "eventDate",
    "decimalLatitude",
    "decimalLongitude",
    "countryCode",
    "geodeticDatum",
]

# Unless we will need to check independent tables, this step is unnecessary and we can
# create a single list.
required_terms = set(required_event_terms + required_emof_terms)


def test_required_columns(df):
    print("🔍 Checking structure...")
    missing_cols = [col for col in required_terms if col not in df.columns]

    if missing_cols:
        print(f"Missing required DwC columns: {', '.join(missing_cols)}")
    else:
        print("All core DwC columns are present.")


def test_null_values(df):
    print("🔍 Checking completeness...")
    for col in required_terms:
        if col in df.columns:
            missing = df[df[col].isna()]
            if not missing.empty:
                print(
                    'WARNING", f"Column "{col}" has {len(missing)} missing values.',
                    missing.index.tolist(),
                )


def validate_coordinates(df):
    print("🔍 Checking coordinates...")
    if "decimalLatitude" in df.columns:
        invalid_lat = df[
            pd.to_numeric(df["decimalLatitude"], errors="coerce").isna()
            | (df["decimalLatitude"] < -90)
            | (df["decimalLatitude"] > 90)
        ]
        if not invalid_lat.empty:
            print(
                "CRITICAL",
                "Invalid decimalLatitude values detected.",
                invalid_lat.index.tolist(),
            )

    if "decimalLongitude" in df.columns:
        invalid_lon = df[
            pd.to_numeric(df["decimalLongitude"], errors="coerce").isna()
            | (df["decimalLongitude"] < -180)
            | (df["decimalLongitude"] > 180)
        ]
        if not invalid_lon.empty:
            print(
                "CRITICAL",
                "Invalid decimalLongitude values detected.",
                invalid_lon.index.tolist(),
            )


# AQUATIC SPECIFIC CHECKS.
def depth_consistency_checks(df):
    print("🌊 Checking aquatic depth logic...")
    # FF: If these are mandatory columns, should they be in the check above?
    # Or do we need to disambiguate Errors from Warnings in the HTML report?
    has_min = "minimumDepthInMeters" in df.columns
    has_max = "maximumDepthInMeters" in df.columns

    if not has_min and not has_max:
        print(
            "WARNING",
            "No depth information found (minimumDepthInMeters/maximumDepthInMeters).",
        )
    if has_min:
        non_numeric_min = df[
            pd.to_numeric(df["minimumDepthInMeters"], errors="coerce").isna()
        ]
        if not non_numeric_min.empty:
            print(
                "WARNING",
                "Non-numeric values in minimumDepthInMeters",
                non_numeric_min.index.tolist(),
            )
    # Checking for numeric values with coerce can hide issues with the data.
    # Example: pd.to_numeric(["1", "3.14", "nan", "ana"], errors="coerce")
    # You do want to coerce NaN, but do you want to coerce Ana?
    if has_min and has_max:
        # Check logic: Min should not be greater than Max
        # We use pd.to_numeric to ensure we aren't comparing strings
        # This should be done at loading time, pandas can be used as a linter there
        # and enforce dtypes
        min_d = pd.to_numeric(df["minimumDepthInMeters"], errors="coerce")
        max_d = pd.to_numeric(df["maximumDepthInMeters"], errors="coerce")

        illogical = df[(min_d > max_d) & min_d.notna() & max_d.notna()]

        if not illogical.empty:
            print(
                "CRITICAL",
                "minimumDepthInMeters is greater than maximumDepthInMeters",
                illogical.index.tolist(),
            )


def validate_scientific_names(df):
    # TODO: Refactor with memoize and individual name check in the memoized function
    # for speed and server side optiomization.
    # FF: If these are mandatory columns, should they be in the check above?
    # Or do we need to disambiguate Errors from Warnings in the HTML report?
    if "scientificName" not in df_event_occur_emof.columns:
        print("Missing scientificName.")

    print("🐠 Verifying taxonomy with WoRMS API (this may take a moment)...")
    # FF: Should this be exact or fuzzy?
    # For example, we have 'polychaeta spp. 1' and 'polychaeta spp. 2' there.
    unique_names = df_event_occur_emof["scientificName"].dropna().unique().tolist()
    # WoRMS AphiaRecordsByMatchNames endpoint
    url = "https://www.marinespecies.org/rest/AphiaRecordsByMatchNames"

    # Process in chunks of 50 to avoid timeouts
    chunk_size = 50
    # invalid_taxa = []  # FF: Unused
    unmatched_taxa = []

    # FF: Gemini thinks this is matlab!?
    for i in range(0, len(unique_names), chunk_size):
        chunk = unique_names[i : i + chunk_size]
        try:
            # Construct query parameters
            params = {"scientificnames[]": chunk, "marine_only": "true"}
            response = requests.get(url, params=params)

            if response.status_code == 200:
                results = response.json()

                # The API returns a list of lists (one list per name queried)
                for original_name, matches in zip(chunk, results):
                    if not matches:
                        unmatched_taxa.append(original_name)
                    else:
                        # Check if accepted
                        match = matches[0]  # Take best match
                        if match["status"] != "accepted":
                            print(
                                f'WARNING", f"Taxon "{original_name}" is {match["status"]}. Accepted name: {match["valid_name"]}, {match["url"]}'
                            )
            else:
                print("WARNING", f"WoRMS API Error: {response.status_code}")

        except Exception as e:
            print(f"Error connecting to WoRMS: {e}")

        # Polite delay
        time.sleep(0.5)

    if unmatched_taxa:
        print(
            "WARNING", f"Taxa not found in WoRMS: {', '.join(unmatched_taxa[:10])}..."
        )


test_required_columns(df_event_occur_emof)
test_null_values(df_event_occur_emof)
validate_coordinates(df_event_occur_emof)
depth_consistency_checks(df_event_occur_emof)
validate_scientific_names(df_event_occur_emof)
