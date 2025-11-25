"""
Matt's home grown code for checking

1. Loads three CSV files (`event_bd.csv`, `occurrence_bd.csv`, and `emof_bd.csv`) into pandas DataFrames.
1. It then attempts to merge these DataFrames.
   1. First, `df_event` is merged with `df_occurrence` on the `eventID` column, with a validation to ensure it's a one-to-many relationship. The event file should have a unique list of `eventID` which match to `eventID` in the occurrence file.
   1. Next, the resulting DataFrame (`df_event_occur`) is merged with df_emof on occurrenceID, again with a one-to-many validation. The event and occurrence data files should have unique `occurrenceID` values which map to the `occurrenceID` in the emof file.
1. Finally, it checks if the number of rows in the final merged DataFrame (df_event_occur_emof) is equal to the number of rows in the df_emof DataFrame, indicating a successful arrangement of event files. Since we are merging all of the data together, there should be the same number of rows in the final dataset as there are in the emof file, but with more columns from the event and occurrence files.

If errors appear from this section, there are problems with the source data files that should be addressed.
"""

import pandas as pd
import requests
import time


df_event = pd.read_csv("event_bd.csv")
df_occurrence = pd.read_csv("occurrence_bd.csv")
df_emof = pd.read_csv("emof_bd.csv")


df_event_occur = df_event.merge(df_occurrence, on="eventID", validate="one_to_many")

df_event_occur_emof = df_event_occur.merge(
    df_emof, on="occurrenceID", validate="one_to_many", suffixes=(None, None)
)

if df_event_occur_emof.shape[0] == df_emof.shape[0]:
    print("Successfully arranged event files!")
else:
    print("Problems matching event, occurrence, and emof")

print(f"event: {df_event.shape}")
print(f"occurrence: {df_occurrence.shape}")
print(f"emof: {df_emof.shape}")


# Here are some hacks to make it work.

df_event_occur = df_event.drop_duplicates(subset="eventID", keep="last").merge(
    df_occurrence,
    on="eventID",
    validate="one_to_many",
)


df_event_occur_emof = df_event_occur.drop_duplicates(
    subset="occurrenceID", keep="first"
).merge(
    df_emof.drop(columns=["eventID"]),
    on="occurrenceID",
    validate="one_to_many",
    suffixes=(None, None),
)

if df_event_occur_emof.shape[0] == df_emof.shape[0]:
    print(
        f"Successfully arranged event files!\nevent + occurrence + emof = {df_event_occur_emof.shape}"
    )
else:
    print("Problems matching event, occurrence, and emof")


"""
Doing validation checks for an EventCore package

Using some of the code from Gemini, we can build a simple checker.

Using the merged Darwin Core dataset (`df_event_occur_emof`). This performs the following checks:

1. Checks for the presence of required columns;
1. Verifies data completeness by looking for null values in critical fields;
1. Validates the geographic coordinates (latitude and longitude) to ensure they are within valid ranges;
1. Checks for depth information, making sure that minimumDepthInMeters is not greater than maximumDepthInMeters and that depth values are numeric;
1. Checks with the World Register of Marine Species (WoRMS) API to validate scientific names, identifying any unaccepted or unfound taxa.
"""

required_occ_terms = [
    "occurrenceID",
    "scientificName",
    "eventDate",
    "decimalLatitude",
    "decimalLongitude",
    "basisOfRecord",
    "occurrenceStatus",
]

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


required_terms = set(required_event_terms + required_emof_terms + required_event_terms)

print("🔍 Checking structure...")
missing_cols = [col for col in required_terms if col not in df_event_occur_emof.columns]

if missing_cols:
    print(f"Missing required DwC columns: {', '.join(missing_cols)}")
else:
    print("All core DwC columns are present.")


# Checks for null values in critical fields.
print("🔍 Checking completeness...")
for col in required_terms:
    if col in df_event_occur_emof.columns:
        missing = df_event_occur_emof[df_event_occur_emof[col].isna()]
        if not missing.empty:
            print(
                'WARNING", f"Column "{col}" has {len(missing)} missing values.',
                missing.index.tolist(),
            )


# Validates Latitude and Longitude.
print("🔍 Checking coordinates...")
if "decimalLatitude" in df_event_occur_emof.columns:
    invalid_lat = df_event_occur_emof[
        pd.to_numeric(df_event_occur_emof["decimalLatitude"], errors="coerce").isna()
        | (df_event_occur_emof["decimalLatitude"] < -90)
        | (df_event_occur_emof["decimalLatitude"] > 90)
    ]
    if not invalid_lat.empty:
        print(
            "CRITICAL",
            "Invalid decimalLatitude values detected.",
            invalid_lat.index.tolist(),
        )

if "decimalLongitude" in df_event_occur_emof.columns:
    invalid_lon = df_event_occur_emof[
        pd.to_numeric(df_event_occur_emof["decimalLongitude"], errors="coerce").isna()
        | (df_event_occur_emof["decimalLongitude"] < -180)
        | (df_event_occur_emof["decimalLongitude"] > 180)
    ]
    if not invalid_lon.empty:
        print(
            "CRITICAL",
            "Invalid decimalLongitude values detected.",
            invalid_lon.index.tolist(),
        )


# AQUATIC SPECIFIC CHECKS.
# Checks depth logic (Min <= Max) and numeric validity.
print("🌊 Checking aquatic depth logic...")
has_min = "minimumDepthInMeters" in df_event_occur_emof.columns
has_max = "maximumDepthInMeters" in df_event_occur_emof.columns

if not has_min and not has_max:
    print(
        "WARNING",
        "No depth information found (minimumDepthInMeters/maximumDepthInMeters).",
    )
if has_min:
    non_numeric_min = df_event_occur_emof[
        pd.to_numeric(
            df_event_occur_emof["minimumDepthInMeters"], errors="coerce"
        ).isna()
    ]
    if not non_numeric_min.empty:
        print(
            "WARNING",
            "Non-numeric values in minimumDepthInMeters",
            non_numeric_min.index.tolist(),
        )
if has_min and has_max:
    # Check logic: Min should not be greater than Max
    # We use pd.to_numeric to ensure we aren"t comparing strings
    min_d = pd.to_numeric(df_event_occur_emof["minimumDepthInMeters"], errors="coerce")
    max_d = pd.to_numeric(df_event_occur_emof["maximumDepthInMeters"], errors="coerce")

    illogical = df_event_occur_emof[(min_d > max_d) & min_d.notna() & max_d.notna()]

    if not illogical.empty:
        print(
            "CRITICAL",
            "minimumDepthInMeters is greater than maximumDepthInMeters",
            illogical.index.tolist(),
        )


# Validates Scientific Names against the World Register of Marine Species (WoRMS) API.
# Uses batching to be polite to the API.

if "scientificName" not in df_event_occur_emof.columns:
    print("Missing scientificName.")

print("🐠 Verifying taxonomy with WoRMS API (this may take a moment)...")

unique_names = df_event_occur_emof["scientificName"].dropna().unique().tolist()

# WoRMS AphiaRecordsByMatchNames endpoint
url = "https://www.marinespecies.org/rest/AphiaRecordsByMatchNames"

# Process in chunks of 50 to avoid timeouts
chunk_size = 50
invalid_taxa = []
unmatched_taxa = []

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
    print("WARNING", f"Taxa not found in WoRMS: {', '.join(unmatched_taxa[:10])}...")
