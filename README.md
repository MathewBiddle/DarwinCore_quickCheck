# DarwinCore_quickCheck
A tool to quickly check the compliance of data files to the Darwin Core standard.

<img width="102" height="102" alt="image" src="https://github.com/user-attachments/assets/c90284a0-a5f7-4f33-b096-42395cc1aad0" />


## Step-by-Step

1. Loads three CSV files (`event_bd.csv`, `occurrence_bd.csv`, and `emof_bd.csv`) into pandas DataFrames. This could be a drop down picker.
1. It then attempts to merge these DataFrames.
   1. First, `df_event` is merged with `df_occurrence` on the `eventID` column, with a validation to ensure it's a one-to-many relationship. The event file should have a unique list of `eventID` which match to `eventID` in the occurrence file.
   1. Next, the resulting DataFrame (`df_event_occur`) is merged with `df_emof` on `occurrenceID`, again with a one-to-many validation. The event and occurrence data files should have unique `occurrenceID` values which map to the `occurrenceID` in the emof file.
1. Finally, it checks if the number of rows in the final merged DataFrame (`df_event_occur_emof`) is equal to the number of rows in the `df_emof` DataFrame, indicating a successful arrangement of event files. Since we are merging all of the data together, there should be the same number of rows in the final dataset as there are in the emof file, but with more columns from the event and occurrence files.
1. Using the merged Darwin Core dataset (df_event_occur_emof). This performs the following checks:
   1. Checks for the presence of required columns;
   1. Verifies data completeness by looking for null values in critical fields;
   1. Validates the geographic coordinates (latitude and longitude) to ensure they are within valid ranges;
   1. Checks for depth information, making sure that minimumDepthInMeters is not greater than maximumDepthInMeters and that depth values are numeric;
   1. Checks with the World Register of Marine Species (WoRMS) API to validate scientific names, identifying any unaccepted or unfound taxa.
1. Prints out the results/errors.
