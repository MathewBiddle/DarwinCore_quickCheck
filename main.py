import pandas as pd
import janitor  # noqa: F401
from io import StringIO
from js import document


kw = {"encoding_errors": "ignore"}
clean_names = {
    "axis": "columns",
    "strip_underscores": True,
    "case_type": "preserve",
    "remove_special": True,
}

def check_merge_tables(df_event, df_occurrence, df_emof):
    """Using `one_to_many` to check if merge keys are unique in left dataset."""
    df = None
    try:
        msg = "✅ Merge tables passed!"
        df_event_occur = df_event.merge(
            df_occurrence, on="eventID", validate="one_to_many"
        )
        if df_event_occur.empty:
            msg = f"❌ Merge tables failed.\nCould not merge {set(df_event['eventID'])}\non\n{set(df_occurrence['eventID'])}."
            return df, msg
        df = df_event_occur.merge(
            df_emof, on="occurrenceID", validate="one_to_many", suffixes=(None, "r_")
        )
        if df.empty:
            msg = f"❌ Merge tables failed.\nCould not merge {set(df_occurrence['occurrenceID'])}\non\n{set(df_emof['occurrenceID'])}."
            return df, msg
    except pd.errors.MergeError as err:
        msg = f"❌ Merge tables failed. \n{err}"
    return df, msg

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
            df = pd.read_csv(StringIO(content), **kw).clean_names(**clean_names)
            k = name.lower()
            dfs.update({k: df})
            loaded += 1
            
            output.innerHTML += f"""
                <div class="dataset">
                    <h3>{name}</h3>
                    <p>
                        Rows: {df.shape[0]} |
                        Columns: {df.shape[1]}
                    </p>
                </div>
            """

            html_table = df.head(5).to_html(
                index=False,
                border=0
            )

            output.innerHTML += html_table

            print(f"\n{name}")
            print(df.info())

        except Exception as e:
            print(f"Error loading {name}: {e}")


    status.innerText = (
        f"{loaded} CSV file(s) loaded successfully."
    )


    if loaded == 3:
        execute_merge()
    else:
        status.innerText += "Fail to load all 3 tables."


def execute_merge():

    global df, msg

    try:
        df, msg = check_merge_tables(
            df_event=dfs["event"],
            df_occurrence=dfs["occurrence"],
            df_emof=dfs["emof"],
        )

        status = document.querySelector("#status")
        status.innerText = msg

        if df is not None:
            print("Merged DataFrame:")
            print(df.head())

    except Exception as e:
        status = document.querySelector("#status")
        status.innerText = f"Error during merge: {e}"
