import pandas as pd
from lib.columns import clean_column_names
from lib.clean import clean_sex, clean_race

def extract_uof_levels(df):
    levels = df.uof_force_type_1.str.extract(r"(L[1234])")
    df.loc[:, "use_of_force_level"] = levels[0].str.replace(r"L(\w+)", r"Level \1", regex=True)
    return df 


def clean_level_desc(df):
    df.loc[:, "use_of_force_level_desc"] = (df.uof_force_type_1
                                       .str.replace(r"^L(\w+)", r"Level \1", regex=True)
                                       .str.replace(r"VehPursuits w/Injury", "Vehicle Pursuit (With Injury)")
                                       .str.replace(r"w\/injury", "With Injury", regex=True)
                                       .str.replace(r"No Wep", "No Weapon")
                                       .str.replace(r"Level (\w{1}) ?- ?(.+)", r"Level \1-\2", regex=True)
    )
    return df 

def clean():
    df = pd.read_fwf("../data/uof_raw.rpt", skiprows=[1])
    df = (df[:-2].reset_index()
                 .pipe(clean_column_names)
                 .pipe(extract_uof_levels)
                 .pipe(clean_level_desc)
                 .pipe(clean_race)
                 .pipe(clean_sex)
    )
    df["counts"] = 1
    df["year"] = "2022"
    return df

