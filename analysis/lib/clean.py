import pandas as pd

from lib.standardize import standardize_from_lookup_table


def clean_disposition(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Cleans disposition columns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            allegation columns

    Returns:
        the updated frame
    """
    for col in cols:
        df.loc[:, col] = (
            df[col]
            .str.strip()
        )
        df = standardize_from_lookup_table(
            df,
            col,
            [
                [
                    "Negoitated Settlement",
                    "NEGOTIATED SETTLEMENT",
                ],
                ["Data Inconsistency",
                 "NULL",
                 "CHARGES DISPROVEN",
                 "DUPLICATE",
                 "INFO",
                 "ABANDONMENT",
                 "CHARGES PROVEN RESIGNED",
                 "NAT",
                 "CANCELLED",
                 "CHARGES WITHDRAWN",
                 "CHARGES PROVEN",
                 "WITHDRAWN",
                 "INVESTIGATION CANCELLED",
                 "NO VIOLATIONS OBSERVED",
                 "RECLASSIFIED AS DI-3",
                 "RECLASSIFIED AS INFO",
                 "DUI",
                 "DECEASED",
                 "Dismissal - Rule 9",
                 "DUI-Dismiss Under Invest",
                 "RETIRED UNDER INVEST.",
                 "RUI-Resigned Under Inves",
                 "RUI-Retired Under Invest",
                 "See note",
                 "Resigned",
                 "DI-3",
                 ],
                ["NFIM", "NFIM CASE",
                "DI-3 NFIM"],
                [
                    "Not Sustained",
                    "NOT SUSTAINED",
                    "NOT SUSTAINED - RUI",
                    "RUI NOT SUSTAINED",
                    "DUI-NOT SUSTAINED",
                ],
                [
                    "Pending",
                    "PENDING",
                    "PENDING INVESTIGATION",
                    "AWAITING HEARING",
                ],
                ["Sustained",
                "SUSTAINED", 
                "RUI SUSTAINED",
                "SUSTAINED - RUI",
                "SUSTAINED - DUI",
                "DUI SUSTAINED",
                "SUSTAINED - Deceased",
                "SUSTAINED - RUI - RESIGN",
                "SUSTAINED - RUI - RETIRE",
                "Sustained - Dismissed",
                "SUSTAINED-OVERTURNED",
                "SUSTAINED - Prescribed",
                "SUSTAINED"],
                ["Unfounded", "UNFOUNDED- DUI", "UNFOUNDED"],
                ["Supervisory Feedback Log", "DI-2", "RECLASSIFIED AS DI-2"],
                ["test", "Withdrawn - Mediation"]
            ],
        )
    return df


def standardize_disposition(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Standardizes disposition columns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            allegation columns

    Returns:
        the updated frame
    """
    for col in cols:
        df.loc[:, col] = (
            df[col]
            .str.strip()
            .str.replace(r"(.+)?Not Sustained(.+)?", "1", regex=True)
            .str.replace(r"(.+)?Sustained(.+)?", "0", regex=True)
        )
    ## Function should be used to generate OIPM col. All PIB ids should have the same disposition and assignment
    ## Presumably, all actions should be the same. CHECK.
    return df


def names_to_title_case(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Converts name columns to title case

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            name columns

    Returns:
        the updated frame
    """
    cols_set = set(df.columns)
    for col in cols:
        if col not in cols_set:
            continue
        df.loc[:, col] = (
            df[col]
            .str.title()
            .str.replace(
                r" I(i|ii|v|x)$", lambda m: " I" + m.group(1).upper(), regex=True
            )
            .str.replace(
                r" V(i|ii|iii)$", lambda m: " V" + m.group(1).upper(), regex=True
            )
        )
    return df


def standardize_uids(df: pd.DataFrame) -> pd.DataFrame:
    """Removes spaces and unwanted characters from unique key

    Args:
        df (pd.DataFrame):
            the frame to process

    Returns:
        the updated frame
    """
    df.loc[:, "aio_num"] = (df.aio_num
                              .astype(str)
                              .str.lower()
                              .str.strip()
                              .str.replace(r"\s+", "", regex=True)
                              .str.replace(r"(\.|\,)", "", regex=True)
    )
    return df




def clean_sex(df: pd.DataFrame) -> pd.DataFrame:
    """Removes spaces and unwanted characters from unique key

    Args:
        df (pd.DataFrame):
            the frame to process

    Returns:
        the updated frame
    """

    df.loc[:, "officer_sex"] = (df.off_sex
                              .astype(str)
                              .str.strip()
                              .fillna("")
                              .str.replace(r"^F$", "Female", regex=True)
                              .str.replace(r"^M$", "Male", regex=True)
                              .str.replace(r"^$", "Unknown Sex", regex=True)
                            .str.replace(r"nan", "Unknown Sex", regex=True)
    )
    df.loc[:, "citizen_sex"] = (df.off_sex
                              .astype(str)
                              .str.strip()
                              .fillna("")
                              .str.replace(r"^M$", "Male", regex=True)
                              .str.replace(r"F", "Female", regex=False)
                              .str.replace(r"^$", "Unknown Sex", regex=True)
                               .str.replace(r"nan", "Unknown Sex", regex=True)

    )
    return df


def map_race(officer_race):
    if officer_race == "White":
        return "White"
    elif officer_race == "Black":
        return "Black / African American"
    elif officer_race == "American Ind":
        return "Native American"
    elif officer_race == "Hispanic":
        return "Hispanic"
    elif officer_race == "India":
        return "Indian"
    elif officer_race == "Asian":
        return "Asian"
    else:
        return "Unknown Race"
    

def clean_race(df):
    off_race = df.off_yr_employ_off_race.str.extract(r"(\w+? ?-?\w+)$")
    df.loc[:, "officer_race"] = off_race[0]
    df["officer_race"] = df["officer_race"].apply(map_race)

    df.loc[:, "citizen_race"] = (df.race
                              .astype(str)
                              .str.strip()
                              .fillna("")
                              .str.replace(r"Hispa", "Hispanic", regex=False)
                              .str.replace(r"w", "White", regex=False)
                              .str.replace(r"India", "Indian", regex=False)
                              .str.replace(r"Black", "Black / African American", regex=False)
                              .str.replace(r"Race-", "", regex=False)
                              .str.replace(r"^$", "Unknown Race", regex=True)
                              .str.replace(r"nan", "Unknown Race", regex=True)
    )

    return df 


def clean_findings(df):
    df.loc[:, "disposition"] = df.disposition.str.replace(r"(\w+) $", r"\1", regex=True)
    findings = ({
    'NEGOTIATED SETTLEMENT': 'Negotiated Settlement',
    'NULL': 'Data Inconsistency',
    'CHARGES DISPROVEN': 'Data Inconsistency',
    'DUPLICATE': 'Data Inconsistency',
    'INFO': 'Data Inconsistency',
    'ABANDONMENT': 'Data Inconsistency',
    'CHARGES PROVEN RESIGNED': 'Data Inconsistency',
    'NAT': 'Data Inconsistency',
    'CANCELLED': 'Data Inconsistency',
    'CHARGES WITHDRAWN': 'Data Inconsistency',
    'CHARGES PROVEN': 'Data Inconsistency',
    'WITHDRAWN': 'Data Inconsistency',
    'INVESTIGATION CANCELLED': 'Data Inconsistency',
    'NO VIOLATIONS OBSERVED': 'Data Inconsistency',
    'RECLASSIFIED AS DI-3': 'Data Inconsistency',
    'RECLASSIFIED AS INFO': 'Data Inconsistency',
    'DUI': 'Data Inconsistency',
    'DECEASED': 'Data Inconsistency',
    'Dismissal - Rule 9': 'Data Inconsistency',
    'DUI-Dismiss Under Invest': 'Data Inconsistency',
    'RETIRED UNDER INVEST.': 'Data Inconsistency',
    'RUI-Resigned Under Inves': 'Data Inconsistency',
    'RUI-Retired Under Invest': 'Data Inconsistency',
    'See note': 'Data Inconsistency',
    'Resigned': 'Data Inconsistency',
    'DI-3': 'Data Inconsistency',
    'DI-2': 'Data Inconsistency',
    'RUI': 'Data Inconsistency',
    'INFO ONLY CASE': 'Data Inconsistency',
    'OTHER': 'Data Inconsistency',
    'RECLASSIFIED AS DI-2': 'Data Inconsistency',
    'DUPLICATE INVESTIGATION': 'Data Inconsistency',
    'DUPLICATE ALLEGATION': 'Data Inconsistency',
    'Proscribed': 'Data Inconsistency',
    'UNFOUNDED - RUI': 'Data Inconsistency',
    'Non-Applicable': 'Data Inconsistency',
    'INFO CASE': 'Data Inconsistency',
    'DI-3': 'Data Inconsistency',
    'WITHDRAWN- MEDIATION': 'Data Inconsistency',
    'NSA - RUI': 'Data Inconsistency',
    'BWC - Redirection': 'Data Inconsistency',
    'REDIRECTION': 'Data Inconsistency',
    'Moot /Per R.S. 40:2531': 'Data Inconsistency',
    'Exonerated - RUI': 'Data Inconsistency',
    'GRIEVANCE': 'Data Inconsistency',
    'Moot/ Per R.S. 40:2531': 'Data Inconsistency',
    'REDIRECTION(SFL)': 'Data Inconsistency',
    'REDIRECTION (SFL)': 'Data Inconsistency',
    'Moot': 'Data Inconsistency',
    'EXONERATED': 'Data Inconsistency',
    'Exonerated': 'Data Inconsistency',
    'NFIM CASE': 'NFIM',
    'NFIM': 'NFIM',
    'DI-3 NFIM': 'NFIM',
    'NOT SUSTAINED': 'Not Sustained',
    'NOT SUSTAINED - RUI': 'Not Sustained',
    'RUI NOT SUSTAINED': 'Not Sustained',
    'DUI-NOT SUSTAINED': 'Not Sustained',
    'PENDING': 'Pending',
    'PENDING INVESTIGATION': 'Pending',
    'AWAITING HEARING': 'Pending',
    'SUSTAINED': 'Sustained',
    'RUI SUSTAINED': 'Sustained',
    'SUSTAINED - RUI': 'Sustained',
    'SUSTAINED - DUI': 'Sustained',
    'DUI SUSTAINED': 'Sustained',
    'SUSTAINED - Deceased': 'Sustained',
    'SUSTAINED - RUI - RESIGN': 'Sustained',
    'SUSTAINED - RUI - RETIRE': 'Sustained',
    'Sustained - Dismissed': 'Sustained',
    'SUSTAINED-OVERTURNED': 'Sustained',
    'SUSTAINED - Prescribed': 'Sustained',
    'UNFOUNDED- DUI': 'Unfounded',
    'UNFOUNDED': 'Unfounded',
    'UNFOUNDED.': 'Unfounded',
    })
    df["disposition"] = df.disposition.str.strip().map(findings)
    return df[~((df.disposition.fillna("") == ""))]

