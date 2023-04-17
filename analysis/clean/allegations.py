import pandas as pd
from lib.columns import clean_column_names
import re
import numpy as np


def get_finding(finding):
    if re.search(r'(?<!not )NOT SUSTAINED|Not Sustained', finding):
        return 'Not Sustained'
    elif re.search(r'SUSTAINED|Sustained', finding):
        return 'Sustained'
    elif re.search(r'(?<!not )SUSTAINED', finding):
        return 'Sustained'
    elif re.search(r'NEGOTIATED SETTLEMENT', finding):
        return 'Sustained'
    elif re.search(r'AWAITING HEARING', finding):
        return 'Sustained'
    elif re.search(r'WITHDRAWN', finding):
        return 'Mediation'
    elif re.search(r'DI-2', finding):
        return 'DI-2'
    elif re.search(r'REDIRECTION', finding):
        return 'DI-2'
    elif re.search(r'PENDING INVESTIGATION|Pending', finding):
        return 'Pending'
    elif re.search(r'NOT SUSTAINED', finding):
        return 'Not Sustained'
    elif re.search(r'UNFOUNDED|Unfounded', finding):
        return 'Unfounded'
    elif re.search(r'NO VIOLATIONS OBSERVED', finding):
        return 'Unfounded'
    elif re.search(r'EXONERATED|Exonerated', finding):
        return 'Exonerated'
    elif re.search(r'NFIM', finding):
        return 'NFIM'
    elif re.search(r'Resigned', finding):
        return 'Sustained'
    elif re.search(r'nan', finding):
        return ''
    elif re.search(r'DUPLICATE (ALLEGATION|Investigation)|CANCELLED|Closure', finding):
        return 'Illegitimate Outcome'
    else:
        return "Illegitimate Outcome"

def clean_disposition(df):
    df['disposition_oipm'] = df.apply(lambda x: f"{x['disposition']} {x['finding']} {x['final_dispo']}", axis=1)
    df['disposition_oipm'] = df.apply(lambda x: get_finding(x['disposition_oipm']), axis=1)
    return df

def clean_aio_num(df):
    df.loc[:, "aio_num"] = (df.aio_num
                            .astype(str)
                            .str.replace(r"\.(.+)", "", regex=True)
                            .str.replace(r"\s+", "", regex=True)
    )
    return df

def clean_incident_types(df):
    df.loc[:, "incident_type"] = df.incident_type.str.replace(r"Rank Initiated  ", "Rank Initiated", regex=True)
    return df 


def clean():
    dfa = pd.read_fwf("../data/allegations_raw.rpt", skiprows=[1], delim_whitespace=True)
    dfa = (dfa[:-2].reset_index()
          .pipe(clean_column_names)
          .pipe(clean_aio_num)
          .drop(columns=["allegation"]))
    
    dfa.loc[:, "aio_num"] = dfa.aio_num.astype(str)
     
    dfb = pd.read_csv("../data/allegations_raw.csv").pipe(clean_column_names).pipe(clean_aio_num)
        
    dfb.loc[:, "aio_num"] = dfb.aio_num.astype(str)
    dfb = dfb[["allegation", "aio_num"]]
    df = pd.merge(dfa, dfb, on="aio_num", how="outer")

    df = df.pipe(clean_disposition).pipe(clean_incident_types)
    df = df.drop_duplicates()
    df["counts"] = 1
    df["year"] = 2022
    return df

