import pandas as pd

def first_non_null(field:str, df:pd.DataFrame) -> any:
        val = df[df[field].notnull()]
        if len(val) == 0: return None
        return val[field].iloc[0]