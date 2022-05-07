import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime, sys

print('version 0.1, last updated: 2022/05/07')

def df_cols(df, cols = "data", no_cols = ['date']):
    assert type(df) in [dict, pd.core.frame.DataFrame] and len(list(df))>0, "input type과 길이를 확인하세요"
    
    if type(cols) in [list, tuple, set]:
        cols = [x for x in cols if x in list(df) and x not in no_cols]
    else:
        cols = [x for x in [cols] if x in list(df) and x not in no_cols]
    if len(cols) == 0:
        cols = [x for x in list(df) if x not in no_cols]
    return(cols)

def python_type(obj):
    try:
        return(obj.item())
    except:
        return(obj)
    
def to_datetime(obj, date_format = '%Y-%m-%d'):
    if type(obj) == datetime.date:
        return(pd.to_datetime(obj))
    if type(obj) == str:
        try:
            return(datetime.datetime.strptime(obj, date_format))
        except:
            return(obj)
    try:
        temp = obj.date()
        return(pd.to_datetime(obj))
    except:
        return(obj)
    
def empty_df(col = 'date'):
    return(pd.DataFrame(columns =[col]).set_index(col))

def set_date(df_o, col = 'date'):
    assert type(df_o) == pd.core.frame.DataFrame
    
    if df_o.shape[0] == 0:
        if pd.DataFrame().index.name == col:
            return(df_o.copy())
        else:
            print("objectError: DataFrame 길이가 0, index 이름은 {0}이 아닙니다".format(col))
            sys.exit(1)
            
    df = df_o.copy()
    if type(to_datetime(df.index[0])) != pd._libs.tslibs.timestamps.Timestamp:
        cand = [x for x in list(df) 
                if type(to_datetime(df.iloc[0][x])) == pd._libs.tslibs.timestamps.Timestamp]
        if len(cand) == 0:
            print("typeError: DataFrame에 timestamp로 삼을 만한 column이 존재하지 않습니다")
            sys.exit(1)
        if col in cand:
            df = df.set_index(col)
        else:
            df = df.set_index(cand[0])
    df.index = df.index.map(lambda x: to_datetime(x))
    df.index.name = col
    return(df)

def dat_ave(df_o, freq = 'MS', how = 'mean', round_n = 6, **kawrgs):
    assert type(df_o) == pd.core.frame.DataFrame and type(freq) == str
    if "cols" in list(kawrgs):
        cols = df_cols(df_o, cols = kawrgs["cols"])
    else:
        cols = df_cols(df_o)
    df = set_date(df_o)[cols]
    if "D" in freq:
        return(df)
    elif "WS" == freq:
        res_df = df.resample('W', label='left', loffset=pd.DateOffset(days=1))
    else:
        try:
            res_df = df.resample(freq)
        except:
            print("objError: freq 입력값을 확인하세요")
            sys.exit(1)
    if how == 'mean':
        return(round(res_df.mean(), round_n))
    else:
        return(round(res_df.sum(), round_n))