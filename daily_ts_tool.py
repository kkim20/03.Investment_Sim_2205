import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime, sys

print('version 0.11, last updated: 2022/05/09')

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
    
def int_to_freq(obj):
    num = sorted([0.5,1.5, 4.5, 12.5, 56, 200, 370] + [obj]).index(obj)
    if num in list({1:"YS",2:"QS", 3:"MS", 4:"WS", 6:"DS"}):
        return({1:"YS",2:"QS", 3:"MS", 4:"WS", 6:"DS"}[num])
    return(np.nan)

def dat_freq(df_o):
    df = set_date(df_o)
    temp = list(set(df.index.year))
    df = pd.DataFrame([[x for x in temp], [df[df.index.year==x].shape[0] for x in temp]], index = ['year', 'count']).T
    df['freq'] = df['count'].map(lambda x: int_to_freq(x))
    return(df.dropna().iloc[-1]['freq'])

def ts_to_sines(df_o):
    df = df_o.copy().dropna()
    n = df.shape[0]
    fhat = np.fft.fft(df.T.values[0], n)
    PSD = fhat * np.conj(fhat) / n

    val = pd.DataFrame(PSD).reset_index()
    val['real'] = val[0].map(lambda x: x.real)
    val = val.head(int(val.shape[0]/2)).sort_values(by = "real", ascending = False)
    
    idx_dict = dict(zip(val['index'], [np.zeros(len(PSD)) for x in range(val.shape[0])]))
    for idx in list(idx_dict):
        idx_dict[idx][idx] = 1
    idx_dict = {k: pd.DataFrame([x.real for x in np.fft.ifft(fhat * v)]) 
                for k,v in idx_dict.items()}
    return(idx_dict)

def fft_proj(df_o, cols = "data", shift_n = 52, add_n = 52 * 3, proj_only = False):
    df, col = df_o.copy(), df_cols(df_o, cols = cols)[0]
    df, freq = df[[col]].dropna(), dat_freq(df[[col]].dropna())
    if freq == 'DS':
        df = dat_ave(df, freq = "WS")
    if df.mean()[col] > 1:
        org_df = df.copy()
        df = df.pct_change(shift_n).dropna()
    idx_dict = ts_to_sines(df)
    return(idx_dict)
