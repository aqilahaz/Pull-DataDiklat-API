
from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
from requests import Session
from zeep import Client
from zeep.transports import Transport
from zeep.helpers import serialize_object
from pypika import Query, Table, Field
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import requests
import json
import time
from itertools import chain
import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import string

user = ''
password = ''

session = Session()
session.auth = HTTPBasicAuth(user, password)
client = Client('#link to client wsdl',
    transport=Transport(session=session, operation_timeout=120 ))
#engine SQLAlchemy
engine = create_engine('#connect to database', connect_args={'options': '-c search_path=]'})

def to_dict(z):
    input_dict = serialize_object(z)
    output_dict = json.loads(json.dumps(input_dict))
    return output_dict
  
def flatten_list(items):
    lists = list(chain.from_iterable(items))
    print(len(lists))
    return lists

def create_dataframe(data_kantor):
    column_names = ["kode_unit","nama_kantor","kode_kantor"]
    dat = pd.DataFrame(columns = column_names)

    dic = {}
    ArrKodeKantor=[]
    for index, item in enumerate(data_kantor):
        dic = item.get('kode_kantor')
        ArrKodeKantor.append(dic)
    return ArrKodeKantor


def request_data_kantor(kd):   
    items=[]
    for x in kd:
        DatKantor = to_dict(client.service.get_list_data_kantor(kode_unit = x))
        item = DatKantor.get('return')
        items.append(item)
        time.sleep(1)
    return items

def request_data_pegawai(d1):
    DatPegawai=[]
    for key in d1:
        link = '#link rest api'+key
        auth = HTTPBasicAuth('user', 'password')
        getListPegawai = requests.get(url=link, auth=auth, timeout=120)
        print("Status Request Data Pegawai:", getListPegawai.status_code)

        Dat = getListPegawai.json()
        if Dat['status']['statusCode'] == '1':
            y = Dat['dataPegawaiPerKantor']
            DatPegawai.append(y)
        else:
            print("....not found")
    return DatPegawai


def request_data_diklat(df):
    DatPegawai=[]
    for i in range(len(df)) :
        nip = df.loc[i, "nip"]
        kode_kantor=df.loc[i, "kode_kantor"]
        payload=""
        headers={'content-type': 'application/json; charset=utf-8'}
        retry_strategy = Retry(
                total=3,
                status_forcelist=[429, 500, 502, 503, 504],
                method_whitelist=["HEAD", "GET", "OPTIONS"]
            )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.headers.update(headers)
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        link = '#rest api'+str(kode_kantor)+'&nip='+nip
        auth = HTTPBasicAuth('#user', '#password')
        getListPegawai = http.get(url=link,data=payload,headers=headers,timeout=120,auth=auth)
        Dat = getListPegawai.json()
        if Dat['status']['statusCode'] == '1':
            y = Dat['diklatPerPegawaiPerKantor']
            DatPegawai.append(y)
            print(str(i+1)+"/"+str(len(df))+"...."+str(nip)+ ".... Status Request Data Diklat:"+ str(getListPegawai.status_code)+"...ada")
        else:
            print(str(i+1)+"/"+str(len(df))+"....not found")
        time.sleep(1)
    return DatPegawai

#preprocess data diklat
def preprocess(data_lengkap):
    diklat=pd.DataFrame(data_lengkap)
    diklat=diklat.drop_duplicates(subset=None, keep='first', inplace=False)
    diklat = diklat.replace(r'^\s*$', np.nan, regex=True)
    diklat = diklat.where(pd.notnull(diklat), None)
    diklat=diklat.drop(['kelompok', 'sektor'], axis=1)
    return diklat

def db_diklat(diklat):
    dff=diklat[['kursus']]
    dff.drop_duplicates(keep='first', inplace=True)
    dff=dff.dropna()
    dff.insert(1, 'sumber_data', 'API PUSTIKOM')
    #remove number in the beginning of string
    dff['kursus']=dff['kursus'].str.lstrip(string.digits)
    return dff

def db_data_diklat(diklat):
    dff=diklat[['kursus','mulai','selesai','tahun','lama','lokasi']]
    dff['mulai']= pd.to_datetime(dff['mulai'], errors = 'coerce')
    dff['selesai']= pd.to_datetime(dff['selesai'], errors = 'coerce')
    dff['lama_diklat'] = (dff['selesai'] - dff['mulai']).dt.days
    dff["lama_diklat"] = dff["lama_diklat"].astype('Int64')
    dff=dff.drop(['lama'], axis=1)
    dff=dff.drop_duplicates(subset=None, keep='first', inplace=False)
    #remove number in the beginning of string
    dff['kursus']=dff['kursus'].str.lstrip(string.digits)
    engine = create_engine('', connect_args={'options': '-c search_path='})
    sql2 = 'SELECT id as diklat_id, kursus FROM diklat'
    df2 = pd.read_sql(sql2, con=engine)
    res = pd.merge(dff, df2, how='left', on="kursus")
    filtered_df = res[res['diklat_id'].notnull()]
    filtered_df=filtered_df.drop_duplicates(subset=None, keep='first', inplace=False)
    dropcol = ['kursus']
    df3=filtered_df.drop(dropcol, axis=1)
    df3=df3.replace({np.nan: None})
    df3=df3.rename(columns={"mulai": "tanggal_mulai","selesai":"tanggal_selesai","lokasi":"tempat","lama_diklat": "lama_pelatihan"})
    df3=df3.where(df3.notnull(), None)
    df3['diklat_id'] = df3['diklat_id'].astype(str)
    df3['diklat_id'] = df3['diklat_id'].str.rstrip('.0')
    return df3

#remove number in the beginning of string
def db_peserta_diklat(diklat):
    df = diklat
    df['kursus'] = df['kursus'].str.lstrip(string.digits)
    df = df.drop(['lama'], axis=1)
    sql2 = 'SELECT tanggal_mulai as mulai, tanggal_selesai as selesai, tahun as tahun, tempat as lokasi, id, diklat_id FROM data_diklat'
    df2 = pd.read_sql(sql2, con=engine)
    sqll = 'SELECT id as diklat_id, kursus FROM diklat'
    dff = pd.read_sql(sqll, con=engine)
    dx = pd.merge(df2, dff, how='left', on='diklat_id')

    dx['tahun'] = dx['tahun'].astype(str)
    dx['mulai'] = dx['mulai'].astype(str)
    dx['selesai'] = dx['selesai'].astype(str)
    dx['lokasi'] = dx['lokasi'].astype(str)
    df['tahun'] = df['tahun'].astype(str)
    df['mulai'] = df['mulai'].astype(str)
    df['selesai'] = df['selesai'].astype(str)
    df['lokasi'] = df['lokasi'].astype(str)
    dz = pd.merge(df, dx, how='left', on=['kursus','lokasi','mulai','selesai','tahun'])
    filtered_df = dz[dz['id'].notnull()]
    filtered_df=filtered_df.drop_duplicates(subset=None, keep='first', inplace=False)
    sqll= 'SELECT id as pegawai_id, kode as nip FROM pegawai'
    df3 = pd.read_sql(sqll, con=engine)
    df3=df3.drop_duplicates(subset=None, keep='first', inplace=False)
    df3['pegawai_id'] = df3['pegawai_id'].astype(str)
    df3['nip'] = df3['nip'].astype(str)
    filtered_df['nip'] = filtered_df['nip'].astype(str)
    filtered_df = pd.merge(filtered_df, df3, how='left', left_on="nip", right_on="nip")
    dm=filtered_df[['pegawai_id','id','keterangan']]
    dm.insert(2, "status", "LULUS")
    nmfile = datetime.datetime.today().strftime('%Y-%m-%d')
    dm.insert(2, 'last_updated', nmfile)
    dm=dm.drop_duplicates(subset=None, keep='first', inplace=False)
    dm=dm.rename(columns={"keterangan": "sertifikat_kompetensi","id":"data_diklat_id"})
    temp2 = dm[dm['data_diklat_id'].notnull()]
    res = temp2[temp2['pegawai_id'].notnull()]
    return res

#-----------db diklat

def diklatInsert(diklat, col):
    tbl = Table("diklat")
    d = list(diklat.values())
    q = Query.into(tbl).columns(*col).insert(*d)
    diklatt = str(q).replace('"','')
    return diklatt


def diklatUpdate(diklat, col):    
    tbl = Table("diklat")
    d = list(diklat.values())
    q = Query.into(tbl).columns(*col).insert(*d)
    upd = tbl.update()
    i = 0
    for c in col:
        upd = upd.set(c, d[i])
        i+=1
    upd = upd.where(tbl.kode == diklat["kursus"])
    diklatt=str(upd).replace('"','')
    return diklatt

def datadiklatInsert(pegawai, col):
    tbl = Table("data_diklat")
    d = list(pegawai.values())
    q = Query.into(tbl).columns(*col).insert(*d)
    pegawaii = str(q).replace('"','')
    return pegawaii

def datadiklatUpdate(pegawai, col):    
    tbl = Table("data_diklat")
    d = list(pegawai.values())
    q = Query.into(tbl).columns(*col).insert(*d)
    upd = tbl.update()
    i = 0
    for c in col:
        upd = upd.set(c, d[i])
        i+=1
    upd = upd.where(tbl.diklat_id == pegawai["diklat_id"])
    pegawaii=str(upd).replace('"','')
    return pegawaii

def create_batch_data_2(df3):
    abc = df3.values.tolist()
    col = list(df3.columns)
        
    batch_sqls2 = ""
    batch2 = []

    print("Going to cek "+ str(len(df3)) + ' data')
    c = 1
    for index, val in enumerate(abc): 
        print(c)
        c+=1
        y = dict(zip(col, val))
        cek = pd.read_sql("select diklat_id,tanggal_mulai, tanggal_selesai, tahun, tempat from data_diklat where diklat_id='%s'" %y['diklat_id'] , engine)
        if cek.shape[0] > 0:
            print("update")
            s = datadiklatUpdate(y,col)
            batch2.append(s)
            batch_sqls2 += s +";\n"
        else:
            print("insert")
            s = datadiklatInsert(y,col)
            batch2.append(s)
            batch_sqls2 += s +";\n"


    print(str(len(df3)) + ' data checked')
    return batch2, batch_sqls2

def create_batch_data(dff):
    abc = dff.values.tolist()
    col = list(dff.columns)
        
    batch_sqls = ""
    batch = []

    print("Going to cek "+ str(len(dff)) + ' data')
    c = 1
    for index, val in enumerate(abc): 
        print(c)
        c+=1
        y = dict(zip(col, val))
        cek = pd.read_sql("select kursus from diklat where kursus='%s'" %y['kursus'] , engine)
        if cek.shape[0] > 0:
            print("update")
            pass
        else:
            print("insert")
            s = diklatInsert(y,col)
            batch.append(s)
            batch_sqls += s +";\n"

    print(str(len(dff)) + ' data checked')
    return batch, batch_sqls


#MAIN FUNCTION
#unit yang akan diambil datanya
kd = ['']

#Get Data Pegawai
items = request_data_kantor(kd)
data_kantor = flatten_list(items)
ArrKodeKantor = create_dataframe(data_kantor)
DatPegawai = request_data_pegawai(ArrKodeKantor)
data_pegawai = flatten_list(DatPegawai)

df = pd.DataFrame(data_pegawai)
# data = df.head(10)

#Data Diklat
DatDiklat = request_data_diklat(df)
data_lengkap = flatten_list(DatDiklat)
diklat = preprocess(data_lengkap)

#FOR TABLE DIKLAT
dff=db_diklat(diklat)
try:
    del dict
except:
    pass
batch,batch_sqls = create_batch_data(dff)

#FOR TABLE DATA_DIKLAT
df3=db_data_diklat(diklat)
try:
    del dict
except:
    pass
batch2, batch_sqls2 = create_batch_data_2(df3)

#FOR TABLE PESERTA DIKLAT
#"-"

from psycopg2 import connect, Error
try:
    connection = connect(user = "",
                        password = "",
                        host = "",
                        port = "",
                        database = "",
                        options="-c search_path=")

    cursor = connection.cursor()
    print("created cursor object:", cursor)
    print("connected successfully")
except (Exception, Error) as err:
    print("pyscopg2 connect error:", err)
    connection = None
    cursor = None


# eksekusi batch sql diklat
c = 1
for s in batch:
    try:
        cursor.execute(s)
        connection.commit()
        print(s)

        print("query executed", c, "from", len(batch))
    except (Exception, Error) as e:
        print("error",e)     
    c+=1
    

# eksekusi batch sql data_diklat
d = 1
for p in batch2:
    try:
        cursor.execute(p)
        connection.commit()
        print(p)

        print("query executed", d, "from", len(batch2))
    except (Exception, Error) as e:
        print("error",e)     
    d+=1

cursor.close()
connection.close()
