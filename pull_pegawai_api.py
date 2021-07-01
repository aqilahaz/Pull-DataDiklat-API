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

user = ''
password = ''

session = Session()
session.auth = HTTPBasicAuth(user, password)
#Fill the Link to WSDL
client = Client(#LINK TO WSDL',
    transport=Transport(session=session, operation_timeout=120 ))

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
        link = '#rest api'+key
        auth = HTTPBasicAuth('getdatasik', '123456')
        getListPegawai = requests.get(url=link, auth=auth, timeout=120)
        print("Status Request Data Pegawai:", getListPegawai.status_code)

        Dat = getListPegawai.json()
        if Dat['status']['statusCode'] == '1':
            y = Dat['dataPegawaiPerKantor']
            DatPegawai.append(y)
        else:
            print("....not found")
    return DatPegawai


def edit_data(data_pegawai):
    df=pd.DataFrame(data_pegawai)
    engine = create_engine(#CONNECT TO POSTGRES DATABASE', connect_args={'options': '-c search_path=aparatur'})
    sql2 = 'SELECT id as instansi_id, kode as kode_kantor FROM instansi'
    df2 = pd.read_sql(sql2, con=engine)
    df2 = df2.astype({"kode_kantor": str})
    res = pd.merge(df, df2, how='left', on="kode_kantor")
    res["date"]= pd.to_datetime(res["tmt_cpns"]) 
    time1 = datetime.datetime.now()
    res['tanggal']=(time1-res['date'])/np.timedelta64(1, 'Y')
    res['masa_kerja']=res['tanggal'].fillna(0).astype(np.int64)
    dropcol = [ 'unit', 'kode_unit', 'kode_unit_lama', 'kantor',
        'kode_kantor', 'kode_kantor_lama',  
            'jabatan_struktural', 'tmt_jabatan_struktural',
        'jabatan_fungsional', 'tmt_jabatan_fungsional',  'tmt_cpns',
        'tmt_pns', 'date','tanggal']
    res=res.drop(dropcol, axis=1)
    res.insert(9, 'status', 'ASN')
    res = res.replace(r'^\s*$', np.nan, regex=True)
    res = res.where(pd.notnull(res), None)
    nmfile = datetime.datetime.today().strftime('%Y-%m-%d')
    res.insert(2, 'last_updated', nmfile)
    res=res.rename(columns={"nip": "kode"})
    temp=res
    return temp

def pegawaiInsert(pegawai, col):
    tbl = Table("pegawai")
    d = list(pegawai.values())
    q = Query.into(tbl).columns(*col).insert(*d)
    pegawaii = str(q).replace('"','')
    return pegawaii


def pegawaiUpdate(pegawai, col):    
    tbl = Table("pegawai")
    d = list(pegawai.values())
    q = Query.into(tbl).columns(*col).insert(*d)
    upd = tbl.update()
    i = 0
    for c in col:
        upd = upd.set(c, d[i])
        i+=1
    upd = upd.where(tbl.kode == pegawai["kode"])
    pegawaii=str(upd).replace('"','')
    return pegawaii

#fill with unit list
kd = []

#execute function
items = request_data_kantor(kd)
data_kantor = flatten_list(items)
ArrKodeKantor = create_dataframe(data_kantor)
DatPegawai = request_data_pegawai(ArrKodeKantor)
data_pegawai = flatten_list(DatPegawai)
temp = edit_data(data_pegawai)

#initiate ----------------
abc = temp.values.tolist()
col = list(temp.columns)

batch_sqls = ""
batch = []
#-----------------------
print("Going to cek "+ str(len(temp)) + ' data')
c = 1
for index, val in enumerate(abc): 
    print(c)
    c+=1
    y = dict(zip(col, val))
    print(y['kode'])
    cek = pd.read_sql("select kode from pegawai where kode='%s'" %y['kode'] , engine)
    if cek.shape[0] > 0:
        print("update")
        s = pegawaiUpdate(y,col)
        batch.append(s)
        batch_sqls += s +";\n"
        continue
    print("insert")
    s = pegawaiInsert(y,col)
    batch.append(s)
    batch_sqls += s +";\n"


print(str(len(temp)) + ' data checked')

#eksekusi batch sql
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


# eksekusi batch sql
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

cursor.close()
connection.close()
