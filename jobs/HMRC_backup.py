import pandas as pd
from sqlalchemy import create_engine
from credentials import dbstring

sqlcon = create_engine(dbstring,fast_executemany=True)


sanctions = pd.read_csv(r"J:\Data and Analytics\Data\HMRC Sanctions Lists\Latest\sanctionsconlist.txt",skiprows=1,sep=';')
#sancfields = list(pd.read_sql('select * from Misc.T_HMRC_Sanctions_List',sqlcon))

fields = ['Name 6',
 'Name 1',
 'Name 2',
 'Name 3',
 'Name 4',
 'Name 5',
 'Title',
 'DOB',
 'Town of Birth',
 'Country of Birth',
 'Nationality',
 'Passport Details',
 'National Identification Number',
 'Position',
 'Address 1',
 'Address 2',
 'Address 3',
 'Address 4',
 'Address 5',
 'Address 6',
 'Post/Zip Code',
 'Country',
 'Other Information',
 'Group Type',
 'Alias Type',
 'Regime',
 'Listed On',
 'Last Updated',
 'Group ID']
sanctions = sanctions[fields]
sanctions.rename(columns={'National Identification Number':'NI Number'},inplace=True)
list(sanctions)


sanctions.to_sql('T_HMRC_Sanctions_List', sqlcon,method='multi', index=False, if_exists="append", schema="Misc",chunksize=50)
# sancfields = list(pd.read_sql('select * from Misc.T_HMRC_Sanctions_List',sqlcon))
# sancfields
# list(sanctions)
