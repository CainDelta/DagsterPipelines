import pandas as pd
from sqlalchemy import create_engine



sqlcon = create_engine('mssql+pyodbc://MIUser:B3nn3tt5!@bendb02.ukfast.bennetts.co.uk/Bennetts_MI?driver=SQL+Server')

riders = pd.read_sql('select * from Policies.T_Raw_Policies_Riders',sqlcon)
riders.drop('RDNAME', axis=1, inplace=True)
riders.to_csv('riders.csv',index=False)
