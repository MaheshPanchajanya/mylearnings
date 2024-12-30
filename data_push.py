import pandas  as pd
from sqlalchemy import create_engine


username ='root'
password = '1234'
conn = create_engine(f"mysql+mysqlconnector://{username}:{password}@localhost/data_warehouse")


df = pd.read_sql('select * from LZ_product',conn)

try:
	df.to_csv('test.csv',index=False)
except Exception as e:
	print(e) 
'''df = pd.read_csv('product.csv')

print(df)
print(df.drop_duplicates())'''

#df.replace({'QTY':0},'out of stock',inplace=True)
#df.dropna()
# df.drop(df.index[df['QTY']==0],inplace=True)
# print(df)
# df.to_sql('handson_pro',con=conn,if_exists='replace',index=False)



# import pandas as pd
# from sqlalchemy import create_engine

# try:
#     cnt = create_engine("mysql+mysqlconnector://root:1234@localhost/data_warehouse")
# except Exception as e:
#     print(e)

# df = pd.read_json("sample.json")
# # print(df)
# # print(df.info())
# # print(df.head(10))
# # print(df["language"])
# df.replace({'language':['Sindhi','Uyghur','isiZulu']},'Hindi',inplace=True)
# # print(df['language'])
# df.to_sql('language_table',con=cnt,if_exists='replace',index=False)




