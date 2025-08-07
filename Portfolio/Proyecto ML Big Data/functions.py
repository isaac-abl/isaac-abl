from pyspark.sql.types import IntegerType, StringType, StructType, StructField, FloatType, DateType
from pyspark.sql.functions import col, round
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, Imputer, OneHotEncoder 


#Cargar los datos
#recibe como parametros lo siguiente:
#path: nombre del archivo
#schema: el schema deseado
def load_data(path, schema, spark):
  df = spark.read.csv(path, header=True, schema=schema)
  return df



#une dos dataframes
#recibe como parametros lo siguiente:
#df1: el dataframe de datos 1
#df2: el dataframe de datos 2
#col1: columna de df1 por la cual hacer el join
#col2: columna de df2 por la cual hacer el join
#join_type: tipo de unión que se hará
def join_data(df1, df2, col1, col2, join_type):
  column = str(col2) + str("_1")
  df2 = df2.withColumnRenamed(col2, column)

  df = df1.join(df2, df1[col1] == df2[column], how= join_type)
  df = df.drop(column)
  return df





#elimina outliers filtrando datos
#recibe como parametros lo siguiente:
#dataframe: el dataframe de datos
#column: nombre la columna en donde se va a eliminar outliers
#min_val: int del valor minimo que puede contener la columna
#max_val: int del valor máximo que puede contener la columna
def elim_outliers(dataframe, column, min_val, max_val):
  df = dataframe.filter(col(column) >= min_val)
  df = df.filter(col(column) <= max_val)
  return df



#realiza un label encoding
#recibe como parametros lo siguiente:
#start: una lista con los valores a cambiar
#finish: una lista con los valores por los cuales se va a cambiar
#column: la columna en donde se van a cambiar datos
#dataframe: el dataframe de datos
def label_encoding(start, finish, column, dataframe):
  df = dataframe.replace(start, finish, column)
  df = df.withColumn(column, col(column).cast(IntegerType()))
  return df



#reemplaza valores faltantes
#recibe como parametros lo siguiente:
#df: el dataframe de datos
#cols: nombre de la columna con nulos
#strategy: elegir por qué estadístico sustituir los nulos
def impute_missing_values(df, cols, strategy):

  imputer = Imputer(strategy=strategy, inputCols=[cols], outputCols=[cols])
  df_imputed = imputer.fit(df).transform(df)


  df_model = df_imputed.withColumn(cols, round(col(cols)).cast(IntegerType()))

  return df_model





# Almacenar el conjunto de datos limpio en la base de datos
#recibe como parametros lo siguiente:
#dataframe: el dataframe de datos a guardar
#name: nombre con que desea guardar
def save_data(dataframe, name):
  dataframe \
      .write \
      .format("jdbc") \
      .mode('overwrite') \
      .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
      .option("user", "postgres") \
      .option("password", "testPassword") \
      .option("dbtable", name) \
      .save()




#realiza one hot encoding a una columna con variable categórica
#recibe como parametros lo siguiente:
#dataframe: el dataframe de datos
#cols: columna a realizar One-H-E
def one_hot_encoder(dataframe, column):
  indexer = StringIndexer(inputCol = column, outputCol = column + "_index")
  dataframe = indexer.fit(dataframe).transform(dataframe)

  encoder = OneHotEncoder(inputCols = [column + "_index"], outputCols = [column + "_vec"])
  dataframe = encoder.fit(dataframe).transform(dataframe)

  dataframe = dataframe.drop(column + "_index", column)

  return dataframe



    