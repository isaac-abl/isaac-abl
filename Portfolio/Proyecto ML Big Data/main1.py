# Importar bibliotecas

import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, FloatType, DateType
from pyspark.sql.functions import col, round

from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, Imputer, OneHotEncoder
import sys 

from functions import load_data, join_data, elim_outliers, label_encoding, impute_missing_values, save_data, one_hot_encoder


# Crear la sesion

spark = SparkSession.builder.appName("Proyecto").getOrCreate()

# Inputs para dataframes

input_1 = sys.argv[1]
input_2 = sys.argv[2]


# Crear dataframes

#creamos la sesion
spark = SparkSession.builder.appName("Proyecto").getOrCreate()

#schema para el primer conjunto
schema_students_1  = StructType([StructField("ID", IntegerType(), True),
                                 StructField("Hours", IntegerType(), True),
                                 StructField("Attendance", IntegerType(), True),
                                 StructField("Parental_Involvement", StringType(), True),
                                 StructField("Access_to_Resources", StringType(), True),
                                 StructField("Extracurricular_Activities", StringType(), True),
                                 StructField("Sleep_Hours", IntegerType(), True),
                                 StructField("Previous_Scores", IntegerType(), True),
                                 StructField("Motivation_Level", StringType(), True),
                                 StructField("Internet_Access", StringType(), True),
                                 StructField("Tutorihg_Sessions", IntegerType(), True)
                                 ])

#schema para el segundo conjunto
schema_students_2 = StructType([StructField("ID", IntegerType(), True),
                                StructField("Family_Income", StringType(), True),
                                StructField("Teacher_Quality", StringType(), True),
                                StructField("School_Type", StringType(), True),
                                StructField("Peer_Influence", StringType(), True),
                                StructField("Physical_Activity", IntegerType(), True),
                                StructField("Learning_Disabilities", StringType(), True),
                                StructField("Parental_Education_Level", StringType(), True),
                                StructField("Distance_from_Home", StringType(), True),
                                StructField("Gender", StringType(), True),
                                StructField("Exam_Score", IntegerType(), True)
                                ])


students_1 = load_data("students_1.csv", schema_students_1, spark)
students_2 = load_data("students_2.csv", schema_students_2, spark)





    
# Preprocesado primer conjunto de datos..........................................................
    
#hacemos un label enconding para las categoricas ordinales

#Parental_Involvemente
students_1 = label_encoding(["High", "Medium", "Low"], ["3", "2", "1"], "Parental_Involvement", students_1)


#Access_to_Resources
students_1 = label_encoding(["High", "Medium", "Low"], ["3", "2", "1"], "Access_to_Resources", students_1)


#Extracurricular_Activities
students_1 = label_encoding(["Yes", "No"], ["1", "0"], "Extracurricular_Activities", students_1)


#Motivation_Level
students_1 = label_encoding(["High", "Medium", "Low"], ["3", "2", "1"], "Motivation_Level", students_1)


#Internet_Access
students_1 = label_encoding(["Yes", "No"], ["1", "0"], "Internet_Access", students_1)






# Preprocesado segundo conjunto de datos..........................................................

#se detecta outliers en exam_score en su valor max
students_2 = elim_outliers(students_2, "Exam_Score", 0, 100)




#realizamos label encoding para las categóricas ordinales

#Family_Income
students_2 = label_encoding(["High", "Medium", "Low"], ["3", "2", "1"], "Family_Income", students_2)


#Teacher_Quality
students_2 = label_encoding(["High", "Medium", "Low"], ["3", "2", "1"], "Teacher_Quality", students_2)


#Parental_Education_Level
students_2 = label_encoding(["Postgraduate", "College", "High School"], ["3", "2", "1"], "Parental_Education_Level", students_2)


#Distance_from_Home
students_2 = label_encoding(["Far", "Moderate", "Near"], ["3", "2", "1"], "Distance_from_Home", students_2)



#Gender
students_2 = label_encoding(["Female", "Male"], ["2", "1"], "Gender", students_2)



#Learning_Disabilities
students_2 = label_encoding(["Yes", "No"], ["1", "0"], "Learning_Disabilities", students_2)


#School_Type
students_2 = label_encoding(["Public", "Private"], ["1", "0"], "School_Type", students_2)




#Hay valores faltantes que se reemplazarán por la mediana
students_2 = impute_missing_values(students_2, "Teacher_Quality", "median")

students_2 = impute_missing_values(students_2, "Parental_Education_Level", "median")

students_2 = impute_missing_values(students_2, "Distance_from_Home", "median")




students_2.show()
students_1.show()

students_1 \
      .write \
      .format("jdbc") \
      .mode('overwrite') \
      .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
      .option("user", "postgres") \
      .option("password", "testPassword") \
      .option("dbtable", "name") \
      .save()


#Guardamos los datos limpios en postgres

save_data(students_1, "students_1_clean")

save_data(students_2, "students_2_clean")





