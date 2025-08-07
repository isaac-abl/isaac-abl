from functions import join_data, elim_outliers, impute_missing_values, label_encoding



#Test 1

def test_join_pass(spark_session):
    print("Unit Test: join pass")
    estudiante_1 = [(1, 'Público', 'Alto'),
                (2, 'Privado', 'Medio'),
                (3, 'Privado', 'Alto'),
                (4, 'Público', 'Bajo')]

    estudiante_12 = [(1, 'Bajo', 85),
            (2, 'Medio', 60),
            (3, 'Alto', 30),
            (4, 'Medio', 100)]


    students_1 = spark_session.createDataFrame(estudiante_1, ['ID', 'Colegio', 'Ingresos_familiares'])
    students_2 = spark_session.createDataFrame(estudiante_12, ['ID', 'Calidad_profesor', 'Nota'])

    joined_df = join_data(students_1, students_2, "ID", "ID", "inner")

    print("Joined DataFrame:")
    joined_df.show()

    correct_df = spark_session.createDataFrame([(1, 'Público', 'Alto', 'Bajo', 85),
                                                (2, 'Privado', 'Medio', 'Medio', 60),
                                                (3, 'Privado', 'Alto', 'Alto', 30),
                                                (4, 'Público', 'Bajo', 'Medio', 100)],
                                               ['ID', 'Colegio', 'Ingresos_familiares', 'Calidad_profesor', 'Nota'])
    print("Correct DataFrame:")
    correct_df.show()

    assert joined_df.collect() == correct_df.collect()








#test 2

def test_elim_outliers_pass(spark_session):
    print("Unit Test: elim_outliers pass")

    students = [(1, "Alto", "2", 100),
                (2, "Bajo", "1", 130),
                (3, "Medio", "4", 90),
                (4, "Bajo", "1", 72),
                (5, "Medio", "3", -2)]

    df = spark_session.createDataFrame(students, ["ID", "Calidad_profesor", "Actividad_fisica", "Nota"])

    print("Dataframe calculado")
    df_calculado = elim_outliers(df, "Nota", 0, 100)
    df_calculado.show()


    print("Dataframe esperado")

    correct_df = spark_session.createDataFrame([(1, "Alto", "2", 100),
                                                (3, "Medio", "4", 90),
                                                (4, "Bajo", "1", 72)],
                                                 ["ID", "Calidad_profesor", "Actividad_fisica", "Nota"])
    correct_df.show()

    assert df_calculado.collect() == correct_df.collect()










#test 3

def test_impute_missing_values_pass(spark_session):
    print("Unit Test: elim_outliers pass")

    students = [(1, "Alto", 2, 100),
                (2, "Bajo", 1, 80),
                (3, "Medio", None, 90),
                (4, "Bajo", None, 72),
                (5, "Medio", 6, 80)]

    df = spark_session.createDataFrame(students, ["ID", "Calidad_profesor", "Actividad_fisica", "Nota"])

    print("Dataframe calculado")
    df_calculado = impute_missing_values(df, "Actividad_fisica", "median")
    df_calculado.show()


    print("Dataframe esperado")

    correct_df = spark_session.createDataFrame([(1, "Alto", 2, 100),
                                                (2, "Bajo", 1, 80),
                                                (3, "Medio", 2, 90),
                                                (4, "Bajo", 2, 72),
                                                (5, "Medio", 6, 80)],
                                                 ["ID", "Calidad_profesor", "Actividad_fisica", "Nota"])
    correct_df.show()

    assert df_calculado.collect() == correct_df.collect()













#test 4

def test_label_encoding_pass(spark_session):
    print("Unit Test: label_encoding pass")

    students = [(1, "Alto", 2, 100),
                (2, "Bajo", 1, 10),
                (3, "Medio", 5, 90),
                (4, "Bajo", 0, 72),
                (5, "Medio", 6, 80)]

    df = spark_session.createDataFrame(students, ["ID", "Calidad_profesor", "Actividad_fisica", "Nota"])

    print("Dataframe calculado")
    df_calculado = label_encoding(["Alto", "Medio", "Bajo"], ["3", "2", "1"], "Calidad_profesor", df)
    df_calculado.show()


    print("Dataframe esperado")

    correct_df = spark_session.createDataFrame([(1, 3, 2, 100),
                                                (2, 1, 1, 10),
                                                (3, 2, 5, 90),
                                                (4, 1, 0, 72),
                                                (5, 2, 6, 80)],
                                                 ["ID", "Calidad_profesor", "Actividad_fisica", "Nota"])
    correct_df.show()

    assert df_calculado.collect() == correct_df.collect()
















