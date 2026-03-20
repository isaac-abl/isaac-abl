# 🏗️ Predicción de la expectativa de vida

Este proyecto se enfoca en realizar predicciones sobre la expectativa de vida de los países usando técnicas de aprendizaje automático.
El set de datos contiene diversas característiicas tales como la tasa de mortalidad, prevalencia de diabetes, gasto nacional en salud, entre otros. Este proyecto busca predecir la expectativa de vida basado en 
información sobre los países considerados.

---

## 📊 Vista general
- **Objetivo:** Predecir la expectativa de vida en los países considerados.  
- **Dataset:** Google Repository – *Health*.  
- **Herramientas y librerías:** Python, Pandas, NumPy, Scikit-learn, Matplotlib, Seaborn.  
- **Técnicas usadas:**  
  - Exploratory Data Analysis (EDA)  
  - Preprocesamiento de datos.  
  - Modelos de aprendizaje automático (Gradient Boosting Regressor, Support Vector Regression, Linear Regression, Random Forest)  
  - Evaluación de modelos con Mean Absolute Error y Median Absolute Error.

---

## ⚙️ Flujo de trabajo
1. **Exploración de datos y limpieza**
   - Comprobación de valores nulos y consistencia del set de datos.  
   - Se analizó la correlación entre las características y la variable objetivo
     para eliminar características que aporten poca información.  

2. **Ingeniería de características**
   - Se escalaron los datos para un mejor rendimiento, especialmente en el SVR que es un modelo que depende
     altamente de la escala de los datos. Los árboles de decisión no. 

3. **Modelado**
   - Se entrenó múltiples modelos de regresión para comparar su desempeño.  
   - Se modificaron hiperparámetros con un conjunto de opciones para hallar la mejor combinación. 

4. **Evaluación**
   - Métricas: MAE, MAD
   - Se seleccionó el mejor modelo basado en los mejores resultados evitando el sobreajuste.  

---

## 📈 Resultados
- El modelo que mejor generaliza en este caso es el Gradient Boosting Regressor.  
- El mejor modelo consiguió errores bastante bajos, capturando relaciones complejas en los datos.

---

## 🚀 Tecnologías
- **Python**  
- **Pandas / NumPy** – Manipulación de datos
- **Matplotlib / Seaborn** – Visualización 
- **Scikit-learn** – Aprendizaje automático  
