# 🏗️ Concrete Compressive Strength Prediction

This project focuses on predicting the **compressive strength of concrete** using Machine Learning techniques.  
Concrete strength depends on several factors such as cement content, water ratio, and curing time. By applying data analysis and ML models, this project aims to estimate the compressive strength based on its mix composition.

---

## 📊 Project Overview
- **Objective:** Predict the compressive strength (MPa) of concrete.  
- **Dataset:** Kaggle Repository – *Concrete Compressive Strength Data Set*.  
- **Tools & Libraries:** Python, Pandas, NumPy, Scikit-learn, Matplotlib, Seaborn.  
- **Techniques Used:**  
  - Exploratory Data Analysis (EDA)  
  - Data preprocessing and feature scaling  
  - Machine Learning models (Random Forest Reggresor, Gradient Boosting Regressor)  
  - Hyperparameter tuning with `GridSearchCV`  
  - Model evaluation with cross-validation  

---

## ⚙️ Workflow
1. **Data Exploration & Cleaning**
   - Checked null values and dataset consistency.  
   - Analyzed correlations between concrete components and strength.  

2. **Feature Engineering**
   - Normalized and scaled features to improve model performance.  

3. **Modeling**
   - Trained and compared multiple regression models.  
   - Tuned hyperparameters using **GridSearchCV** (which applies cross-validation internally).  

4. **Evaluation**
   - Metrics: R², MAE.  
   - Selected the best-performing model based on predictive accuracy.  

---

## 📈 Results
- The best-performing model was **Random Forest Regressor** after hyperparameter tuning.  
- The model achieved strong predictive accuracy, capturing nonlinear relationships in the dataset.  

---

## 🚀 Technologies
- **Python**  
- **Pandas / NumPy** – Data manipulation  
- **Matplotlib / Seaborn** – Visualization  
- **Scikit-learn** – Machine Learning  
