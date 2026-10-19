import pandas as pd
import os
import joblib
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

cadena_conexion = 'postgresql+psycopg2://postgres:R0bb34l3x@host.docker.internal:5432/Sistema_Predictivo_de_Respuesta_Metabolica_y_Nutricion_Clinica?client_encoding=utf8'
engine = create_engine(cadena_conexion)

print("Ejecutando JOINs y extrayendo datos de PostgreSQL...")
consulta_sql = """
    SELECT p.edad, p.genero, p.imc, d.tipo_dieta, r.indice_microbioma, 
           r.proteina_c_reactiva, r.calprotectina_fecal, r.uso_antibioticos,
           e.clase_enfermedad AS target_enfermedad
    FROM registros_clinicos r
    INNER JOIN pacientes p ON r.id_paciente = p.id_paciente
    INNER JOIN cat_dietas d ON r.id_dieta = d.id_dieta
    INNER JOIN cat_enfermedades e ON r.id_enfermedad = e.id_enfermedad;
"""

df_ml = pd.read_sql(consulta_sql, engine)
df_ml_procesado = pd.get_dummies(df_ml, columns=['genero', 'tipo_dieta'], drop_first=True)
df_ml_procesado['uso_antibioticos'] = df_ml_procesado['uso_antibioticos'].astype(int)

X = df_ml_procesado.drop('target_enfermedad', axis=1)
y = df_ml_procesado['target_enfermedad']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print("Entrenando el modelo Random Forest Classifier...")
modelo_rf = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced', n_jobs=-1)
modelo_rf.fit(X_train, y_train)

# Exportar el modelo en la misma carpeta del script (la carpeta dags)
ruta_base = os.path.dirname(os.path.abspath(__file__))
joblib.dump(modelo_rf, os.path.join(ruta_base, 'modelo_metabolico.pkl'))
joblib.dump(X_train.columns.tolist(), os.path.join(ruta_base, 'columnas_entrenamiento.pkl'))
print("Modelo entrenado y exportado correctamente.")
