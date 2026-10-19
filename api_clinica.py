from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import pandas as pd
import joblib
import uvicorn
import os

# 1. Inicializar la aplicación FastAPI
app = FastAPI(
    title="API de Predicción Gastrointestinal",
    description="API RESTful para predecir enfermedades metabólicas y gastrointestinales usando Machine Learning.",
    version="1.0.0"
)

# 2. Cargar el modelo y las columnas en la memoria al iniciar el servidor
# Definir la ruta de red de Windows hacia la carpeta de Airflow en WSL
ruta_wsl = r"\\wsl$\Ubuntu\home\robbealex\airflow\dags"

ruta_modelo = os.path.join(ruta_wsl, 'modelo_metabolico.pkl')
ruta_columnas = os.path.join(ruta_wsl, 'columnas_entrenamiento.pkl')

# 2. Cargar el modelo desde el cerebro de Airflow
try:
    modelo = joblib.load(ruta_modelo)
    columnas_entrenamiento = joblib.load(ruta_columnas)
    print(f"Modelo cargado exitosamente desde: {ruta_wsl}")
except FileNotFoundError:
    raise RuntimeError(f"No se encontraron los .pkl en {ruta_wsl}. Revisa que el DAG haya terminado.")

# 3. Definir el esquema de validación de datos usando Pydantic
class PacienteInput(BaseModel):
    edad: int = Field(..., gt=0, description="Edad del paciente en años")
    imc: float = Field(..., gt=10.0, description="Índice de Masa Corporal")
    indice_microbioma: float = Field(..., ge=0.0, le=1.0, description="Índice de diversidad del microbioma (0.0 a 1.0)")
    proteina_c_reactiva: float = Field(..., ge=0.0, description="Niveles de PCR en mg/L")
    calprotectina_fecal: float = Field(..., ge=0.0, description="Niveles de Calprotectina en μg/g")
    uso_antibioticos: bool = Field(..., description="¿Ha usado antibióticos recientemente? (true/false)")
    genero: str = Field(..., description="Género del paciente (ej. Male, Female)")
    tipo_dieta: str = Field(..., description="Tipo de dieta (ej. Vegan, Omnivore, Standard)")

# 4. Crear el Endpoint predictivo (Método POST)
@app.post("/predecir_diagnostico/")
def predecir_diagnostico(paciente: PacienteInput):
    try:
        # Convertir el JSON validado por Pydantic a un diccionario y luego a DataFrame (1 fila)
        datos_crudos = pd.DataFrame([paciente.dict()])
        
        # Convertir el booleano a entero (1/0) tal como lo espera el modelo
        datos_crudos['uso_antibioticos'] = datos_crudos['uso_antibioticos'].astype(int)
        
        # Aplicar One-Hot Encoding a las variables categóricas (género y dieta)
        datos_procesados = pd.get_dummies(datos_crudos)
        
        # Alinear las columnas con las del entrenamiento (rellenando con 0 las categorías faltantes)
        datos_finales = datos_procesados.reindex(columns=columnas_entrenamiento, fill_value=0)
        
        # Ejecutar la predicción
        prediccion = modelo.predict(datos_finales)[0]
        
        # Devolver la respuesta en formato JSON
        return {
            "status": "success",
            "modelo_utilizado": "RandomForestClassifier",
            "prediccion_clinica": str(prediccion)
        }
        
    except Exception as e:
        # Manejo de errores de servidor
        raise HTTPException(status_code=500, detail=f"Error al procesar la predicción: {str(e)}")

# Bloque de ejecución para levantar el servidor
if __name__ == "__main__":
    # Levanta el servidor asíncrono en el puerto 8000
    uvicorn.run("api_clinica:app", host="127.0.0.1", port=8000, reload=True)