import streamlit as st
import requests

st.title("🩺 Sistema Predictivo de Respuesta Metabólica")
st.markdown("Ingresa los biomarcadores y el plan nutricional del paciente para predecir el diagnóstico gastrointestinal.")

# 1. Crear el formulario de entrada en la interfaz
col1, col2 = st.columns(2)

with col1:
    edad = st.number_input("Edad", min_value=1, max_value=120, value=30)
    imc = st.number_input("Índice de Masa Corporal (IMC)", min_value=10.0, max_value=50.0, value=25.0)
    indice_microbioma = st.slider("Índice de Microbioma", 0.0, 1.0, 0.5)
    uso_antibioticos = st.selectbox("¿Uso reciente de antibióticos?", ["Sí", "No"])

with col2:
    proteina_c_reactiva = st.number_input("Proteína C Reactiva (mg/L)", min_value=0.0, max_value=200.0, value=10.0)
    calprotectina_fecal = st.number_input("Calprotectina Fecal (μg/g)", min_value=0.0, max_value=1000.0, value=50.0)
    genero = st.selectbox("Género", ["Male", "Female"])
    tipo_dieta = st.selectbox("Tipo de Dieta", ["Vegan", "Vegetarian", "Omnivore", "Pescatarian", "Standard"])

# 2. Enviar los datos a la API al hacer clic en el botón
if st.button("Generar Predicción Clínica"):
    
    # Preparar el paquete JSON exactamente con los nombres que espera Pydantic en FastAPI
    datos_paciente = {
        "edad": edad,
        "imc": imc,
        "indice_microbioma": indice_microbioma,
        "proteina_c_reactiva": proteina_c_reactiva,
        "calprotectina_fecal": calprotectina_fecal,
        "uso_antibioticos": True if uso_antibioticos == "Sí" else False,
        "genero": genero,
        "tipo_dieta": tipo_dieta
    }
    
    # Hacer la petición POST a la API
    try:
        respuesta = requests.post("http://127.0.0.1:8000/predecir_diagnostico/", json=datos_paciente)
        
        if respuesta.status_code == 200:
            # Extraer la predicción del JSON de respuesta
            resultado = respuesta.json()
            diagnostico = resultado["prediccion_clinica"]
            
            st.success("Análisis clínico completado exitosamente vía MLOps")
            st.metric(label="Diagnóstico Previsto", value=diagnostico)
        else:
            st.error(f"Error del servidor API: {respuesta.status_code} - {respuesta.text}")
            
    except requests.exceptions.ConnectionError:
        st.error("🚨 Error de conexión: Asegúrate de que la API de FastAPI esté encendida (uvicorn) en el puerto 8000.")