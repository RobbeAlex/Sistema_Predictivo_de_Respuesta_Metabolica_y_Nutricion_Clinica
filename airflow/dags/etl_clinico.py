import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.types import Boolean

# Conexión apuntando al puente de Docker
cadena_conexion = 'postgresql+psycopg2://postgres:R0bb34l3x@host.docker.internal:5432/Sistema_Predictivo_de_Respuesta_Metabolica_y_Nutricion_Clinica?client_encoding=utf8'
engine = create_engine(cadena_conexion)

def ejecutar_etl():
    print("Iniciando proceso ETL...")
    
    # Asegurar que lea el CSV desde la misma carpeta donde está el script dentro de Docker
    ruta_base = os.path.dirname(os.path.abspath(__file__))
    ruta_csv = os.path.join(ruta_base, 'gastrointestinal_disease_dataset.csv')
    
    df_csv = pd.read_csv(ruta_csv, encoding='latin1')
 
    df_csv = df_csv.rename(columns={
        'Diet_Type': 'tipo_dieta', 'Disease_Class': 'clase_enfermedad', 'Age': 'edad',
        'Gender': 'genero', 'BMI': 'imc', 'Microbiome_Index': 'indice_microbioma',
        'CRP_ESR': 'proteina_c_reactiva', 'Fecal_Calprotectin': 'calprotectina_fecal',
        'Antibiotic_Use': 'uso_antibioticos'    
    })

    df_csv['uso_antibioticos'] = df_csv['uso_antibioticos'].astype(bool)

    print("Procesando catálogos y tablas...")
    df_dietas = pd.DataFrame(df_csv['tipo_dieta'].dropna().unique(), columns=['tipo_dieta'])
    df_dietas.to_sql('cat_dietas', engine, if_exists='append', index=False)
    
    df_enfermedades = pd.DataFrame(df_csv['clase_enfermedad'].dropna().unique(), columns=['clase_enfermedad'])
    df_enfermedades.to_sql('cat_enfermedades', engine, if_exists='append', index=False)
    
    db_dietas = pd.read_sql("SELECT id_dieta, tipo_dieta FROM cat_dietas", engine)
    db_enfermedades = pd.read_sql("SELECT id_enfermedad, clase_enfermedad FROM cat_enfermedades", engine)
    
    df_csv = df_csv.merge(db_dietas, on='tipo_dieta', how='left')
    df_csv = df_csv.merge(db_enfermedades, on='clase_enfermedad', how='left')

    df_pacientes = df_csv[['edad', 'genero', 'imc']].copy()
    df_pacientes.to_sql('pacientes', engine, if_exists='append', index=False)
    
    db_pacientes = pd.read_sql("SELECT id_paciente FROM pacientes ORDER BY id_paciente ASC", engine)
    df_csv['id_paciente'] = db_pacientes['id_paciente']

    df_registros = df_csv[[
        'id_paciente', 'id_dieta', 'id_enfermedad', 'indice_microbioma', 
        'proteina_c_reactiva', 'calprotectina_fecal', 'uso_antibioticos'
    ]].copy()
    
    df_registros.to_sql('registros_clinicos', engine, if_exists='append', index=False, dtype={'uso_antibioticos': Boolean})
    print("¡ETL completado exitosamente! Los datos están normalizados en SQL.")

if __name__ == "__main__":
    ejecutar_etl()
