# 📊 Diagramas de Flujo, Arquitectura y UML

Este documento contiene la representación visual completa del **Sistema Predictivo de Respuesta Metabólica y Nutrición Clínica** utilizando sintaxis **Mermaid** (compatible de forma nativa con el renderizador de GitHub).

---

## Índice de Diagramas
1. [Diagrama de Arquitectura e Infraestructura Híbrida (Componentes y Despliegue)](#1-diagrama-de-arquitectura-e-infraestructura-híbrida-componentes-y-despliegue)
2. [Diagrama Entidad-Relación (ERD - Base de Datos PostgreSQL)](#2-diagrama-entidad-relación-erd---base-de-datos-postgresql)
3. [Diagrama de Flujo del Pipeline MLOps (Orquestación en Apache Airflow)](#3-diagrama-de-flujo-del-pipeline-mlops-orquestación-en-apache-airflow)
4. [Diagrama de Flujo Detallado del Proceso ETL (`etl_clinico.py`)](#4-diagrama-de-flujo-detallado-del-proceso-etl-etl_clinicopy)
5. [Diagrama de Flujo de Entrenamiento e Inferencia ML](#5-diagrama-de-flujo-de-entrenamiento-e-inferencia-ml)
6. [Diagrama de Secuencia UML: Inferencia Clínica en Tiempo Real](#6-diagrama-de-secuencia-uml-inferencia-clínica-en-tiempo-real)
7. [Diagrama de Secuencia UML: Ciclo Automatizado MLOps Nocturno](#7-diagrama-de-secuencia-uml-ciclo-automatizado-mlops-nocturno)
8. [Diagrama de Clases y Módulos UML](#8-diagrama-de-clases-y-módulos-uml)
9. [Diagrama de Estados UML: Ciclo de Vida del Modelo Predictivo](#9-diagrama-de-estados-uml-ciclo-de-vida-del-modelo-predictivo)

---

## 1. Diagrama de Arquitectura e Infraestructura Híbrida (Componentes y Despliegue)

Representa la topología física y lógica del sistema distribuido entre **Windows Host**, **WSL2 (Ubuntu)** y **Docker Engine**.

```mermaid
flowchart TB
    subgraph WindowsHost["🖥️ Sistema Anfitrión: Windows"]
        direction TB
        User(["👨‍⚕️ Médico / Nutriólogo"])
        Streamlit["🎨 Frontend Clínico\napp.py (Streamlit :8501)"]
        FastAPI["⚡ Backend RESTful API\napi_clinica.py (FastAPI / Uvicorn :8000)"]
        PostgresHost[("🐘 PostgreSQL Server (:5432)\nDB: Sistema_Predictivo_de_Respuesta_\nMetabolica_y_Nutricion_Clinica")]
        Notebook["📓 Entorno Exploratorio\nscript.ipynb (Jupyter)"]
    end

    subgraph WSL2["🐧 Subsistema Linux: WSL2 (Ubuntu)"]
        direction TB
        subgraph SharedVol["📂 Volumen Compartido (/home/user/airflow/dags)"]
            CSV[📄 gastrointestinal_disease_dataset.csv]
            PKL_Model[🧠 modelo_metabolico.pkl]
            PKL_Cols[📋 columnas_entrenamiento.pkl]
        end

        subgraph DockerCluster["🐳 Clúster Docker (docker-compose.yaml)"]
            direction TB
            Scheduler["⏱️ Airflow Scheduler\nCron: 0 3 * * * (03:00 AM)"]
            Redis[("🔴 Redis Broker (:6379)")]
            AirflowDB[("🗄️ Postgres Metadata DB")]
            
            subgraph CeleryWorker["⚙️ Airflow Celery Worker (/opt/airflow/dags)"]
                Task1["Tarea 1: ejecutar_etl_postgresql\n(etl_clinico.py)"]
                Task2["Tarea 2: reentrenar_random_forest\n(entrenamiento_ml.py)"]
                Task3["Tarea 3: recargar_api_fastapi\n(curl POST /recargar_modelo)"]
                Task1 --> Task2 --> Task3
            end
            
            Scheduler --> Redis --> CeleryWorker
            Scheduler -.-> AirflowDB
        end
    end

    %% Relaciones e Interacciones
    User -->|"Ingresa biomarcadores"| Streamlit
    Streamlit -->|"HTTP POST /predecir_diagnostico/\n(JSON Payload)"| FastAPI
    FastAPI -->|"Respuesta JSON\n(Diagnóstico Previsto)"| Streamlit

    CSV -->|"Lectura CSV (latin1)"| Task1
    Task1 -->|"SQLAlchemy INSERT\n(host.docker.internal:5432)"| PostgresHost
    PostgresHost -->|"INNER JOIN Query\n(30,560 registros)"| Task2
    Task2 -->|"Exporta .pkl (joblib)"| PKL_Model
    Task2 -->|"Exporta .pkl (joblib)"| PKL_Cols
    
    PKL_Model -.->|"Carga en memoria vía UNC\n(\\wsl$\Ubuntu\home\user\airflow\dags)"| FastAPI
    PKL_Cols -.->|"Carga en memoria vía UNC\n(\\wsl$\Ubuntu\home\user\airflow\dags)"| FastAPI
    Task3 -->|"Webhook HTTP POST\n(host.docker.internal:8000)"| FastAPI
    Notebook -.->|"Experimentación y Métricas"| PostgresHost
```

> [!NOTE]
> **Nota de configuración**: En el diagrama anterior, `user` representa el nombre del directorio del usuario dentro de WSL2 (`Ubuntu`) donde se monta el volumen de ejecución y exportación de resultados (`/home/user/airflow/dags`).

---

## 2. Diagrama Entidad-Relación (ERD - Base de Datos PostgreSQL)

Modelo relacional en Estrella / 3ra Forma Normal implementado en `Database/Sistema_Predictivo_de_Respuesta_Metabolica_y_Nutricion_Clinica.sql`.

```mermaid
erDiagram
    PACIENTES ||--o{ REGISTROS_CLINICOS : "tiene (ON DELETE CASCADE)"
    CAT_DIETAS ||--o{ REGISTROS_CLINICOS : "clasifica"
    CAT_ENFERMEDADES ||--o{ REGISTROS_CLINICOS : "diagnostica"

    CAT_DIETAS {
        SERIAL id_dieta PK "Identificador único de dieta"
        VARCHAR(50) tipo_dieta UK "NOT NULL (Vegan, Omnivore, etc.)"
    }

    CAT_ENFERMEDADES {
        SERIAL id_enfermedad PK "Identificador único de enfermedad"
        VARCHAR(100) clase_enfermedad UK "NOT NULL (Variable Target)"
    }

    PACIENTES {
        SERIAL id_paciente PK "Identificador único del paciente"
        INT edad "CHECK (edad >= 0)"
        VARCHAR(20) genero "Male / Female"
        DECIMAL(5_2) imc "Índice de Masa Corporal"
    }

    REGISTROS_CLINICOS {
        SERIAL id_registro PK "Identificador del historial clínico"
        INT id_paciente FK "NOT NULL -> Pacientes"
        INT id_dieta FK "NOT NULL -> Cat_Dietas"
        INT id_enfermedad FK "NOT NULL -> Cat_Enfermedades"
        DECIMAL(6_3) indice_microbioma "Diversidad microbiana (0.000 - 1.000)"
        DECIMAL(6_2) proteina_c_reactiva "Marcador inflamatorio CRP (mg/L)"
        DECIMAL(6_2) calprotectina_fecal "Marcador intestinal (ug/g)"
        BOOLEAN uso_antibioticos "Administración reciente (True/False)"
        DATE fecha_registro "DEFAULT CURRENT_DATE"
    }
```

---

## 3. Diagrama de Flujo del Pipeline MLOps (Orquestación en Apache Airflow)

Representa la lógica de control, programación y tolerancia a fallos definida en `Para_Linux/airflow/dags/mlops_clinico_pipeline.py`.

```mermaid
flowchart TD
    Start(["🕒 Trigger Cron Diario: 03:00 AM\nDAG: pipeline_predictivo_metabolico"]) --> T1

    subgraph AirflowDAG["Orquestación Apache Airflow (CeleryExecutor)"]
        T1["📥 Tarea 1: ejecutar_etl_postgresql\nComando: python /opt/airflow/dags/etl_clinico.py"]
        T1_Check{"¿Ejecución\nExitosa?"}
        T1_Retry["⏳ Política de Reintento\nretries: 2 | retry_delay: 5 min"]
        
        T2["🌲 Tarea 2: reentrenar_random_forest\nComando: python /opt/airflow/dags/entrenamiento_ml.py"]
        T2_Check{"¿Entrenamiento\nExitoso?"}
        T2_Retry["⏳ Política de Reintento\nretries: 2 | retry_delay: 5 min"]

        T3["🔄 Tarea 3: recargar_api_fastapi\nComando: curl -X POST http://host.docker.internal:8000/recargar_modelo || true"]
        
        Alert["📧 Alerta por Correo\nemail_on_failure: True"]
    end

    T1 --> T1_Check
    T1_Check -- "Sí (Exit Code 0)" --> T2
    T1_Check -- "No (Error)" --> T1_Retry
    T1_Retry -- "Reintentos <= 2" --> T1
    T1_Retry -- "Reintentos Agotados" --> Alert

    T2 --> T2_Check
    T2_Check -- "Sí (Artefactos .pkl creados)" --> T3
    T2_Check -- "No (Error)" --> T2_Retry
    T2_Retry -- "Reintentos <= 2" --> T2
    T2_Retry -- "Reintentos Agotados" --> Alert

    T3 --> End(["✅ Pipeline MLOps Finalizado\nModelo en Producción Actualizado"])
```

---

## 4. Diagrama de Flujo Detallado del Proceso ETL (`etl_clinico.py`)

Describe paso a paso la transformación desde el archivo CSV crudo hasta las tablas relacionales en PostgreSQL.

```mermaid
flowchart TD
    A(["Inicio: ejecutar_etl()"]) --> B["Leer gastrointestinal_disease_dataset.csv\ncon encoding='latin1'"]
    B --> C["Renombrar 9 columnas de inglés a español\n(Diet_Type -> tipo_dieta, CRP_ESR -> proteina_c_reactiva, etc.)"]
    C --> D["Convertir columna 'uso_antibioticos'\nexplícitamente a tipo booleano (.astype(bool))"]
    
    D --> E["Extraer valores únicos de 'tipo_dieta'\ne insertar en tabla SQL 'cat_dietas'"]
    E --> F["Extraer valores únicos de 'clase_enfermedad'\ne insertar en tabla SQL 'cat_enfermedades'"]
    
    F --> G["Consultar 'id_dieta' e 'id_enfermedad'\ngenerados en PostgreSQL (pd.read_sql)"]
    G --> H["Cruzar IDs con DataFrame maestro\nmediante LEFT JOIN (pd.merge)"]
    
    H --> I["Extraer ['edad', 'genero', 'imc']\ne insertar en tabla SQL 'pacientes'"]
    I --> J["Recuperar secuencia 'id_paciente'\nordenada por ID ascendente y asignar al DataFrame"]
    
    J --> K["Construir DataFrame 'df_registros' con FKs\ny biomarcadores clínicos"]
    K --> L["Insertar en tabla SQL 'registros_clinicos'\nforzando dtype={'uso_antibioticos': Boolean}"]
    L --> M(["Fin: Datos Normalizados en PostgreSQL"])
```

---

## 5. Diagrama de Flujo de Entrenamiento e Inferencia ML

Muestra cómo se procesan las características tanto en la fase de entrenamiento (`entrenamiento_ml.py`) como en la fase de inferencia (`api_clinica.py`), garantizando consistencia dimensional.

```mermaid
flowchart LR
    subgraph FaseEntrenamiento["🏋️ Fase de Entrenamiento (entrenamiento_ml.py)"]
        direction TB
        E1[("PostgreSQL\n4 Tablas Normalizadas")] -->|"Triple INNER JOIN"| E2["DataFrame Consolidado\n(30,560 filas x 9 cols)"]
        E2 --> E3["One-Hot Encoding\npd.get_dummies(drop_first=True)\n+ uso_antibioticos.astype(int)"]
        E3 --> E4["Separación X (10 features)\ny (target_enfermedad - 6 clases)"]
        E4 --> E5["Train / Test Split (80% / 20%)\nTrain: 24,448 | Test: 6,112"]
        E5 --> E6["RandomForestClassifier\n(n_estimators=100, class_weight='balanced')"]
        E6 --> E7(["💾 Exportación Joblib:\n1. modelo_metabolico.pkl\n2. columnas_entrenamiento.pkl"])
    end

    subgraph FaseInferencia["⚡ Fase de Inferencia en Producción (api_clinica.py)"]
        direction TB
        I1(["JSON de Entrada (1 Paciente)\nValidado por Pydantic"]) --> I2["Conversión a DataFrame (1 fila)\ny uso_antibioticos.astype(int)"]
        I2 --> I3["One-Hot Encoding Dinámico\npd.get_dummies(datos_crudos)"]
        I3 --> I4["Alineación Estructural de Columnas\n.reindex(columns=columnas_entrenamiento, fill_value=0)"]
        I4 --> I5["Inferencia del Modelo\nmodelo.predict(datos_finales)[0]"]
        I5 --> I6(["Respuesta JSON:\nprediccion_clinica"])
    end

    E7 -.->|"Sincronización de Artefactos"| I4
    E7 -.->|"Carga del Modelo"| I5
```

---

## 6. Diagrama de Secuencia UML: Inferencia Clínica en Tiempo Real

Detalla el intercambio de mensajes síncronos cuando un profesional de la salud solicita una predicción desde la interfaz web.

```mermaid
sequenceDiagram
    autonumber
    actor Medico as 👨‍⚕️ Médico / Usuario
    participant UI as 🎨 Streamlit UI (app.py)
    participant API as ⚡ FastAPI (api_clinica.py)
    participant Pydantic as 🛡️ Esquema PacienteInput
    participant Pandas as 🐼 Preprocesador Pandas
    participant RF as 🧠 RandomForest (.pkl)

    Medico->>UI: Ingresa biomarcadores, dieta y datos del paciente
    Medico->>UI: Clic en "Generar Predicción Clínica"
    UI->>UI: Construye diccionario JSON (uso_antibioticos: Sí -> True)
    UI->>API: POST http://127.0.0.1:8000/predecir_diagnostico/ (JSON)
    
    activate API
    API->>Pydantic: Validar tipos y rangos clínicos (edad > 0, imc > 10, etc.)
    alt Datos Inválidos
        Pydantic-->>API: ValidationError (422 Unprocessable Entity)
        API-->>UI: HTTP 422 Error Detail
        UI-->>Medico: Muestra st.error() con detalle del fallo
    else Datos Válidos
        Pydantic-->>API: Instancia PacienteInput validada
        API->>Pandas: pd.DataFrame([paciente.dict()])
        Pandas->>Pandas: uso_antibioticos.astype(int) + pd.get_dummies()
        Pandas->>Pandas: reindex(columns=columnas_entrenamiento, fill_value=0)
        Pandas-->>API: Matriz datos_finales (1 x 10 alineada)
        
        API->>RF: modelo.predict(datos_finales)[0]
        activate RF
        RF-->>API: Clase predicha (ej. "Abdominal cramps or pain")
        deactivate RF
        
        API-->>UI: HTTP 200 OK {"status": "success", "prediccion_clinica": "..."}
        UI-->>Medico: Muestra st.success() y tarjeta st.metric() con el diagnóstico
    end
    deactivate API
```

---

## 7. Diagrama de Secuencia UML: Ciclo Automatizado MLOps Nocturno

Detalla la interacción desatendida entre los servicios de Airflow en Docker, la base de datos PostgreSQL y la API de producción.

```mermaid
sequenceDiagram
    autonumber
    participant Cron as ⏱️ Airflow Scheduler
    participant Worker as ⚙️ Celery Worker (Docker)
    participant ETL as 🐍 etl_clinico.py
    participant DB as 🐘 PostgreSQL (:5432)
    participant Train as 🐍 entrenamiento_ml.py
    participant FS as 📂 Volumen WSL2 (/dags)
    participant API as ⚡ FastAPI (:8000)

    Cron->>Worker: Dispara DAG "pipeline_predictivo_metabolico" (03:00 AM)
    activate Worker
    
    Note over Worker,DB: Etapa 1: Proceso ETL Clínico
    Worker->>ETL: Ejecuta tarea "ejecutar_etl_postgresql"
    activate ETL
    ETL->>FS: Lee gastrointestinal_disease_dataset.csv (latin1)
    ETL->>DB: Inserta cat_dietas y cat_enfermedades
    DB-->>ETL: Retorna id_dieta e id_enfermedad
    ETL->>DB: Inserta pacientes y recupera id_paciente
    ETL->>DB: Inserta registros_clinicos normalizados
    ETL-->>Worker: Exit Code 0 (ETL Completado)
    deactivate ETL

    Note over Worker,FS: Etapa 2: Reentrenamiento del Modelo ML
    Worker->>Train: Ejecuta tarea "reentrenar_random_forest"
    activate Train
    Train->>DB: SELECT con INNER JOIN (registros + pacientes + dietas + enfermedades)
    DB-->>Train: Retorna DataFrame consolidado
    Train->>Train: One-Hot Encoding + train_test_split + RandomForest.fit()
    Train->>FS: Sobrescribe modelo_metabolico.pkl y columnas_entrenamiento.pkl
    Train-->>Worker: Exit Code 0 (Modelo Exportado)
    deactivate Train

    Note over Worker,API: Etapa 3: Notificación a Producción
    Worker->>API: curl -X POST http://host.docker.internal:8000/recargar_modelo
    API-->>Worker: Respuesta HTTP (o continúa vía || true)
    deactivate Worker
```

---

## 8. Diagrama de Clases y Módulos UML

Estructura orientada a objetos y modular de los scripts que conforman el sistema.

```mermaid
classDiagram
    class PacienteInput {
        <<Pydantic BaseModel>>
        +int edad
        +float imc
        +float indice_microbioma
        +float proteina_c_reactiva
        +float calprotectina_fecal
        +bool uso_antibioticos
        +str genero
        +str tipo_dieta
        +dict() dict
    }

    class APIClinica {
        <<FastAPI Module: api_clinica.py>>
        +FastAPI app
        +str ruta_wsl
        +RandomForestClassifier modelo
        +list~str~ columnas_entrenamiento
        +predecir_diagnostico(paciente: PacienteInput) dict
    }

    class StreamlitApp {
        <<Frontend Module: app.py>>
        +int edad
        +float imc
        +float indice_microbioma
        +str uso_antibioticos
        +float proteina_c_reactiva
        +float calprotectina_fecal
        +str genero
        +str tipo_dieta
        +renderizar_formulario() void
        +enviar_peticion_post(datos_paciente: dict) Response
    }

    class ETLClinico {
        <<Data Pipeline Module: etl_clinico.py>>
        +str cadena_conexion
        +Engine engine
        +ejecutar_etl() void
    }

    class EntrenamientoML {
        <<ML Training Module: entrenamiento_ml.py>>
        +str cadena_conexion
        +Engine engine
        +str consulta_sql
        +RandomForestClassifier modelo_rf
        +entrenar_y_exportar() void
    }

    class MLOpsClinicoPipelineDAG {
        <<Airflow DAG: mlops_clinico_pipeline.py>>
        +dict default_args
        +DAG dag
        +BashOperator tarea_etl_sql
        +BashOperator tarea_entrenamiento_ml
        +BashOperator tarea_recargar_api
    }

    StreamlitApp ..> APIClinica : "HTTP POST JSON"
    APIClinica *-- PacienteInput : "valida entrada con"
    MLOpsClinicoPipelineDAG --> ETLClinico : "orquesta (Tarea 1)"
    MLOpsClinicoPipelineDAG --> EntrenamientoML : "orquesta (Tarea 2)"
    MLOpsClinicoPipelineDAG ..> APIClinica : "notifica recarga (Tarea 3)"
    EntrenamientoML ..> APIClinica : "provee artefactos .pkl"
```

---

## 9. Diagrama de Estados UML: Ciclo de Vida del Modelo Predictivo

Representa las transiciones de estado por las que atraviesan los datos y el modelo dentro del flujo continuo MLOps.

```mermaid
stateDiagram-v2
    [*] --> DatosCrudosCSV : Llegada de lote clínico (.csv)
    
    DatosCrudosCSV --> NormalizandoEnSQL : Airflow inicia ejecutar_etl_postgresql
    NormalizandoEnSQL --> AlmacenadoRelacional : Tablas Catálogos, Pacientes y Registros pobladas
    NormalizandoEnSQL --> ErrorETL : Fallo de conexión / codificación
    ErrorETL --> NormalizandoEnSQL : Reintento automático (máx 2)

    AlmacenadoRelacional --> PreprocesandoFeatures : Airflow inicia reentrenar_random_forest
    PreprocesandoFeatures --> EntrenandoBosqueAleatorio : INNER JOIN + One-Hot Encoding (10 columnas)
    EntrenandoBosqueAleatorio --> ArtefactosSerializados : joblib.dump(.pkl) en volumen compartido

    ArtefactosSerializados --> ModeloEnMemoriaAPI : FastAPI carga modelo_metabolico.pkl
    
    state ModeloEnMemoriaAPI {
        [*] --> EsperandoPeticion
        EsperandoPeticion --> ValidandoPayload : POST /predecir_diagnostico/
        ValidandoPayload --> AlineandoColumnas : Pydantic OK
        ValidandoPayload --> EsperandoPeticion : Error 422
        AlineandoColumnas --> EjecutandoInferencia : reindex(fill_value=0)
        EjecutandoInferencia --> EsperandoPeticion : Retorna JSON 200 OK
    }

    ModeloEnMemoriaAPI -->NormalizandoEnSQL : Nuevo ciclo Cron 03:00 AM
```
