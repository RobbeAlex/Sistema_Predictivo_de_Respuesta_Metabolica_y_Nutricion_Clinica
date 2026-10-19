# 📘 Documentación Técnica: Sistema Predictivo de Respuesta Metabólica y Nutrición Clínica

---

## Tabla de Contenidos
1. [Visión General del Sistema](#1-visión-general-del-sistema)
2. [Arquitectura de Infraestructura Híbrida (Windows + WSL2 + Docker)](#2-arquitectura-de-infraestructura-híbrida-windows--wsl2--docker)
3. [Ingeniería de Datos y Modelo Relacional (PostgreSQL)](#3-ingeniería-de-datos-y-modelo-relacional-postgresql)
4. [Pipeline ETL Automatizado (`etl_clinico.py`)](#4-pipeline-etl-automatizado-etl_clinicopy)
5. [Pipeline de Machine Learning y Feature Engineering (`entrenamiento_ml.py`)](#5-pipeline-de-machine-learning-y-feature-engineering-entrenamiento_mlpy)
6. [Orquestación MLOps con Apache Airflow (`mlops_clinico_pipeline.py`)](#6-orquestación-mlops-con-apache-airflow-mlops_clinico_pipelinepy)
7. [Especificación del Backend API RESTful (`api_clinica.py`)](#7-especificación-del-backend-api-restful-api_clinicapy)
8. [Interfaz Clínica de Usuario (`app.py`)](#8-interfaz-clínica-de-usuario-apppy)
9. [Decisiones de Diseño, Limitaciones Conocidas y Hoja de Ruta](#9-decisiones-de-diseño-limitaciones-conocidas-y-hoja-de-ruta)

---

## 1. Visión General del Sistema

El **Sistema Predictivo de Respuesta Metabólica y Nutrición Clínica** es una solución integral de **Ingeniería de Datos, Machine Learning y MLOps** diseñada para asistir en la predicción de diagnósticos y sintomatologías gastrointestinales a partir de biomarcadores clínicos de inflamación, métricas antropométricas, diversidad del microbioma intestinal, historial farmacológico (uso de antibióticos) y régimen dietético del paciente.

### Componentes Principales del Ecosistema
| Capa | Tecnología | Archivo Principal | Responsabilidad |
| :--- | :--- | :--- | :--- |
| **Almacenamiento Relacional** | PostgreSQL 16 | `Sistema_Predictivo_de_Respuesta_Metabolica_y_Nutricion_Clinica.sql` | Persistencia normalizada (3NF / Esquema Estrella) de pacientes, catálogos y registros clínicos. |
| **Ingesta y Normalización (ETL)** | Python, Pandas, SQLAlchemy | `airflow/dags/etl_clinico.py` | Extracción desde dataset clínico CSV (`latin1`), limpieza, mapeo de llaves foráneas y carga en PostgreSQL. |
| **Entrenamiento ML** | Scikit-Learn, Joblib | `airflow/dags/entrenamiento_ml.py` / `script.ipynb` | Reconstrucción relacional vía `INNER JOIN`, One-Hot Encoding, entrenamiento de `RandomForestClassifier` y serialización `.pkl`. |
| **Orquestación MLOps** | Apache Airflow 3.3.2 (Docker + Celery + Redis) | `airflow/dags/mlops_clinico_pipeline.py` | Ejecución programada diariamente (`03:00 AM`) del ciclo ETL → Reentrenamiento → Notificación a la API. |
| **Servicio de Inferencia (Backend)** | FastAPI, Pydantic, Uvicorn | `api_clinica.py` | Exposición del endpoint REST `POST /predecir_diagnostico/`, validación estricta del payload y alineación de matriz de características. |
| **Interfaz Clínica (Frontend)** | Streamlit, Requests | `app.py` | Captura interactiva de biomarcadores del paciente y visualización del diagnóstico previsto en tiempo real. |

---

## 2. Arquitectura de Infraestructura Híbrida (Windows + WSL2 + Docker)

El proyecto opera sobre una arquitectura híbrida que integra el entorno nativo de **Windows** con **WSL2 (Ubuntu)** y contenedores **Docker**.

> [!NOTE]
> **Nota sobre las rutas del repositorio**: Este repositorio reúne en una sola estructura unificada los artefactos que en ejecución residen distribuidos entre el sistema anfitrión Windows y el subsistema Linux (`WSL2`). Se han preservado las rutas originales en el código fuente (`\\wsl$\Ubuntu\home\user\airflow\dags` y `/opt/airflow/dags/`) para documentar fielmente la comunicación inter-sistema sin alterar el flujo de producción.

### Mecanismos de Comunicación Inter-Entorno

1. **Contenedores Docker (Airflow) → Host Windows (PostgreSQL y FastAPI)**:
   - Los contenedores de Airflow (`airflow-worker`, `airflow-scheduler`) se comunican con el servidor PostgreSQL (puerto `5432`) y el servidor FastAPI (puerto `8000`) alojados en el host mediante el DNS interno de Docker: `host.docker.internal`.
   - Cadena de conexión SQLAlchemy empleada en los scripts del DAG:
     ```python
     cadena_conexion = 'postgresql+psycopg2://<USERNAME>:<PASSWORD>@host.docker.internal:5432/<DATABASENAME>?client_encoding=utf8'
     ```

2. **Host Windows (FastAPI) → WSL2 / Volumen Docker (`dags`)**:
   - Cuando el worker de Airflow finaliza la tarea `reentrenar_random_forest`, exporta los artefactos binarios (`modelo_metabolico.pkl` y `columnas_entrenamiento.pkl`) dentro de `/opt/airflow/dags/`, directorio montado como volumen hacia `/home/user/airflow/dags` en WSL2.
   - El servicio `api_clinica.py` ejecutado en Windows consume directamente dichos artefactos a través de la ruta de red UNC de WSL:
     ```python
     ruta_wsl = r"\\wsl$\Ubuntu\home\user\airflow\dags"
     ```

> [!IMPORTANT]
> **Guía de Sustitución de Parámetros y Variables de Entorno**:  
> Por motivos de seguridad y portabilidad, las credenciales y rutas de usuario han sido generalizadas en el código fuente. Antes de ejecutar los scripts, sustituye los siguientes marcadores por los valores correspondientes a tu entorno local:
>
> | Parámetro / Marcador | Descripción | Con qué sustituirlo (Ejemplo) | Archivos donde se utiliza |
> | :--- | :--- | :--- | :--- |
> | **`<USERNAME>`** | Nombre de usuario de PostgreSQL asociado a la base de datos a utilizar. | `postgres` (o tu usuario configurado en PostgreSQL) | `airflow/dags/etl_clinico.py`, `airflow/dags/entrenamiento_ml.py`, `script.ipynb` |
> | **`<PASSWORD>`** | Contraseña de autenticación para acceder al servidor de base de datos PostgreSQL. | `tu_contraseña_segura` | `airflow/dags/etl_clinico.py`, `airflow/dags/entrenamiento_ml.py`, `script.ipynb` |
> | **`<DATABASENAME>`** | Nombre de la base de datos creada en PostgreSQL donde residen las tablas del modelo clínico. | `Sistema_Predictivo_de_Respuesta_Metabolica_y_Nutricion_Clinica` | `airflow/dags/etl_clinico.py`, `airflow/dags/entrenamiento_ml.py`, `script.ipynb` |
> | **`user`** | Nombre del directorio del usuario del sistema operativo (en WSL2 / Ubuntu o Windows) donde se aloja la carpeta `airflow/dags` para ejecutar/exportar los resultados (`.pkl`). | Tu nombre de usuario en Ubuntu WSL2 (ej. `/home/tu_usuario/airflow/dags` o `\\wsl$\Ubuntu\home\tu_usuario\airflow\dags`) | `api_clinica.py`, `docs/DIAGRAMAS_FLUJO_Y_UML.md` |

---

## 3. Ingeniería de Datos y Modelo Relacional (PostgreSQL)

El diseño de la base de datos transforma un archivo plano desnormalizado (`gastrointestinal_disease_dataset.csv`) en un modelo relacional normalizado compuesto por **dos tablas de catálogo (dimensiones)**, **una tabla de entidades (`Pacientes`)** y **una tabla central de hechos (`Registros_Clinicos`)**.

### Diccionario de Datos

#### 3.1. Tabla `Cat_Dietas` (Dimensión)
Almacena los regímenes alimenticios únicos identificados en la población clínica.
| Columna | Tipo de Dato | Restricciones | Descripción |
| :--- | :--- | :--- | :--- |
| `id_dieta` | `SERIAL` | `PRIMARY KEY` | Identificador único autoincremental de la dieta. |
| `tipo_dieta` | `VARCHAR(50)` | `NOT NULL`, `UNIQUE` | Nombre del régimen dietético (`Vegan`, `Vegetarian`, `Omnivore`, `Pescatarian`, `Standard`). |

#### 3.2. Tabla `Cat_Enfermedades` (Dimensión / Target)
Almacena el catálogo de diagnósticos o cuadros clínicos gastrointestinales que fungen como variable objetivo (`target_enfermedad`).
| Columna | Tipo de Dato | Restricciones | Descripción |
| :--- | :--- | :--- | :--- |
| `id_enfermedad` | `SERIAL` | `PRIMARY KEY` | Identificador único autoincremental de la condición clínica. |
| `clase_enfermedad` | `VARCHAR(100)` | `NOT NULL`, `UNIQUE` | Cuadro clínico diagnosticado (ej. `Abdominal cramps or pain`, `Bloating`, `Blood in stool`, `Diarrhea or constipation`, `Nausea or vomiting`, `Unexplained weight loss`). |

#### 3.3. Tabla `Pacientes` (Entidad Demográfica y Antropométrica)
Contiene los atributos demográficos y físicos base de cada paciente evaluado.
| Columna | Tipo de Dato | Restricciones | Descripción |
| :--- | :--- | :--- | :--- |
| `id_paciente` | `SERIAL` | `PRIMARY KEY` | Identificador único autoincremental del paciente. |
| `edad` | `INT` | `CHECK (edad >= 0)` | Edad biológica del paciente en años cumplidos. |
| `genero` | `VARCHAR(20)` | `NULLABLE` | Género reportado (`Male`, `Female`). |
| `imc` | `DECIMAL(5,2)` | `NULLABLE` | Índice de Masa Corporal ($\text{kg}/\text{m}^2$). |

#### 3.4. Tabla `Registros_Clinicos` (Tabla de Hechos)
Concentra las mediciones de biomarcadores inflamatorios, estado del microbioma y vínculos relacionales.
| Columna | Tipo de Dato | Restricciones | Descripción Clínica |
| :--- | :--- | :--- | :--- |
| `id_registro` | `SERIAL` | `PRIMARY KEY` | Identificador único del evento clínico. |
| `id_paciente` | `INT` | `NOT NULL`, `FK -> Pacientes(id_paciente) ON DELETE CASCADE` | Referencia al paciente evaluado. |
| `id_dieta` | `INT` | `NOT NULL`, `FK -> Cat_Dietas(id_dieta)` | Referencia al plan nutricional activo. |
| `id_enfermedad` | `INT` | `NOT NULL`, `FK -> Cat_Enfermedades(id_enfermedad)` | Referencia al diagnóstico gastrointestinal asociado. |
| `indice_microbioma` | `DECIMAL(6,3)` | `NULLABLE` | Índice cuantitativo de diversidad de la microbiota intestinal (rango normalizado `0.000` a `1.000`). |
| `proteina_c_reactiva` | `DECIMAL(6,2)` | `NULLABLE` | Nivel sérico de Proteína C Reactiva / velocidad de sedimentación (`CRP_ESR`) en $\text{mg/L}$, marcador sistémico de inflamación. |
| `calprotectina_fecal` | `DECIMAL(6,2)` | `NULLABLE` | Concentración de Calprotectina Fecal en $\mu\text{g/g}$, biomarcador específico de inflamación intestinal neutrofílica. |
| `uso_antibioticos` | `BOOLEAN` | `NULLABLE` | Indicador binario sobre administración reciente de antibióticos (`TRUE` / `FALSE`). |
| `fecha_registro` | `DATE` | `DEFAULT CURRENT_DATE` | Fecha de inserción del registro en el almacén SQL. |

---

## 4. Pipeline ETL Automatizado (`etl_clinico.py`)

El módulo `etl_clinico.py` implementa la lógica de **Extracción, Transformación y Carga (ETL)** hacia PostgreSQL siguiendo 8 etapas secuenciales:

1. **Extracción Resiliente a Codificación**:
   - Lee el archivo `gastrointestinal_disease_dataset.csv` especificando `encoding='latin1'` y `client_encoding=utf8` en el motor de SQLAlchemy para prevenir excepciones `UnicodeDecodeError` durante transacciones entre Windows y contenedores Linux.
2. **Estandarización de Esquema**:
   - Renombra las columnas originales en inglés a la nomenclatura técnica del modelo relacional en español:
     - `Diet_Type` → `tipo_dieta`
     - `Disease_Class` → `clase_enfermedad`
     - `Age` → `edad`
     - `Gender` → `genero`
     - `BMI` → `imc`
     - `Microbiome_Index` → `indice_microbioma`
     - `CRP_ESR` → `proteina_c_reactiva`
     - `Fecal_Calprotectin` → `calprotectina_fecal`
     - `Antibiotic_Use` → `uso_antibioticos`
3. **Tipado Booleano Estricto**:
   - Fuerza la conversión explícita de la columna `uso_antibioticos` (`0`/`1` en origen) a tipo `bool` de Python (`astype(bool)`) y posteriormente a `sqlalchemy.types.Boolean` al insertar en PostgreSQL.
4. **Poblamiento de Dimensiones**:
   - Extrae los valores únicos sin nulos (`.dropna().unique()`) de `tipo_dieta` y `clase_enfermedad` y los inserta en `cat_dietas` y `cat_enfermedades`.
5. **Resolución de Claves Subrogadas (Lookup Merge)**:
   - Consulta las tablas `cat_dietas` y `cat_enfermedades` recién pobladas en PostgreSQL para recuperar sus llaves primarias (`id_dieta`, `id_enfermedad`) y realiza un `LEFT JOIN` en memoria (`pd.merge`) con el DataFrame principal.
6. **Inserción de Entidades `Pacientes` y Mapeo de `id_paciente`**:
   - Inserta los atributos `['edad', 'genero', 'imc']` en la tabla `pacientes`, recupera la secuencia ordenada de `id_paciente` generada por PostgreSQL y la asigna al DataFrame maestro.
7. **Carga de la Tabla de Hechos `Registros_Clinicos`**:
   - Selecciona las llaves foráneas y los biomarcadores cuantitativos e inserta el bloque completo mediante `df_registros.to_sql('registros_clinicos', engine, if_exists='append', index=False, dtype={'uso_antibioticos': Boolean})`.

---

## 5. Pipeline de Machine Learning y Feature Engineering (`entrenamiento_ml.py`)

El entrenamiento del modelo se encuentra documentado de forma exploratoria en `script.ipynb` y productivizado para ejecución desatendida en `airflow/dags/entrenamiento_ml.py`.

### 5.1. Reconstrucción del Dataset Analítico desde SQL
El script ejecuta una consulta relacional con triple `INNER JOIN` para consolidar la matriz de entrenamiento directamente desde la fuente única de verdad (PostgreSQL):

```sql
SELECT 
    p.edad, 
    p.genero, 
    p.imc, 
    d.tipo_dieta, 
    r.indice_microbioma, 
    r.proteina_c_reactiva, 
    r.calprotectina_fecal,
    r.uso_antibioticos,
    e.clase_enfermedad AS target_enfermedad
FROM registros_clinicos r
INNER JOIN pacientes p ON r.id_paciente = p.id_paciente
INNER JOIN cat_dietas d ON r.id_dieta = d.id_dieta
INNER JOIN cat_enfermedades e ON r.id_enfermedad = e.id_enfermedad;
```

### 5.2. Ingeniería de Características (Feature Engineering)
- **Codificación Categórica (One-Hot Encoding)**:
  Se aplica `pd.get_dummies(df_ml, columns=['genero', 'tipo_dieta'], drop_first=True)` para evitar la multicolinealidad perfecta (trampa de las variables ficticias).
- **Transformación Booleana a Numérica**:
  La columna `uso_antibioticos` se convierte de booleano a entero binario (`0` o `1`).
- **Vector Final de Características (`columnas_entrenamiento.pkl`)**:
  La matriz $X$ resultante consta de **10 características**:
  1. `edad`
  2. `imc`
  3. `indice_microbioma`
  4. `proteina_c_reactiva`
  5. `calprotectina_fecal`
  6. `uso_antibioticos`
  7. `genero_Male`
  8. `tipo_dieta_Pescatarian`
  9. `tipo_dieta_Vegan`
  10. `tipo_dieta_Vegetarian`

### 5.3. Configuración del Modelo e Hiperparámetros
- **Partición de Datos**: `train_test_split(X, y, test_size=0.2, random_state=42)`
  - Conjunto de entrenamiento ($X_{\text{train}}$): **24,448 registros** $\times$ 10 variables.
  - Conjunto de prueba ($X_{\text{test}}$): **6,112 registros** $\times$ 10 variables.
- **Algoritmo**: `RandomForestClassifier` (Scikit-Learn)
  - `n_estimators = 100`: Ensamble de 100 árboles de decisión.
  - `class_weight = 'balanced'`: Ajuste automático de pesos inversamente proporcional a las frecuencias de clase en los datos de entrada.
  - `random_state = 42`: Semilla determinista para reproducibilidad.
  - `n_jobs = -1`: Paralelización en todos los núcleos de CPU disponibles.

### 5.4. Importancia de Biomarcadores (Feature Importance)
De acuerdo con los resultados exportados en `importancia_biomarcadores.csv`, el peso predictivo de cada característica según la reducción de impureza de Gini es el siguiente:

| Rango | Variable / Biomarcador | Importancia Relativa | Porcentaje |
| :---: | :--- | :---: | :---: |
| **1** | `proteina_c_reactiva` (PCR) | `0.191748` | **19.17%** |
| **2** | `calprotectina_fecal` | `0.187851` | **18.79%** |
| **3** | `imc` (Índice de Masa Corporal) | `0.181556` | **18.16%** |
| **4** | `indice_microbioma` | `0.170320` | **17.03%** |
| **5** | `edad` | `0.166282` | **16.63%** |
| **6** | `uso_antibioticos` | `0.025764` | **2.58%** |
| **7** | `genero_Male` | `0.020442` | **2.04%** |
| **8** | `tipo_dieta_Pescatarian` | `0.019082` | **1.91%** |
| **9** | `tipo_dieta_Vegan` | `0.018871` | **1.89%** |
| **10** | `tipo_dieta_Vegetarian` | `0.018084` | **1.81%** |

> [!IMPORTANT]
> **Hallazgo Clínico-Analítico**: Los 5 biomarcadores cuantitativos continuos (`proteina_c_reactiva`, `calprotectina_fecal`, `imc`, `indice_microbioma` y `edad`) concentran el **89.78%** del poder discriminativo del modelo, confirmando la relevancia predominante de los marcadores inflamatorios e intestinales frente a las variables demográficas y dietéticas categóricas (que suman el **10.22%** restante).

### 5.5. Métricas de Evaluación (`reporte_metricas_clinicas.txt`)
Evaluación sobre el conjunto de prueba ($N = 6,112$ muestras distribuidas en 6 clases balanceadas):

| Clase Diagnóstica | Precision | Recall | F1-Score | Support |
| :--- | :---: | :---: | :---: | :---: |
| `Abdominal cramps or pain` | 0.17 | 0.18 | 0.18 | 1,089 |
| `Bloating` | 0.17 | 0.19 | 0.18 | 969 |
| `Blood in stool` | 0.18 | 0.18 | 0.18 | 1,035 |
| `Diarrhea or constipation` | 0.17 | 0.15 | 0.16 | 1,022 |
| `Nausea or vomiting` | 0.17 | 0.17 | 0.17 | 986 |
| `Unexplained weight loss` | 0.17 | 0.15 | 0.16 | 1,011 |
| **Accuracy Global / Promedio Macro** | **0.17** | **0.17** | **0.17** | **6,112** |

*(Nota: Al tratarse de un dataset sintético con distribución uniforme entre 6 clases, la precisión base esperada por azar es $1/6 \approx 16.67\%$. La arquitectura MLOps permite sustituir el dataset por registros hospitalarios reales manteniendo intacto todo el flujo de ingeniería).*

---

## 6. Orquestación MLOps con Apache Airflow (`mlops_clinico_pipeline.py`)

El despliegue de Airflow se gestiona mediante `airflow/docker-compose.yaml` utilizando la imagen oficial `apache/airflow:3.3.2` con arquitectura distribuida **`CeleryExecutor`**, broker **Redis 7.2** y base de datos de metadatos **PostgreSQL 16**.

### Especificación del DAG: `pipeline_predictivo_metabolico`
- **Propietario (`owner`)**: `postgres`
- **Planificación (`schedule`)**: `'0 3 * * *'` (Ejecución automática todos los días a las **03:00 AM**).
- **`catchup`**: `False` (Evita ejecuciones retroactivas innecesarias).
- **Política de Tolerancia a Fallos**:
  - `retries`: `2` reintentos automáticos por tarea fallida.
  - `retry_delay`: `timedelta(minutes=5)` entre cada reintento.
  - `email_on_failure`: `True`.

### Secuencia de Tareas (`BashOperator`)
1. **`ejecutar_etl_postgresql`**:
   - Comando: `python /opt/airflow/dags/etl_clinico.py`
   - Acción: Ingesta y normaliza los nuevos lotes de datos clínicos en PostgreSQL.
2. **`reentrenar_random_forest`**:
   - Comando: `python /opt/airflow/dags/entrenamiento_ml.py`
   - Acción: Extrae el historial actualizado con `JOINs`, reentrena el `RandomForestClassifier` y sobrescribe `modelo_metabolico.pkl` y `columnas_entrenamiento.pkl`.
3. **`recargar_api_fastapi`**:
   - Comando: `curl -X POST http://host.docker.internal:8000/recargar_modelo || true`
   - Acción: Envía una señal HTTP POST al servidor FastAPI en el host Windows para notificar la disponibilidad de un nuevo modelo entrenado.

---

## 7. Especificación del Backend API RESTful (`api_clinica.py`)

Construida con **FastAPI** y servida mediante **Uvicorn** en `http://127.0.0.1:8000`, la API carga en memoria al iniciar los artefactos serializados desde la ruta de red de WSL2.

### 7.1. Esquema de Validación Pydantic (`PacienteInput`)
Garantiza la integridad de los datos clínicos antes de invocar el modelo:

| Campo JSON | Tipo | Regla de Validación Pydantic | Descripción |
| :--- | :--- | :--- | :--- |
| `edad` | `int` | `gt=0` ($> 0$) | Edad del paciente en años. |
| `imc` | `float` | `gt=10.0` ($> 10.0$) | Índice de Masa Corporal. |
| `indice_microbioma` | `float` | `ge=0.0, le=1.0` ($[0.0, 1.0]$) | Índice de diversidad del microbioma. |
| `proteina_c_reactiva` | `float` | `ge=0.0` ($\ge 0.0$) | Niveles de PCR en $\text{mg/L}$. |
| `calprotectina_fecal` | `float` | `ge=0.0` ($\ge 0.0$) | Niveles de Calprotectina en $\mu\text{g/g}$. |
| `uso_antibioticos` | `bool` | Requerido (`true`/`false`) | Historial reciente de antibióticos. |
| `genero` | `str` | Requerido | Género (`"Male"`, `"Female"`). |
| `tipo_dieta` | `str` | Requerido | Tipo de dieta (`"Vegan"`, `"Vegetarian"`, `"Omnivore"`, `"Pescatarian"`, `"Standard"`). |

### 7.2. Alineación Dinámica de Características en Inferencia
Cuando un cliente envía un único registro JSON, `pd.get_dummies()` solo genera columnas para la categoría presente en esa petición (por ejemplo, `tipo_dieta_Vegan`), omitiendo el resto de categorías vistas en el entrenamiento. Para evitar errores de dimensionalidad en Scikit-Learn, la API ejecuta una **re-indexación estructural**:

```python
datos_procesados = pd.get_dummies(datos_crudos)
datos_finales = datos_procesados.reindex(columns=columnas_entrenamiento, fill_value=0)
prediccion = modelo.predict(datos_finales)[0]
```

### 7.3. Ejemplo de Petición y Respuesta (`POST /predecir_diagnostico/`)

**Request Payload (`application/json`)**:
```json
{
  "edad": 35,
  "imc": 24.8,
  "indice_microbioma": 0.62,
  "proteina_c_reactiva": 14.5,
  "calprotectina_fecal": 180.0,
  "uso_antibioticos": true,
  "genero": "Female",
  "tipo_dieta": "Vegetarian"
}
```

**Response Payload (`200 OK`)**:
```json
{
  "status": "success",
  "modelo_utilizado": "RandomForestClassifier",
  "prediccion_clinica": "Abdominal cramps or pain"
}
```

---

## 8. Interfaz Clínica de Usuario (`app.py`)

Desarrollada en **Streamlit**, proporciona un panel médico limpio dividido en dos columnas (`st.columns(2)`):
- **Columna Izquierda**: Controles numéricos y deslizadores para `Edad` (`1-120`), `IMC` (`10.0-50.0`), `Índice de Microbioma` (`0.0-1.0`) y selector de `¿Uso reciente de antibióticos?` (`Sí`/`No`, traducido internamente a `True`/`False`).
- **Columna Derecha**: Entradas para `Proteína C Reactiva` (`0.0-200.0 mg/L`), `Calprotectina Fecal` (`0.0-1000.0 μg/g`), `Género` (`Male`/`Female`) y `Tipo de Dieta` (`Vegan`, `Vegetarian`, `Omnivore`, `Pescatarian`, `Standard`).
- **Manejo de Excepciones de Red**: Captura errores `requests.exceptions.ConnectionError` presentando una alerta clara en interfaz si el backend Uvicorn no se encuentra activo en el puerto `8000`.

---

## 9. Decisiones de Diseño, Limitaciones Conocidas y Hoja de Ruta

1. **Exclusión de `modelo_metabolico.pkl` en Git**:
   - El archivo binario del bosque aleatorio ocupa **~228 MB**, superando el límite estricto de **100 MB** de GitHub. Por ello se ha incluido en `.gitignore`, garantizando que el repositorio pueda subirse sin bloqueos. El archivo se regenera automáticamente ejecutando `entrenamiento_ml.py` o `script.ipynb`.
2. **Idempotencia en el ETL (`cat_dietas` y `cat_enfermedades`)**:
   - Las tablas de catálogo poseen la restricción `UNIQUE` en `tipo_dieta` y `clase_enfermedad`. En ejecuciones subsecuentes del ETL sobre una base de datos ya poblada, se recomienda emplear una estrategia `INSERT ... ON CONFLICT DO NOTHING` o truncado previo de staging para garantizar idempotencia total.
3. **Implementación del Endpoint `/recargar_modelo`**:
   - El DAG de Airflow ya incluye el hook `curl -X POST http://host.docker.internal:8000/recargar_modelo || true`. Como siguiente paso evolutivo en `api_clinica.py`, se puede agregar la función decorada con `@app.post("/recargar_modelo")` que vuelva a ejecutar `joblib.load()` en caliente sin reiniciar Uvicorn.
