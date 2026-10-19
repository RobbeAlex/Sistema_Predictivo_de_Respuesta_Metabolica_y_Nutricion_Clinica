-- 1. Crear Tablas de Catálogo (Dimensiones)
CREATE TABLE Cat_Dietas (
    id_dieta SERIAL PRIMARY KEY,
    tipo_dieta VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE Cat_Enfermedades (
    id_enfermedad SERIAL PRIMARY KEY,
    clase_enfermedad VARCHAR(100) NOT NULL UNIQUE
);

-- 2. Crear Tabla de Pacientes
CREATE TABLE Pacientes (
    id_paciente SERIAL PRIMARY KEY,
    edad INT CHECK (edad >= 0),
    genero VARCHAR(20),
    imc DECIMAL(5,2) -- Índice de Masa Corporal
);

-- 3. Crear Tabla Central de Registros (Tabla de Hechos)
CREATE TABLE Registros_Clinicos (
    id_registro SERIAL PRIMARY KEY,
    id_paciente INT NOT NULL,
    id_dieta INT NOT NULL,
    id_enfermedad INT NOT NULL,
    
    -- Biomarcadores
    indice_microbioma DECIMAL(6,3), 
    proteina_c_reactiva DECIMAL(6,2), -- Marcador de inflamación (CRP)
    calprotectina_fecal DECIMAL(6,2), -- Marcador intestinal específico
    uso_antibioticos BOOLEAN,
    fecha_registro DATE DEFAULT CURRENT_DATE,

		-- Llaves Foráneas (Relaciones)
		FOREIGN KEY (id_paciente) REFERENCES Pacientes(id_paciente) ON DELETE CASCADE,
		FOREIGN KEY (id_dieta) REFERENCES Cat_Dietas(id_dieta),
		FOREIGN KEY (id_enfermedad) REFERENCES Cat_Enfermedades(id_enfermedad)
);