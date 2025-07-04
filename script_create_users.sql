WITH datos_a_verificar (
    user_name, first_name, last_name, enabled, email, numero_documento, modulo_id, tipousuario, tipo_documento,
    accion_modificacion, departamento_id, municipio_id, cargo_nombre_str, timeout_min
) AS (
VALUES
    ('1024588020', 'Lady', 'Hernandez', TRUE, 'lady.hernandez@linktic.com', '1024588020', 1, 'Proveedor', 3, 'Creacion de usuario', 6, 149, 'PROFESIONAL', 4)
)
SELECT DISTINCT dv.cargo_nombre_str AS "Cargos No Encontrados en la Tabla 'cargos'"
FROM datos_a_verificar dv
LEFT JOIN cargos c ON dv.cargo_nombre_str = c.nombre
WHERE c.id IS NULL;

-- PASO 5: Inserción Masiva de Usuarios
WITH datos_usuarios_a_insertar (
    user_name,
    first_name,
    last_name,
    enabled,
    email,
    numero_documento,
    modulo_id,
    tipousuario,
    tipo_documento,
    accion_modificacion,
    departamento_id,
    municipio_id,
    cargo_nombre_str, -- Temporal: Nombre del cargo (STRING) para buscar su ID
    timeout_min
) AS (
VALUES
    ('1024588020', 'Lady', 'Hernandez', TRUE, 'lady.hernandez@linktic.com', '1024588020', 1, 'Proveedor', 3, 'Creacion de usuario', 6, 149, 'PROFESIONAL', 4)
)
INSERT INTO usuarios (
    user_name,
    first_name,
    last_name,
    enabled,
    email,
    numero_documento,
    modulo_id,
    tipousuario,
    tipo_documento,
    accion_modificacion,
    departamento_id,
    municipio_id,
    cargo, -- Aquí se espera el ID numérico del cargo
    timeout_min
)
SELECT
    du.user_name,
    du.first_name,
    du.last_name,
    du.enabled,
    du.email,
    du.numero_documento,
    du.modulo_id,
    du.tipousuario,
    du.tipo_documento,
    du.accion_modificacion,
    du.departamento_id,
    du.municipio_id,
    c.id AS cargo_id, -- Obtenemos el ID del cargo de la tabla 'cargos'
    du.timeout_min
FROM
    datos_usuarios_a_insertar du
JOIN
    cargos c ON du.cargo_nombre_str = c.nombre;