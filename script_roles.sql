WITH datos_crudos (documento_usuario, nombre_ubicacion, nombre_rol) AS (
    VALUES
        ('1012454567', 'GERENCIA JURÍDICA', 'Gestionador')
),
datos_calculados AS (
                SELECT
                    dc.documento_usuario,
                    dc.nombre_ubicacion,
                    dc.nombre_rol,
                    u.id AS usuario_id,
                    r.id AS rol_id,
                    o.id_dependencia AS seccionsubseccion_id_oficina,
                    o.id AS oficina_id,
                    s.idseccionsubseccion AS seccionsubseccion_id_seccion,
                    CASE
                        WHEN o.id IS NOT NULL THEN 'GRUPO_ESPECIAL'
                        ELSE 'SECCION_UNICA'
                    END AS tipo_ubicacion_mapeo
                FROM datos_crudos dc
                LEFT JOIN usuarios u ON u.numero_documento = dc.documento_usuario
                LEFT JOIN roles r ON r.nombre = dc.nombre_rol
                LEFT JOIN oficina o ON o.nombre = dc.nombre_ubicacion
                LEFT JOIN seccionsubseccion s ON s.nombre = dc.nombre_ubicacion
),
-- CTE para identificar los registros que SÍ se insertarían (no existen y tienen IDs válidos)
registros_a_insertar_procesado AS (
    SELECT
        calc.documento_usuario,
        calc.nombre_ubicacion,
        calc.nombre_rol,
        calc.usuario_id,
        -- Asignación final de IDs para la tabla de destino
        CASE
            WHEN calc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' THEN calc.seccionsubseccion_id
            ELSE calc.seccionsubseccion_id
        END AS seccionsubseccion_id_final,
        CASE
            WHEN calc.tipo_ubicacion_mapeo = 'SECCION_UNICA' THEN NULL
            ELSE calc.oficina_id
        END AS oficina_id_final,
        calc.rol_id,
        'OK - LISTO PARA INSERTAR' AS estado_registro
    FROM
        datos_calculados calc
    WHERE
        calc.usuario_id IS NOT NULL
        AND calc.rol_id IS NOT NULL
        AND (
            (calc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' AND calc.oficina_id IS NOT NULL AND calc.seccionsubseccion_id IS NOT NULL) OR
            (calc.tipo_ubicacion_mapeo = 'SECCION_UNICA' AND calc.seccionsubseccion_id IS NOT NULL AND calc.oficina_id IS NULL)
        )
        AND NOT EXISTS (
            SELECT 1
            FROM usuario_relacion ur
            WHERE
                ur.usuario_id = calc.usuario_id
                AND ur.seccionsubseccion_id IS NOT DISTINCT FROM (CASE WHEN calc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' THEN calc.seccionsubseccion_id ELSE calc.seccionsubseccion_id END)
                AND ur.oficina_id IS NOT DISTINCT FROM (CASE WHEN calc.tipo_ubicacion_mapeo = 'SECCION_UNICA' THEN NULL ELSE calc.oficina_id END)
                AND ur.rol_id = calc.rol_id
        )
),
-- CTE para identificar los registros que NO se insertarían por ya existir
registros_ya_existentes_procesado AS (
    SELECT
        calc.documento_usuario,
        calc.nombre_ubicacion,
        calc.nombre_rol,
        calc.usuario_id,
        -- Asignación final de IDs para la tabla de destino
        CASE
            WHEN calc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' THEN calc.seccionsubseccion_id
            ELSE calc.seccionsubseccion_id
        END AS seccionsubseccion_id_final,
        CASE
            WHEN calc.tipo_ubicacion_mapeo = 'SECCION_UNICA' THEN NULL
            ELSE calc.oficina_id
        END AS oficina_id_final,
        calc.rol_id,
        'IGNORADO - YA EXISTE' AS estado_registro
    FROM
        datos_calculados calc
    WHERE
        calc.usuario_id IS NOT NULL
        AND calc.rol_id IS NOT NULL
        AND (
            (calc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' AND calc.oficina_id IS NOT NULL AND calc.seccionsubseccion_id IS NOT NULL) OR
            (calc.tipo_ubicacion_mapeo = 'SECCION_UNICA' AND calc.seccionsubseccion_id IS NOT NULL AND calc.oficina_id IS NULL)
        )
        AND EXISTS (
            SELECT 1
            FROM usuario_relacion ur
            WHERE
                ur.usuario_id = calc.usuario_id
                AND ur.seccionsubseccion_id IS NOT DISTINCT FROM (CASE WHEN calc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' THEN calc.seccionsubseccion_id ELSE calc.seccionsubseccion_id END)
                AND ur.oficina_id IS NOT DISTINCT FROM (CASE WHEN calc.tipo_ubicacion_mapeo = 'SECCION_UNICA' THEN NULL ELSE calc.oficina_id END)
                AND ur.rol_id = calc.rol_id
        )
),
-- CTE para identificar los registros que NO se insertarían por datos inválidos (usuario, ubicación o rol no encontrados)
registros_con_error_mapeo_procesado AS (
    SELECT
        dc.documento_usuario,
        dc.nombre_ubicacion,
        dc.nombre_rol,
        calc.usuario_id,
        calc.seccionsubseccion_id,
        calc.oficina_id,
        calc.rol_id,
        CASE
            WHEN calc.usuario_id IS NULL THEN 'ERROR - USUARIO NO ENCONTRADO'
            WHEN calc.rol_id IS NULL THEN 'ERROR - ROL NO ENCONTRADO'
            WHEN NOT (
                (calc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' AND calc.oficina_id IS NOT NULL AND calc.seccionsubseccion_id IS NOT NULL) OR
                (calc.tipo_ubicacion_mapeo = 'SECCION_UNICA' AND calc.seccionsubseccion_id IS NOT NULL AND calc.oficina_id IS NULL)
            ) THEN 'ERROR - UBICACIÓN NO ENCONTRADA O MAL ASIGNADA'
            ELSE 'ERROR - DATOS INVÁLIDOS (DETALLE NO ESPECIFICADO)'
        END AS estado_registro
    FROM
        datos_crudos dc
    LEFT JOIN datos_calculados calc ON dc.documento_usuario = calc.documento_usuario AND dc.nombre_ubicacion = calc.nombre_ubicacion AND dc.nombre_rol = calc.nombre_rol
    WHERE
        calc.usuario_id IS NULL
        OR calc.rol_id IS NULL
        OR NOT (
            (calc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' AND calc.oficina_id IS NOT NULL AND calc.seccionsubseccion_id IS NOT NULL) OR
            (calc.tipo_ubicacion_mapeo = 'SECCION_UNICA' AND calc.seccionsubseccion_id IS NOT NULL AND calc.oficina_id IS NULL)
        )
)
-- La sentencia SELECT para el reporte
SELECT
    documento_usuario,
    nombre_ubicacion,
    nombre_rol,
    usuario_id,
    seccionsubseccion_id_final AS seccionsubseccion_id,
    oficina_id_final AS oficina_id,
    rol_id,
    estado_registro
FROM registros_a_insertar_procesado
UNION ALL
SELECT
    documento_usuario,
    nombre_ubicacion,
    nombre_rol,
    usuario_id,
    seccionsubseccion_id_final AS seccionsubseccion_id,
    oficina_id_final AS oficina_id,
    rol_id,
    estado_registro
FROM registros_ya_existentes_procesado
UNION ALL
SELECT
    documento_usuario,
    nombre_ubicacion,
    nombre_rol,
    usuario_id,
    seccionsubseccion_id,
    oficina_id,
    rol_id,
    estado_registro
FROM registros_con_error_mapeo_procesado
ORDER BY estado_registro, documento_usuario, nombre_ubicacion, nombre_rol;

---

WITH datos_crudos (documento_usuario, nombre_ubicacion, nombre_rol) AS (
    VALUES
        ('4888175', 'N/A', 'Aprobador')
),
datos_calculados AS (
                SELECT
                    dc.documento_usuario,
                    dc.nombre_ubicacion,
                    dc.nombre_rol,
                    u.id AS usuario_id,
                    r.id AS rol_id,
                    o.id_dependencia AS seccionsubseccion_id_oficina,
                    o.id AS oficina_id,
                    s.idseccionsubseccion AS seccionsubseccion_id_seccion,
                    CASE
                        WHEN o.id IS NOT NULL THEN 'GRUPO_ESPECIAL'
                        ELSE 'SECCION_UNICA'
                    END AS tipo_ubicacion_mapeo
                FROM datos_crudos dc
                LEFT JOIN usuarios u ON u.numero_documento = dc.documento_usuario
                LEFT JOIN roles r ON r.nombre = dc.nombre_rol
                LEFT JOIN oficina o ON o.nombre = dc.nombre_ubicacion
                LEFT JOIN seccionsubseccion s ON s.nombre = dc.nombre_ubicacion
)
INSERT INTO usuario_relacion (usuario_id, seccionsubseccion_id, oficina_id, rol_id, punto_radicacion_id)
SELECT
    calc.usuario_id,
    -- Asignación final de IDs para la tabla de destino
    CASE
        WHEN calc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' THEN calc.seccionsubseccion_id -- Mantener id_dependencia para grupos especiales
        ELSE calc.seccionsubseccion_id
    END AS seccionsubseccion_id_final,
    CASE
        WHEN calc.tipo_ubicacion_mapeo = 'SECCION_UNICA' THEN NULL
        ELSE calc.oficina_id
    END AS oficina_id_final,
    calc.rol_id,
    NULL AS punto_radicacion_id
FROM
    datos_calculados calc
WHERE
    calc.usuario_id IS NOT NULL
    AND calc.rol_id IS NOT NULL
    AND (
        (calc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' AND calc.oficina_id IS NOT NULL AND calc.seccionsubseccion_id IS NOT NULL) OR
        (calc.tipo_ubicacion_mapeo = 'SECCION_UNICA' AND calc.seccionsubseccion_id IS NOT NULL AND calc.oficina_id IS NULL)
    )
    AND NOT EXISTS (
        SELECT 1
        FROM usuario_relacion ur
        WHERE
            ur.usuario_id = calc.usuario_id
            AND ur.seccionsubseccion_id IS NOT DISTINCT FROM (CASE WHEN calc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' THEN calc.seccionsubseccion_id ELSE calc.seccionsubseccion_id END)
            AND ur.oficina_id IS NOT DISTINCT FROM (CASE WHEN calc.tipo_ubicacion_mapeo = 'SECCION_UNICA' THEN NULL ELSE calc.oficina_id END)
            AND ur.rol_id = calc.rol_id
    );
