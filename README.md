# Bundle de clientes y provincia

Este ejemplo despliega un job de Databricks que actualiza `ventas.clientes`: agrega `provincia STRING` si aún no existe e inserta cinco clientes ficticios de San José. El job no se ejecuta al desplegar; los cambios ocurren únicamente al iniciarlo.

## Configuración

El job usa serverless, por lo que no requiere un ID de cluster. El workspace debe tener habilitado serverless jobs. El catálogo predeterminado actualmente es `lab4_calvo`; configura un catálogo de desarrollo distinto si no quieres que el target `dev` modifique la tabla de producción. El target `prod` apunta a `lab4_calvo`. La tabla y su esquema base deben existir en ambos catálogos antes de ejecutar el job.

El usuario que ejecute el job necesita permisos para leer y alterar la tabla, y para insertar/actualizar sus datos. La columna `email` existente no se reemplaza ni se altera; sus tags de clasificación PII se conservan.

## Comandos

Ejecuta desde la raíz del bundle y sustituye `<perfil>` por el perfil autenticado de tu workspace:

```powershell
databricks bundle validate -t dev --profile <perfil>
databricks bundle deploy -t dev --profile <perfil>
databricks bundle run clientes_provincia -t dev --profile <perfil>

databricks bundle validate -t prod --profile <perfil>
databricks bundle deploy -t prod --profile <perfil>
databricks bundle run clientes_provincia -t prod --profile <perfil>
```

Los cinco IDs de ejemplo son negativos y estables. El `MERGE` usa `cliente_id` como clave, por lo que volver a ejecutar el job no vuelve a insertar esos registros.