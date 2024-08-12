# Descripción

Los archivo AIS, CIS, PIS corresponden a cuentas, clientes y pagos (en inglés 
_accounts_, _customers_, _payments_).  

1. La hoja de cálculo `Security Info.xlsx` contiene las especificaciones de los campos
requeridos.  
2. El _script_ `src.info_security.py` genera las especificaciones en formato de computadora.  
    Crea dos versiones: en el _blob_ de Azure se pueden leer para ambientes de desarrollo;   
    en `refs.upload-specs` se guarda una copia física para ambientes de producción.  
3. El _notebook_ `1 🦝 Falcon Tables` se ejecuta desde Databricks para generar 
    los reportes con la frecuencia esperada.  


# Especificaciones técnicas

El archivo `config.py` contiene las especificaciones para la interacción con las 
demás áreas de infraestructura.  Particularmente:  

* Usamos el sufijo `(data-)(ops-)fraud-prevention` para designar los elementos correspondientes.  
* _Service principal_ `sp-fraud-prevention` para la autenticación del proceso. 
* _Scope de Databricks_ `ops-fraud-prevention` para almacenar las credenciales del
    principado.
* Recursos de Azure asociados:
  * _Key Vault_ `kv-ops-data-{env}` para almacenar secretos adicionales.  
  * _Storage_ `stlakehylia{env}` donde se leen y depositan datos de reportes.  
  * Ruta del _storage_ `/ops/fraud-prevention` para cualquier generación de archivos.  
  * Más aún, utilizamos la siguiente especificación en la generación de reportes: 
    - `.../reports/{cual-reporte}/yyyy-mm-dd.csv` donde `cual-reporte` es uno de 
      los correspondientes `{customers,accounts,payments}`.  
  * Para los datos de insumos utilizamos:  
    - tablas en _metastore_/_unity catalog_ de Databricks de acuerdo con `config.DBKS_MAPPING`. 
    - especificaciones técnicas de las tablas; idealmente se guardan en el _storage_, 
      aunque una solución temporal es leerlas de la carpeta del repositorio.  


# Instalación

0. Naturalmente se requiere Python.  
  i) Variables de ambiente.  `template.env` remplazar y guardar en `.env`
  ii) Instalar Python3.10, guardar ubicación como `$PY310`. 
  iii) Crear ambiente, activar e instalar: 
    ```shell
    > virtualenv fraud-env -p ${PY310} 
    > python .\install_reqs.py
    ```
  



