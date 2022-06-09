## **Sobre el activo**

Este activo está diseñado para actuar como una plataforma de selección para Watson Assistants. De los registros de conversación que genera, se extraen datos sobre su desempeño y se transforman en información de alto nivel dirigida a la audiencia ejecutiva. A modo de ejemplo tenemos: número de conversaciones no contestadas (transferidas a operador humano o no), calidad de comprensión de las intenciones del cliente por parte del asistente, historial de volumen de conversaciones diarias, desempeño en el tiempo, entre otros.

## Implementación de recursos

Esta documentación explica los detalles de cómo funciona el activo. Para ver la documentación de implementación de activos [consulte esta guía](https://github.ibm.com/Innovation-Studio-Assets/assistant-curator/tree/master/terraform).

## **Arquitectura**

![img](https://portal-de-demos-imgs.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator-arch.png)

El activo está estructurado en dos partes principales:

- Una secuencia de [**cloud functions**](https://cloud.ibm.com/docs/openwhisk) que actúa en la plataforma IBM Cloud en intervalos de tiempo definidos recibiendo, transformando y almacenando los registros del asistente;

- Una interfaz en la que el curador puede evaluar la comprensión del asistente a nivel de conversación y ver un [**Panel ejecutivo**](https://cloud.ibm.com/docs/cognos-dashboard-embedded?topic=cognos-dashboard-embedded-gettingstartedtutorial) generado a partir de los datos extraídos.

La cadena de cloud functions se comunica con el asistente a través del método [_listLogs()_](https://cloud.ibm.com/apidocs/assistant-v1?code=node#listlogs) de la [API v1](https://cloud.ibm.com/apidocs/assistant-v1) de Watson Assistant. Una vez que todos los registros están en su lugar, la secuencia realiza las siguientes tareas:

1. Recopila las informaciones consideradas más relevantes para la tarea de curación a **nivel de registro**:

   - ID del usuario con el que estaba hablando el asistente;
   - ID de la conversación a la que pertenece este registro;
   - ID del inicio de sesión en cuestión;
   - El mensaje enviado por el cliente y la hora en que fue recibido;
   - La respuesta dada por el asistente y la hora en que fue entregada;
   - El título del _node_ del árbol de diálogo del asistente activado en esta transacción;
   - Las intenciones entendidas por el asistente en esta transacción;
   - La confianza que tiene el asistente en la intención que ha elegido como principal (valor numérico);
   - Las entidades comprendidas por el asistente en esta transacció

2. Compila registros de conversaciones de acuerdo con el _conversationID_ que lleva y:

   - Clasifica si el medio utilizado en la conversación fue por teléfono o por intercambio de mensajes;
   - Da la fecha y hora de inicio;
   - Guarda toda su duración (en segundos);
   - Guarda, si alguna , la nota de retroalimentación que el asistente recibió del cliente;
   - Analiza y guarda si la conversación se transfirió o no a un asistente humano;
   - Clasifica si la conversación es relevante en función de si existen o no ciertas intenciones en alguno de los registros participantes;
   - Indica si esta conversación es el primer contacto del cliente con el asistente o no.

3. Para conversaciones telefónicas, almacena datos referentes al número e IP del usuario y también si hubo interrupciones de voz por parte del cliente (cliente hablando “encima” del asistente)

4. Para todas las conversaciones, también almacena las variables de entorno generadas durante la conversación, clasificando su tipo de datos (Ej.: "string", "objeto", "booleano", etc.)

5. Manipulando los registros de esta manera, la secuencia termina insertándolos en tres bases de datos:[ **Cloudant**](https://cloud.ibm.com/docs/Cloudant?topic=Cloudant-client-libraries#client-libraries), [**COS**](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-getting-started-cloud-object-storage) y [**Db2**](https://cloud.ibm.com/docs/Db2onCloud?topic=Db2onCloud-getting-started)

Al conectar [**Db2**](https://cloud.ibm.com/docs/Db2onCloud?topic=Db2onCloud-getting-started) con [**Cognos**](https://cloud.ibm.com/docs/cognos-dashboard-embedded?topic=cognos-dashboard-embedded-gettingstartedtutorial), podemos crear gráficos a partir de la información almacenada en él, generada por las cloud functions.

Estos gráficos se consumirán en la interfaz, que también tiene una tabla para que el curador pueda evaluar la comprensión del asistente. Esta calificación - con un estándar que va del 1 al 5 - se utiliza para validar la calidad de la construcción y formación del asistente, ya que, al cruzar la confianza que tiene el asistente en la intención principal que entendió en la oración del cliente con la nota atribuida por el curador en esta misma sentencia, es posible validar la **Eficiencia** x **Eficacia** del asistente.

La eficiencia se entiende como "brindar una amplia cobertura de las preguntas" y la eficacia como "brindar las respuestas correctas".

## **Componentes**

Como se indicó anteriormente, este activo se compone de dos partes principales:

### **Cadena de cloud functions**

Compuesta por 5 funciones diferentes, la secuencia está organizada de forma que la salida de la primera es la entrada de la siguiente, y así sucesivamente. Son ellas:

- **create-tables-cf**

  Crea, si es necesario, las cuatro tablas que recibieron los registros procesados ​​en la instancia de Db2 especificada.

- **process-logs-cf**

  Realiza la extracción de información a nivel de registro (Paso 1 descrito en el tema de arquitectura)

- **process-conversations-cf**

  Agrupar registros a nivel de conversación y extraer información a nivel de conversación (Pasos 2 y 3)

- **enrich-cf**

  Consulta [**NLU**](https://cloud.ibm.com/docs/natural-language-understanding?topic=natural-language-understanding-getting-started) para extraer sentimientos del texto, almacenadas en forma numérica en valores que van desde **-1**, para sentimientos negativos, y **+1**, para sentimientos positivos. Además, esta función extrae las variables de contexto de las conversaciones (Paso 4)

- **insert-logs-cf**

  A partir de los objetos generados en las funciones anteriores, en este paso final se realiza la comunicación con las tres bases de datos: [**Cloudant**](https://cloud.ibm.com/docs/Cloudant?topic=Cloudant-ibm-cloud-public), [**COS**](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-getting-started-cloud-object-storage) y [**Db2**](https://cloud.ibm.com/docs/Db2onCloud?topic=Db2onCloud-getting-started). En Cloudant y COS se almacenan los logs para mantener su respaldo, en Db2 tenemos el almacenamiento de la información relevante filtrada y generada por las funciones.

### **Interfaz de curador**

La interfaz del curador tiene dos características principales:

- Presentar la conversación entre el cliente y el asistente en el nivel de intercambio de mensajes para la evaluación humana de su asertividad a través de la asignación de calificaciones (1 - 5);

- Proporcionar una interfaz para construir y consultar paneles interactivos que muestren el desempeño del asistente a lo largo del tiempo.

La interfaz curator se comunica con la instancia de Db2 a través del [**ibm_db**](https://www.npmjs.com/package/ibm_db), paquete para Node que funciona como la API de la instancia de Db2 seleccionada.

Habiendo recibido estos datos, tenemos la presentación de los registros, organizados en conversaciones, en forma de tabla:

![img](https://portal-de-demos-imgs.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator01.png)

También hay una pestaña dedicada para buscar mensajes según las intenciones identificadas en ellos, que muestra todas las intenciones identificadas y una barra de búsqueda para filtrar el resultado:

![img](https://portal-de-demos-imgs.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator02.png)

La comunicación con la instancia de Cognos Embedded también se realiza a través de su API, cuya documentación está disponible en los siguientes enlaces: [**CognosApi Docs**](https://dde-us-south.analytics.ibm.com/daas/jsdoc/cognosapi/CognosApi.html), [**CognosApi File**](https://dde-us-south.analytics.ibm.com/daas/CognosApi.js).

Una vez que se realiza la conexión, se inicia una sesión de Cognos para el usuario, que comienza a ver la interfaz de paneles disponible en la pestaña "Cognos" de la interfaz:

![img](https://portal-de-demos-imgs.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator04.png)

Los paneles, una vez abiertos y/o modificados, se pueden guardar en [**Cloudant**](https://cloud.ibm.com/docs/Cloudant?topic=Cloudant-ibm-cloud-public), para que estén disponibles entre accesos.

## **Requisitos previos**

Para continuar con esta documentación, debe tener una cuenta con [**IBM Cloud**](https://cloud.ibm.com/).

También se requiere una instancia de Watson Assistant, con un asistente implementado y algunos registros de conversación disponibles (de forma predeterminada, los registros de asistente para los planes Lite y Plus solo se almacenan durante 30 días).

Requiere instancias de [**Cloudant**](https://cloud.ibm.com/docs/Cloudant?topic=Cloudant-ibm-cloud-public), [**COS**](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-getting-started-cloud-object-storage) y [**Db2**](https://cloud.ibm.com/docs/Db2onCloud?topic=Db2onCloud-getting-started). Se recomienda desplegarlos todos en el propio IBM Cloud.

Para extraer opiniones de los mensajes enviados por el cliente, también es necesario instanciar el servicio de [**NLU**](https://cloud.ibm.com/docs/natural-language-understanding?topic=natural-language-understanding-getting-started).

El último servicio que se instanciará en IBM Cloud es [**Cognos Embedded**](https://cloud.ibm.com/docs/cognos-dashboard-embedded?topic=cognos-dashboard-embedded-gettingstartedtutorial), responsable por los Tableros que se construirán y presentarán en el frente.

Se recomienda saber cómo cargar código como [**cloud function**](https://cloud.ibm.com/docs/openwhisk) en IBM Cloud, pero este proceso se realizará automáticamente a través de [**terraform**](https://www.terraform.io/docs).

Finalmente, asegúrese de que [**Docker**](https://docs.docker.com/get-docker/) esté instalado en su máquina, ya que lo necesitaremos para cargar algunos contenedores y archivos.

## **Cómo realizar la implementación**

Para obtener documentación sobre cómo implementar este activo, consulte la siguiente guía paso a paso para instanciar automáticamente todos los servicios necesarios: [**terraform**](https://github.ibm.com/Innovation-Studio-Assets/assistant-curator/tree/master/terraform).

## **Guía del usuario**

Como se mencionó en temas anteriores, la interfaz del curador está conectada a tres servicios:

- [**Db2**](https://cloud.ibm.com/docs/Db2onCloud?topic=Db2onCloud-getting-started), con sus 4 tablas pobladas al ejecutar las cloud functions;

- [**Cognos Embedded**](https://cloud.ibm.com/docs/cognos-dashboard-embedded?topic=cognos-dashboard-embedded-gettingstartedtutorial), para crear y mostrar paneles;

- [**Cloudant**](https://cloud.ibm.com/docs/Cloudant?topic=Cloudant-client-libraries#client-libraries), para guardar los cambios realizados en los paneles.

Para conectarse a sus respectivas instancias, las credenciales de cada una de ellas deben ser informadas por el usuario a través de la interfaz del curador. Hay dos módulos de configuración para completar:

El primer modal de configuración se refiere a la conexión con **Db2** para recopilar datos de su tabla de registro. Se muestra al hacer clic en el ícono de ajustes ubicado en la esquina superior derecha del encabezado si el usuario está en la **pestaña de inicio** o en la **pestaña de intención de búsqueda**:

![img](https://portal-de-demos-imgs.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator03.png)

Para llenarlo se requieren las siguientes credenciales:

- **Connection String** de su instancia Db2:

  Aunque todos los datos necesarios para componer esta información se encuentran en las credenciales de servicio disponibles en la página de inicio de la instancia de **Db2** en **IBM Cloud**, no tiene el formato correcto. La **connection string** debe estar compuesta en esta sintaxis:

      DATABASE=<nome de su database>;HOSTNAME=<su hostname>;PORT=<port de su ejemplo>;PROTOCOL=TCPIP;UID=<su usuario de inicio de sesión>;PWD=<su contraseña de acceso>;Security=SSL;

- **Nombre de la tabla de registro**

  De forma predeterminada, el nombre de la tabla se establece en "**logs**".

El segundo modo de configuración se refiere a los datos necesarios para conectarse con **Cognos**, con **Db2 a través de Cognos** y con **Cloudant**, para (respectivamente) **presentar**, **popular** y **guardar** tableros. El se muestra haciendo clic en el mismo icono de engranaje, una vez que el usuario está en la pestaña **Cognos**:

![img](https://portal-de-demos-imgs.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator05.png)

Para llenarlo se requieren las siguientes credenciales:

- **Cognos**

  - Usuario de acceso a Cognos
  - Contraseña de acesso a Cognos

    Ambos datos están disponibles en las "credenciales de servicio" de la página de inicio de su instancia en IBM Cloud, aunque con nombres diferentes: el nombre de usuario se define como "**client_id**" y la contraseña como "**client_secret**".

- **Cloudant**

  - Nombre de la base de datos a consultar

    Puede establecer el nombre que desee, aunque por defecto espera "**dashboards**".

  - URL de su instancia de Cloudant
  - apiKey de acesso a Cloudant

    Ambos datos están disponibles en las "credenciales de servicio" de la página de inicio de su instancia en IBM Cloud.

- **Db2**

  - Enlace del módulo.xsd

    Como se explica en la documentación "trabajar con fuente de datos" disponible en este [**link**](https://cloud.ibm.com/docs/cognos-dashboard-embedded?topic=cognos-dashboard-embedded-workingwithdatasources).

  - URL de conexión a través del estándar JDBC

    Estos datos se pueden encontrar en la pestaña "credenciales de servicio" en la página de inicio de la instancia, aunque es necesario formatearlos. Su sintaxis debe ser como el siguiente ejemplo:

        jdbc:db2://<su hostname>:<su PORT>/<nombre de su database>:sslConnection=true;

  - Clase de controlador JDBC

    También se explica en la documentación "trabajar con fuente de datos" disponible en este [**link**](https://cloud.ibm.com/docs/cognos-dashboard-embedded?topic=cognos-dashboard-embedded-workingwithdatasources).

  - _Schema_ donde se guardan las tablas en Db2

    De forma predeterminada, el esquema de Db2 es su nombre de usuario en mayúsculas, a menos que se especifique lo contrario.

  - Usuario de acceso a Db2
  - Contraseña de acesso ao Db2

    Ambos datos están disponibles en las "credenciales de servicio" de la página de inicio de su instancia en IBM Cloud.

- **Nombre das Tabelas**

  En esta pestaña se esperan los nombres de las cuatro tablas guardadas en Db2. Por defecto, son, respectivamente:

  - **Logs**
  - **Conversations**
  - **Calls**
  - **ContextVariables**

Una vez rellenadas las credenciales por primera vez, no es necesario volver a hacerlo en los demás accesos, salvo excepciones.

En la pestaña "**Cognos**" tienes la interfaz de Dashboard interactiva que se puede guardar en Cloudant para que, en el próximo acceso, se carguen tus cambios desde la base de datos.

Para hacerlo, simplemente haga clic en el icono del disquete que abrirá dos opciones: **Guardar** o **Buscar** un tablero:

![img](https://portal-de-demos-imgs.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator06.png)

![img](https://portal-de-demos-imgs.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator07.png)

El nombre ingresado para guardarlo se establecerá como predeterminado, de modo que la próxima vez que se acceda a la pestaña Cognos, este será el tablero que se muestre inicialmente.

Si se carga otro dashboard mediante la opción **Buscar**, por defecto se elegirá este nuevo, sin que se elimine el previamente guardado.

En caso de que no haya un tablero guardado en el **Cloudant** consultado, se inicializará y mostrará un tablero estándar con visualizaciones ya disponibles.
