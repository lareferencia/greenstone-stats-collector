# greenstone-stats-collector

Greenstone platform usage stats sender.

## Uso

1. El proceso se inicia con `python main.py`, pero el trabajo se ejecuta mayormente en nuevoProceso.py

1. El algoritmo descarga el archivo de documentos válidos exportados del backend (Isis) en `idtitulos.txt`.

    De este documento se extrae el título del documento que no está en la base.

1. Se crea el archivo `debug.log`.

1. Se lee el archivo que determina el estado del proceso, `seguimiento_rsnd.txt`.

    Estructura del archivo de seguimiento:

    ```
    2021-01-04 00:00:00|lasIdRow|0000-00-00 00:00:00
    ```

    donde se detalla a partir desde que día y row de la base debe ejecutarse el script

    Este archivo se va grabando a medida que se procesan correctamente los bloques de request contra el Matomo del RSND.

    Si se genera un error, se graba el estado hasta el que se llegó y se envía un raise, que detiene el script para que se le dé seguimiento manualmente.

    Si el proceso se completa, el archivo quedará solo con la fecha de finalización.

1. Se hace una lista de días (porque la idea inicial era hacer un envío del histórico desde el 2020) y se van consultando los eventos ocurridos para cada día.

1. Se consolida el dato del evento con el "título del registro"

1. Se carga el buffer de eventos. Cuando se llena, se envía.

## TODO

- Manejo de errores.
- Registro de eventos, qué información debe guardar.
- Envío por email.
