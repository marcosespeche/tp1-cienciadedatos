# Astro Pokémon - Proyecto Airflow

Este proyecto utiliza Apache Airflow y Astro CLI para construir un pipeline de extracción, transformación y consolidación de datos sobre Pokémon, empleando llamadas a APIs públicas. Su propósito es servir como ejemplo didáctico para la materia **Ciencia de Datos** de la carrera de Ingeniería en Sistemas de la UTN FRM, mostrando cómo automatizar flujos de trabajo de análisis de datos.

---

## 🗂 Estructura del proyecto

```
astro-pokemon/
├── dags/                     # DAGs de Airflow (pipelines)
│   ├── pokedag_paralelo.py  # DAG principal con paralelismo
│   └── .airflowignore       # Archivos ignorados por Airflow
├── tests/                   # Tests para los DAGs
├── .astro/                  # Configuración del proyecto Astro
├── Dockerfile               # Imagen personalizada para Airflow
├── requirements.txt         # Paquetes de Python requeridos
├── packages.txt             # Dependencias del sistema
├── airflow_settings.yaml    # Configuración inicial de Airflow (conexiones, etc.)
├── .env                     # Variables de entorno
├── README.md                # Documentación del proyecto
```

---

## ⚙️ Requisitos

* Docker
* Astro CLI (`astro dev`)
* Git (opcional, para clonar el repo)

Instalación de Astro CLI: [https://docs.astronomer.io/astro/cli/install-cli](https://docs.astronomer.io/astro/cli/install-cli)

---

## 🚀 Cómo ejecutar el proyecto

```bash
# 1. Clonar el repositorio
$ git clone https://github.com/usuario/astro-pokemon.git
$ cd astro-pokemon

# 2. Instalar dependencias necesarias
# (Solo una vez por máquina, según el sistema operativo)

## En Windows:
- Instalar Docker Desktop: https://www.docker.com/products/docker-desktop
- Instalar Astro CLI: https://docs.astronomer.io/astro/cli/install-cli#windows

## En Linux:
- Instalar Docker Engine: https://docs.docker.com/engine/install/
- Instalar Astro CLI: https://docs.astronomer.io/astro/cli/install-cli#linux

# 3. Iniciar el entorno
$ astro dev start

# 4. Abrir el navegador
# Ir a http://localhost:8080 (Airflow)
# Usuario: admin | Contraseña: admin (por defecto)
```

---

## 📈 Qué hace el DAG

El DAG `pokedag_paralelo.py` automatiza el siguiente flujo:

1. **Extracción de datos** desde dos endpoints:

   * `/pokemon` (información principal)
   * `/pokemon-species` (características complementarias)

2. **Almacenamiento temporal** en archivos JSON (`/tmp/pokemon_data/`).

3. **Transformación y merge**: los datos se combinan y se exportan como `pokemon_merged.csv`.

4. (Opcional) Exportar resultados localmente con `docker cp`.

---

## 🔍 Archivos clave

* `dags/pokedag_paralelo.py`: define tareas, dependencias y paralelismo.
* `airflow_settings.yaml`: permite precargar conexiones y variables.
* `.env`: almacena valores sensibles o configurables.
* `requirements.txt`: si se agregan librerías adicionales (pandas, etc.).

---

## 🧪 Tests

Los DAGs tienen test básico en `tests/dags/test_dag_example.py`. Podés agregar tests funcionales con `pytest` para cada función.

---

## 🧑‍🏫 Propósito educativo

Este proyecto está pensado como material de clase para enseñar:

* El uso de DAGs en flujos ETL.
* La ejecución paralela de tareas en Airflow.
* La interacción con APIs públicas.
* El trabajo con entornos reproducibles en contenedores.
* La trazabilidad y modularización de procesos.

---

## 📤 Exportar resultados

Para copiar el CSV generado a tu máquina local:

```bash
docker cp astro-pokemon_XXXX-scheduler-1:/tmp/pokemon_data/pokemon_merged.csv ./pokemon_merged.csv
```

Reemplazá `XXXX` por el ID correspondiente del contenedor (usá `docker ps`).

---

## 📌 Notas adicionales

* El contenedor usa Airflow 2.x y Python 3.12.
* Se puede extender fácilmente para incluir visualización o carga a una base de datos.
* Está preparado para ejecutarse en entorno local con Docker Compose vía Astro.

---

## 📎 Licencia

MIT - Usalo libremente con fines educativos.
