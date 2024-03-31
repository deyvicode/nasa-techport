# Data Engineering
Proyecto del curso online Data Engineering de Coderhouse

## TechPort API
En este proyecto usamos la API TechPort de la NASA, servicios web RESTful para que los datos de proyectos tecnológicos estén disponibles para otros sistemas y servicios.

## Instrucciones
Para instalar este proyecto en un entorno local (su computador) abra la terminal de comandos y siga estos pasos:

**1. Clonar el repositorio** \
Para este paso es necesario tener instalado [Git](https://git-scm.com/).
``` shell
git clone 
```
**2. Variables de entorno** \
Al ejecutar el projecto necesitaremos credenciales de conexion a la base de datos Redshift. 
* Haremos una copia del archivo `.env.example` y lo nombraremos como `.env`
* En el archivo `.env` ingresaremos las credenciales faltantes de usuario y contraseña de Redshift