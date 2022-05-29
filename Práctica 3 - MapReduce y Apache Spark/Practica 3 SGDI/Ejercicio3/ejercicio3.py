"""
Asignatura: Sistemas de gestión de datos y de la información
Práctica 3: MapReduce y Apache Spark
Grupo 06
Autores: Beatriz Herguedas Pinedo y Jorge Villarrubia Elvira

Declaramos que esta solución es fruto exclusivamente de nuestro tra-
bajo personal. No hemos sido ayudados por ninguna otra persona ni
hemos obtenido la solución de fuentes externas, y tampoco hemos
compartido nuestra solución con otras personas. Declaramos además
que no hemos realizado de manera deshonesta ninguna otra actividad
que pueda mejorar nuestros resultados ni perjudicar los resultados de
los demás.
"""


import sys
from pyspark.sql import SparkSession


def apartado1(id_ratingIMDB, scriptLines):

	"""
	1) Seleccionamos el id de episodio y el id de localización.
	2) Nos quedamos solo con los que sean distintos (queremos contar localizaciones diferentes por cada episodio).
	3) Agrupamos por identificador de episodio.
	4) Contamos (función de agregación) cuantos elementos (localizaciones diferentes) hay en cada grupo.
	5) Renombramos la columna con el número de localizaciones a "num_locs".
	"""	
	capId_numLocs = scriptLines.select("episode_id", "location_id").distinct().groupBy("episode_id").count().withColumnRenamed("count","num_locs")


	"""
	1) Hacemos un join entre las tablas según "episode_id" (queremos que sea inner join porque hay muchos capítulos sin valoración imdb que no deben aparecer en el resultado).
	2) Calculamos el coeficiente de correlación entre las columnas con la puntuación IMDB y el número de localizaciones.
	"""	
	corr_ratingIMDB_numLocs = id_ratingIMDB.join(capId_numLocs, "episode_id").corr("imdb_rating", "num_locs")

	# Mostramos con 4 decimales el coeficiente obtenido por pantalla
	print("Correlación entre el rating IMDB y el número de localizaciones:", "{:.4f}".format(corr_ratingIMDB_numLocs))





def apartado2(id_ratingIMDB, scriptLines, femaleCharacters):


	# De la tabla con personajes femeninos no quedamos solo con el id de personaje (será lo único que haga falta para contar cuántos personajes femeninos hay en cada episodio).
	idfemaleCharacters = femaleCharacters.select(femaleCharacters.id.alias("character_id"))


	"""
	1) Del Dataframe con los las líneas de los capítulos, seleccionamos el id de episodio y el id de caracter.
	2) Hacemos un inner join entre el dataframe anterior y la tabla con los ids de personajes femeninos (solo queremos los que tengan id en ambas tablas).
	3) Nos quedamos solo con las filas distintas (un personaje puede aparecer en un capítulo varias veces y solo habrá que contarlo una vez).
	4) Agrupamos por id de episodio.
	5) Contamos (función de agregación) cuantos elementos (personajes femeninos diferentes) hay en cada grupo.
	6) Renombramos el resultado de ese count a "numFem".
	7) Hacemos un rightouter join con la tabla de la puntuación IMDB según el id de episodio. Queremos que sea rightouter porque queremos que aparezcan con null
	los capítulos con 0 personajes femeninos (habrá que sacar la correlación también teniendolos en cuenta a ellos).
	8) Cambiamos los "num_fem" a null (no hay personajes femeninos) por el valor '0'.
	"""
	epId_numFem_ratingIMDB = scriptLines.select("episode_id", "character_id").join(idfemaleCharacters, "character_id").distinct().groupBy("episode_id").count().withColumnRenamed("count","num_fem").join(id_ratingIMDB, "episode_id", "rightouter").na.fill({'num_fem':0})



	# Calculamos y mostramos con 4 decimales el coeficiente de correlación entre las columnas con la puntuación IMDB y el número de personajes femeninos.
	corr_ratingIMDB_numFem = epId_numFem_ratingIMDB.corr("imdb_rating", "num_fem")
	print("Correlación el rating IMDB y el número de personajes femeninos:", "{:.4f}".format(corr_ratingIMDB_numFem))






def apartado3(id_ratingIMDB, scriptLines):

	"""
	1) Filtramos quedándonos solo con las filas de scriptLines que sean diálogos (speaking_line == 'true').
	2) Seleccionamos las columnas "episode_id" y "word_count", que son las que utilizaremos.
	3) Agrupamos por el identificador de episodio.
	4) Utilizamos las funciones de agregación "sum" sobre la columna w"ord_count" para contar cuantas palabras hay en total por cada grupo (por cada episodio) y 
	   la función de agregación "count" sobre todas las filas para saber cuantos diálogos hay por cada grupo (por cada episodio).
	5) Renombramos la columna con el número de palabras a "total_word_count" y la columna con el número de dialogos a "total_dialogs".
	6) Hacemos un inner join con el dataframe "id_ratingIMDB" según la columna "episode_id" (debe ser inner porque el episodio debe estar en ambas tablas).
	"""
	epId_ratingIMDB_numDialogs_numTotalWords = scriptLines.filter("speaking_line == 'true'").select("episode_id", "word_count").groupBy("episode_id").agg({"word_count": "sum", "*": "count"}).withColumnRenamed("sum(word_count)","total_word_count").withColumnRenamed("count(1)","total_dialogs").join(id_ratingIMDB, "episode_id")



	# Calculamos el coeficiente de correlación entre la puntuación IMDB y el total de diálogos
	corr_ratingIMDB_numDialogs = epId_ratingIMDB_numDialogs_numTotalWords.corr ("imdb_rating", "total_dialogs")

	# Calculamos el coeficiente de correlación entre la puntuación IMDB y el total de palabras en diálogos
	corr_ratingIMDB_totalWordsDialogs = epId_ratingIMDB_numDialogs_numTotalWords.corr ("imdb_rating", "total_word_count")
	
	
	# Mostramos con 4 decimales los coeficientes por pantalla
	print("Correlación rating IMDB y el número total de diálogos:", "{:.4f}".format(corr_ratingIMDB_numDialogs))
	print("Correlación rating IMDB y las palabras totales en diálogos:", "{:.4f}".format(corr_ratingIMDB_totalWordsDialogs))




def main():

	spark = SparkSession.builder.getOrCreate()
	
	
	# Para que no muestre tantos mensajes "INFO" por pantalla y se vea claramente la salida.
	spark.sparkContext.setLogLevel("ERROR")
	
	
	# Leemos el fichero con los episodios en un Dataframe (inforSchema=True para que deduzca el tipo y no ponga string en todos).
	episodes = spark.read.options(header=True, inferSchema=True).csv("simpsons_episodes.csv")


	"""
	Seleccionamos el id de episodio y la puntuación IMDB (las usaremos en todos los apartados).
	¡Cuidado! Hay 3 filas sin campo imdb_rating que nos quitamos con un filter.
	"""
	id_ratingIMDB = episodes.select(episodes.id.alias("episode_id"), "imdb_rating").filter("imdb_rating is not NULL")



	# Leemos el fichero "simpsons_script_lines.csv" en un Dataframe
	scriptLines = spark.read.options(header=True, inferSchema=True).csv("simpsons_script_lines.csv")



	apartado1(id_ratingIMDB, scriptLines)


	"""
	1) Leemos el fichero sobre personajes en un Dataframe.
	2) Nos quedamos con los que tienen género femenino  (son los únicos que usaremos) mediante un filter.
	"""
	femaleCharacters = spark.read.options(header=True, inferSchema=True).csv("simpsons_characters.csv").filter("gender=='f'")
	apartado2(id_ratingIMDB, scriptLines, femaleCharacters)



	apartado3(id_ratingIMDB, scriptLines)
 

    
if __name__ == "__main__":
    main()

