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
import csv

def main():
	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext
	
	# Para que no muestre tantos mensajes de "INFO" por pantalla y se vea clara la salida por pantalla.
	sc.setLogLevel("ERROR")
	
	
	# Cargamos un RDD con los datos del fichero simpsons_script_lines.csv
	lines = sc.textFile("simpsons_script_lines.csv")
	
	"""
	1) Filtramos las líneas quedanondos con las que sean distintas de la primera (para quitarnos la cabecera del CSV)
	2) Mapeamos la operación que parsea el csv (el método reader() ya tiene en cuenta que pueda haber ',' entre ""). Esta operación recibe una 
	   lista, así que le pasamos la lista unitaria [list ]con la línea y, tras el correspondiente casting, nos devuelve una lista de listas de
	   la que queremos la lista que está en la posición 0.
	3) Como en el fichero original hay algunas filas que están mal (se abren comillas que no se cierran), filtramos lasfilas de nuestro RDD 
	   quedándonos con las que tienen exactamente tamaño 13 (los campos que debe tener una fila correcta). Con ello, apenas perdemos 46 filas de 150.000.
	4) A cada fila del RDD (fila del csv que se ha convertido en una lista con los campos), sobre el split de su campo 11 (el que lleva el
	   normalized_text) mapeamos la función que crea tuplas con cada elemento (cada palabra del normalized_text) y con el identificador de
	   capítulo (esto lo hace "map(lambda word : (word, int(l_list[1])), l_list[11].split())"). Como esto se lo queremos hacer a cada fila del RDD
	   podríamos pensar a su vez en un map, pero tendríamos una lista de pares (palabra, id de capítulo) que queremos dejar en filas distintas de un RDD,
	   y por eso utilizamos la operación flatMap.	 
	"""
	
	header = lines.first()
	lines = lines.filter(lambda line : line != header)\
		.map(lambda line : list(csv.reader([line]))[0])\
		.filter(lambda l_list : len(l_list) == 13)\
		.flatMap(lambda l_list : map(lambda word : (word, int(l_list[1])), l_list[11].split()))
		
		

	"""
	1) Cargamos el fichero happiness.txt en un RDD
	2) Spliteamos cada entrada por espacios (es la separación que tienen los campos en el fichero)
	3) Mapemos la función que deja la tupla formada por el campo 0 del split (la palabra) y el campo 2 (su felicidad media).
	"""

	happy = sc.textFile("happiness.txt")\
		.map(lambda line : line.split())\
		.map(lambda fila : (fila[0],float(fila[2])))
		
		
		
	"""
	1) Hacemos un inner join entre los RDD's de pares anteriores (en ambos pusimos la como clave la palabra). Así, obtendremos filas del RDD
	   de la forma (palabra, (id capitulo, felicidad media)) solamente para palabras que aparezcan en algún capítulo y par las que conocemos su felicidad media.
	2) Como la palabra no nos interesa y solo queremos (id capitulo, felicidad media), mapeamos la función que toma la componente 1 de las tuplas.
	3) Como queremos sumar las felicidades para el mismo id de capítulo obteniendo así la felicidad acumulada, hacemos un reduceByKey con la función que suma.
	4) Ordenamos las filas del RDD descendentemente según la primera componente de las tuplas (la felicidad acumulada).
	"""

	cap_punt = lines.join(happy)\
		   .map(lambda tupla : tupla[1])\
		   .reduceByKey(lambda happy1, happy2 : happy1 + happy2)\
		   .sortBy(keyfunc = lambda tupla : tupla[1], ascending = False)

	# Mostramos por pantalla los resultados (en un ambiente real no se debe hacer un collect porque no cabrán todos los datos distribuidos en la memoria del driver)
	for num_cap, happiness in cap_punt.collect():
		print(num_cap, "{:.2f}".format(happiness))

if __name__ == "__main__":
    main()

