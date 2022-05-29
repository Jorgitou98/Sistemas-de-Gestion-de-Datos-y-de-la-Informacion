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

from mrjob.job import MRJob
import json
from collections import Counter
import string

class Noticias(MRJob):

    def mapper(self, key, line):
    
    	# Parseamos la linea que tiene formato json para obtener un diccionario python con los datos
    	line_dict = json.loads(line)
    	
    	"""
    	1) Sobre el campo "content" del diccionario (el que lleva el mensaje) aplicamos el método translate() indicandole que cambie
    	   los signos de puntuación por la cadena vacía (con ello los estamos eliminando)
    	2) Sobre el mensaje sin signos de puntuación aplicamos el método lower() que convierte todos los caracteres alfabéticos a minúscula
    	3) Finalmente, Counter sobre el mensaje sin signos de puntuación y en minúscula nos devuelve un diccionario con cada palabra y su 
    	   número de apariciones
    	"""
    	counter_words = Counter(line_dict["content"].translate(str.maketrans('', '', string.punctuation)).lower().split())
    	
    	
    	"""
    	Emitimos tantas pares como palabras haya en el diccionario. La clave de estos pares es la palabra y su valor es una tupla con el nombre de fichero
    	y el número de repeticiones que tenga la palabra en él.
    	""" 
    	for word, repetitions in counter_words.items():
    		yield(word, (line_dict["filename"], repetitions))
    
    
    
    
    def combiner(self, key, values):
    	"""
    	El combinador hace lo mismo que el reducer, emite un par que tiene la misma clave que recibe (la palabra) y cuyo valor es la tupla donde
    	se alcanza el número de repeticiones máximo (el número de repeticiones es el campo 1 de las tuplas de values).
    	"""
    	yield(key, max(values, key = lambda t: t[1]))

    def reducer(self, key, values):
    	"""
    	El reducer emite un par que tiene la misma clave que recibe (la palabra) y cuyo valor es lla tupla donde
    	se alcanza el número de repeticiones máximo. Así, de todas las tuplas (fichero, repeticiones) para una palabra, estamos escogiendo el
    	fichero en el que más se repite junto a dicho número de repeticiones máximo.
    	"""
    	yield(key, max(values, key = lambda t: t[1]))


if __name__ == '__main__':
    Noticias.run()

