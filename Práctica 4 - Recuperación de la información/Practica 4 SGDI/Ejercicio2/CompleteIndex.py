"""
Asignatura: Sistemas de Gestión de Datos y de la Información
Práctica 4: Recuperación de información
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


import string
import os
import itertools
import time
from operator import itemgetter
from collections import defaultdict
from pprint import pprint

"""
Dada una linea de texto, devuelve una lista de palabras no vacias 
convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
 Ejemplo:
   > extrae_palabras("Hi! What is your name? John.")
   ['hi', 'what', 'is', 'your', 'name', 'john']
"""
def extrae_palabras(linea):
  return filter(lambda x: len(x) > 0, 
    map(lambda x: x.lower().strip(string.punctuation), linea.split()))

"""
Es más correcto que las siguientes funciones no vayan en la clase, pues no son métodos de la clase ya que no
utilizan niguno de sus atributos.
"""

"""
Para ver si hay apariciones consecutivas vamos a utilizan la intersección. Si tenemos n listas
y queremos ver si contienen algún elemento consecutivo basta, lista por lista, restarle k a los elementos de
la lista k-ésima y el problema se reduce a ver si tienen algún elemento en común. Como la intersección será
también sobre listas ordenadas, copiamos las funciones de que usamos en VectorialIndex, que usaremos de nuevo.
"""
def intersect(l1, l2):
	sol = []
	while l1 != [] and l2 != []:
	
		if l1[0] == l2[0]:
		
			sol.append(l1[0])
			l1 = l1[1:]
			l2 = l2[1:]
			
		elif l1[0] < l2[0]:
		
			l1 = l1[1:]
			
		else:
			l2 = l2[1:]
			
	return sol

def intersect_multiple(l_lists):

	if l_lists == []:
		return []

	l_lists = sorted(l_lists, key = len)
	
	# Sabemos que l_lists != [] y podemos acceder a su posición 0
	sol = l_lists[0]
	terms = l_lists[1:]
	
	while terms != [] and sol != []:
		sol = intersect(sol, terms[0])
		terms = terms[1:]
		
	return sol

"""
Como ya hemos comentado, para comprobar si en una lista de listas, dichas listas tienen terminos cosecutivos,
restamos a cada uno de sus elementos la posición que ocupa la lista. (A los elementos de la lista k-ésima,
numerando las listas desde 0, les restamos k). Luego, basta ver si hay un elemento en la intersección de todas o no.
"""
def consecutive(l_list):
	# A los elementos de la lista que ocupa la posición p les restamos p
	l_list = [list(map(lambda x : x-p, l_list[p])) for p in range(len(l_list))]
	# Devolvemos True si la intersección no es vacía y False en caso contrario
	return intersect_multiple(l_list) != []
	


"""
Dada una lista "l", esta función devuelve otra lista con los índices de los elementos que sin mínimos en "l".
Esta fucnión es útil para que "intersect_phrase" decida cuando en la cabeza de todas las listas hay el mismo
documento y saber qué listas debe avanzar en cada paso.
"""
def getIndexesMinimums(l):
	# Guardamos el una variable el valor mínimo de la lista
	minimo = min(l)
	# Devolvemos una lista con los índices donde el valor coincide con el mínimo
	return [index for index in range(len(l)) if l[index] == minimo]


"""
Esta función recibe un elemento de tipo [[(Int, [Int])]], es decir, una lista de listas (cada una es la entrada del
índice para una palabra), donde las listas internas llevan tuplas (Int, [Int]) (documento en el que aparece la palabra
y lista de apariciones en ese documento). La función devuelve una lista con los Int de la primera componente de las tuplas
(los documentos) que estén en todas las listas y cuyas listas [Int] asociadas en la segunda componente de las tupla tengan
elementos consecutivos.

Por ejemplo, [[(1,[3]),(2,[8,9])],[(2,[10])],[(1,[2]),(2,[4,11])]] devolvería la lista [2] porque el documento 2 aparece
en todos las listas internas como primera componente de las tuplas (dicho documento constiene todas las palbras de la
consulta) y sus listas asociadas ([8,9], [10], [4,11]) tienen elementos consecutivos 9, 10 y 11.
"""
def intersect_phrase(l_list):
	# Inicalizamos la solución a una lista vacía
	answer = []
	# Mientras ninguna de las listas internas sea la vacía
	while all(l_list):
		"""
		Obtenemos los índices donde se alcanza el mínimo de la lista formada por la primera componente de la primera
		tupla de cada lista (el primer documento de cada palabra). Para ello, mapeamos la función "lambda t: t[0][0]" y
		llamamos a getIndexesMinimums.
		"""
		indexesMinimums = getIndexesMinimums(list(map(lambda t: t[0][0], l_list)))
		"""
		Si hay tantos índices con el mínimo, como posiciones en la lista, es que todas tienen el mismo valor y estamos
		ante un documento que contiene todas las plabras de la cosulta
		"""		
		if len(l_list) == len(indexesMinimums):
			"""
			Llamamos a "consecutive" con la lista de listas formada por las segundas componentes de los documentos
			cabeza de cada palabra. En el caso de [[(2,[8,9])],[(2,[10]), (3,[7])],[(2,[4,11])]] llamaríamos
			con la lista [[8,9], [10], [4,11]].
			"""
			if consecutive(list(map(lambda l : l[0][1], l_list))):
				"""
				Si en todas aparece el mismo documento y en posiciones consecutivas añadimos el documento a la
				solución (como en todas las listas el primer documento es el mismo para obtenerlo podemos
				acceder a la primera de ellas, a su primer documento, a la primera componente de la tupla)
				"""
				answer.append(l_list[0][0][0])
		
		"""
		Coincidisen a no los primeros documentos de las listas avanzamos aquellas en que se tenga el documento mínimo.
		Para ello nos apoyamos en "indexesMinimums" que obtuvimos anteriormente.
		"""
		for index in indexesMinimums:
			l_list[index] = l_list[index][1:]	
	return answer		
	
	

class CompleteIndex(object):

    def __init__(self, directorio, compresion=None):
        	
    	# Numeraremos los documentos para almacenarlos como enteros y no su ruta completa. Inicializamos un contador a 0 para ello.
    	numDoc = 0
    	
    	
    	# Tendremos una lista de str que en la posición k guarda la ruta del documento k-ésimo según nuestra representación interna.
    	self.rutaFich = []
    	
    	
    	"""
    	Inicializamos el índice inverso completo, que será un diccionario (str->[(int, [int])]), cuyas claves por defecto
    	son listas vacías
    	"""
    	self.completeIndex = defaultdict(list)
    	
    	"""
    	En caso de que el directorio no exista o no esté a la altura del .py "os.walk" no falla. Añadimos Este código para
    	indicarselo al usuario por pantalla.
    	"""
    	if not os.path.exists(directorio):
    		print('ERROR: el directorio que has indicado no existe a esta altura.')
    		exit()
    	
    	# Recorremos recursivamente todos los documentos del directorio con os.walk()
    	for dirpath, _ , filenames in os.walk(directorio):
    	
    		# Para cada documento de la lista filenames (cadena con nombres de fichero en la ruta dirpath) 
    		for filename in filenames:
    		
    			"""
    			Obtenemos la ruta absoluta de un documento concatenando con '/' la ruta de su directorio y su nombre
    			y la almacenamos en la traducción. Como empezamos a numerar documentos desde 0, basta hacer un append.
    			"""
    			self.rutaFich.append(os.path.join(dirpath, filename))
    			

    			"""
    			Abrimos el documento con codificación 'utf−8' y tratamos los errores remplazándolos por
    			'?' (errors='replace' hace eso)
    			"""
    			with open(self.rutaFich[numDoc], encoding='utf−8', errors= 'replace') as f:
    				"""
    				A la lista con las líneas del fichero le mapeamos la función que extrae palabras, aplanamos
    				las listas internas con la función "chain", y de esta forma obtenemos una lista con las palabras
    				del documento colocadas en la posición en que aparecen en él. Con "enumerate" se transforma en
    				una lista de pares donde la primera componente es la posición empezando en 1 (parámetro start=1
    				en enumarate). Recorremos esa lista de pares (posición, palabra).
    				"""
    				for pos, word in enumerate(itertools.chain(*map(extrae_palabras, f.readlines())), start=1):
    					"""
    					Si es la primera vez que encontramos la palabra (no está en el índice) o el número de
    					documento en que se enocontró la última vez no es el número de documento actual 
    					(self.completeIndex[word][-1][0] != numDoc).
    					"""
    					if self.completeIndex[word] == [] or self.completeIndex[word][-1][0] != numDoc:
    						"""
    						Añadimos a la palabra una tupla con el documento y una lista unitaria con la posición
    						"""
    						self.completeIndex[word].append((numDoc, [pos]))
    					else:
    						"""
    						Si no, añadimos la posición detrás de la última tupla para la palabra
    						(self.completeIndex[word][-1][1])
    						"""
    						self.completeIndex[word][-1][1].append(pos)
    					
    					
    			#Aumentamos el nº de fichero para la siguiente iteración	
    			numDoc += 1

    def consulta_frase(self, frase):
    	"""
    	Aplicamos la función extrae_palabras a la frase para obtener las distintas palabras que aparecen en ella. Construimos una
    	lista con el valor de cada una de ella en el índice inverso completo (respetando el orden de aparición). Llamamos a
    	"intersect_phrase" con esa lista para que devuelva los documentos (como enteros) en los que hay apariciones consecutivas
    	de las palabras de la frase. Finalmente, traducimos los documentos cambiando su número interno por su ruta. Para ello,
    	aplicamos la función que coge un item de una lista según su posición sobre la lista de items self.rutaFich, dándole las
    	posiciones devueltas por intersect_phrase.
    	"""
    	return list(itemgetter(*intersect_phrase([self.completeIndex[word] for word in extrae_palabras(frase)]))(self.rutaFich))

    
if __name__ == "__main__":
	start = time.time()
	i = CompleteIndex('20news-18828')
	end = time.time()
	print('Tiempo:', end - start, 'segundos')
	
	consulta1 = 'either terrestrial or alien'
	print('Consulta completa:', consulta1)
	pprint(i.consulta_frase(consulta1), compact = False)
	
	consulta2 = 'is more complicated'
	print('Consulta completa:', consulta2)
	pprint(i.consulta_frase(consulta2), compact = False)
	
