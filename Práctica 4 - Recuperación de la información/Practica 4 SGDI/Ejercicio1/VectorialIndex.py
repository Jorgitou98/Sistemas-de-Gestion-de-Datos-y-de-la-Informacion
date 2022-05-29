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
from collections import Counter, defaultdict
import math
from operator import itemgetter
from pprint import pprint
import time

"""
Dada una linea de texto, devuelve una lista de palabras no vacias 
convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
 Ejemplo:
   > extrae_palabras("Hi! What is your name? John.")
   ['hi', 'what', 'is', 'your', 'name', 'john']
"""
def extrae_palabras(linea):
	return filter(lambda x: len(x) > 0, map(lambda x: x.lower().strip(string.punctuation), linea.split()))


"""
Es más correcto que las siguientes funciones no vayan en la clase, pues no son métodos de la clase ya que no
utilizan niguno de sus atributos.
"""

"""
Recibe un par de listas de enteros ordenadas (crecientemente) y devuelve una lista de enteros con la intersección de ambas
también ordenada (crecientemente). Para ello, avanza en la lista que tenga el entero más pequeño (debido al orden sólo
hay que mirar la primera posición de cada una), hasta que alguna de las listas se quede vacía. Si el entero más pequeño
en las dos listas coincide, se añade a la solución y se avanza en las dos listas.
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

"""
Recibe una lista de listas de enteros ordenados crecientemente y devuelve una lista de enteros ordenados también crecientemente
compuesta por los valores que están en la interesección de todas las listas recibidas.

Primeramente, se ordenan crecientemente según su tamaño las listas a intersecar. Esta heurística hace que se intersequen primero
las listas más pequeñas, lo que tiende a dar intersecciones más pequeñas y hace que los cálculos sean más rápidos (se reutiliza
intersect, que tiene coste lineal en la suma de los tamaños de las dos listas, y es deseable que reciba listas lo más pequeñas
posibles lo antes posible).
"""
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
Función que calcula el tf-idf.
"""
def tf_idf(f_ij, N, n_i):
	return (1 + math.log2(f_ij)) * math.log2(N/n_i)
	
	
"""
Función que, dado un número "k" y una lista de pares "score", devuelve los k pares de la lista socre con mayor valor en
su segunda componente.
"""	
def best(k, scores):
	return sorted(scores, key = itemgetter(1), reverse = True)[0:k]



class VectorialIndex(object):

    """
    Crea el índice vectorial invertido con los documentos que estén a cualquier altura dentro del directorio
    que recibe como parámetro (el índice se construye en el atributo "self.vectorialIndex"). Además, precalcula
    para cada documento la norma de su vector de tf-idfs (será necesaria en las consultas y es preferible precalcularla).
    """
    def __init__(self, directorio):
    	
    	#Numeraremos los documentos para almacenarlos como enteros y no su ruta completa. Inicializamos un contador a 0 para ello.
    	numDoc = 0
    	
    	
    	# Tendremos una lista de str que en la posición k guarda la ruta del documento k-ésimo según nuestra representación interna.
    	self.rutaFich = []
    	
    	
    	"""
    	Inicializamos el índice vectorial inverso como un diccionario (str->[(int, float)]), cuyas claves por defecto
    	son listas vacías.
    	"""
    	self.vectorialIndex = defaultdict(list)
    	
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
    				las listas internas con la función "chain" y utilizamos la clase Counter para obtener un
    				diccionario (palabra -> nº de apariciones de la palabra en el documento).
    				"""
    				count_words = Counter(itertools.chain(*map(extrae_palabras, f.readlines())))
    				"""
    				Para cada palabra, añadimos la tupla (nº de fichero, frecuencia en el documento) "por detrás" en
    				su entrada del índice inverso. Puesto que estamos numerando los ficheros crecientemente y los
    				añadimos al índice según los procesamos, añadir "por detrás" garantiza que las tuplas para de cada
    				palabra están ordenadas según el nº de fichero.
    				"""
    				for key, freq in count_words.items():
    					self.vectorialIndex[key].append((numDoc, freq))
    					
    					
    			#Aumentamos el nº de fichero para la siguiente iteración	
    			numDoc += 1
    	
    	
    	
    	
    	# Inicializamos a 0's una lista donde precalcular la norma de los vectores para los distintos documentos.
    	self.normDocs = [0] * numDoc
    	
    	# Para cada elemento del índice invertido (palabra, [(nº doc, frecuencia de palabra en doc)])
    	for word, list_documents in self.vectorialIndex.items():
    	
    		"""
    		Sobre la lista [(nº doc, frecuencia de palabra en doc)], mapeamos la función que deja igual las palabras y
    		cambia la frecuencia por el tf-idf de la palabra en el documento. Para ello, llamamos a la función "tf_idf"
    		con la frecuencia en el documento, el número total de documentos y el nº de documentos en los que aparece
    		alguna vez la palabra ("tf_idf(pair[1], numFich, len(list_documents)))").(Hacer el cálculo del tf-idf de
    		manera vectorial con numpy habría sido muy eficiente, pero nos resultó difícil adaptar nuestra solución a ello).
    		"""
    		self.vectorialIndex[word] = list(map(lambda pair: (pair[0], tf_idf(pair[1], numDoc, len(list_documents))), list_documents))
    		
    		"""
    		Para cada documento en el que aparezca alguna vez la palabra, acumulamos el cuadrado del tf-idf que acabamos
    		de calcular, para así obtener la norma del vector tf-idfs del documento (la raiz cuadrada no se puede hacer
    		hasta haber sumado todos).
    		"""
    		for doc, tfidf in self.vectorialIndex[word]:
    			self.normDocs[doc] += tfidf**2
    	
    	"""
    	Hacemos la raiz cuadrada de de los valores de self.normDocs para dejar finalmente la lista con las normas de los
    	vectores tf-idf de los documentos
    	"""
    	self.normDocs = list(map(math.sqrt, self.normDocs))
    
 
 
    
    
    def consulta_vectorial(self, consulta, n=3):
    

    	"""
    	Inicializamos un diccionario que tendrá los documentos como clave y como valor el producto escalar del vector tf-idf de
    	cada documento y la consulta. Los valores por defecto son 0.0.
    	"""
    	doc_prodEscalar = defaultdict(float)
    	
    	# Para cada palabra en la consulta (usamos de nuevo "extrae_palabras").
    	for word in extrae_palabras(consulta):
    	
    		"""
    		Para cada documento en el que aparece la palabra sumamos su tf-idf al producto escalar con el vector del documento.
    		No hay que preocuparse de los documentos donde no aparece la palabra porque el sumando sería 0.
    		"""
    		for document, tfidf in self.vectorialIndex[word]:
    			doc_prodEscalar[document] += tfidf
    	
    	"""
    	Obtenemos la relevancia de cada documento dividiendo su producto escalar obtenido para consulta, entre la norma correspondiente
    	de su vector tf-idf. No hace falta dividir por la norma de la consulta ya que es una constante (el orden de relevancia
    	de documentos no se ve alterado).
    	"""
    	scores = [(document, prodEsc/self.normDocs[document]) for document, prodEsc in doc_prodEscalar.items()]
    	
    	
    	""" 
    	Llamamos a la la función "best" con el número n de elementos que hay que mostrar y la lista de parejas
    	(documento, puntuación). Nos devolverá las n parejas con mayor puntuación, cuya primera componente
    	(el nº de documento) traducimos a su ruta absoluta.
    	"""
    	return list(map(lambda t: (self.rutaFich[t[0]], t[1]), best(n, scores)))
    	
    	
    	
    	
    def consulta_conjuncion(self, consulta):
    	"""
    	Al valor en el índice invertido de cada palabra de la consulta (parseada con extrae_palabras) le mapeamos la función que
    	nos da la primera componente de una tupla; con ello obtenemos una lista ordenada con los documentos en los que aparece la palabra.
    	Estas listas, para cada una de las plabras, se acumulan a su vez en una lista intensional. Tenemos, por tanto, una lista de listas
    	con los documentos en los que aparece cada palabra de la consulta. Con esta lista de listas llamamos a "intersect_multiple" que
    	nos devuelve los documentos que están en todas las listas.
    	"""
    	num_answer = intersect_multiple([list(map(itemgetter(0), self.vectorialIndex[word])) for word in extrae_palabras(consulta)])
    	
    	"""
    	Finalmente, traducimos los documentos cambiando su número interno por su ruta. Para ello, aplicamos la función que coge un item de
    	una lista según su posición sobre la lista de items self.rutaFich, dándole las posiciones de la lista num_answer.
    	"""
    	return list(itemgetter(*num_answer)(self.rutaFich))
    	
    	
	
		
if __name__ == "__main__":
	start = time.time()
	i = VectorialIndex('20news-18828')
	end = time.time()
	print('Tiempo:', end - start, 'segundos')
	
	consulta1 = 'DES Diffie-Hellman'
	n = 5
	print('Consulta vectorial con n =', n, ':', consulta1)
	pprint(i.consulta_vectorial(consulta1, n=n), compact=False)
	print('Consulta conjunción:', consulta1)
	pprint(i.consulta_conjuncion(consulta1), compact=False)
    	
