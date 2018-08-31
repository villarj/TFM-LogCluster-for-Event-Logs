#Algoritmo SLCT
from pyspark import SparkContext, SparkConf
from time import time
import re
import sys


## --- Metodos para todos los algoritmos --- ## 
#Funcion que transforma las lineas en FECHA y HORA
def transformarTexto(x):
    import ExpresionesRegulares
    #Obtenemos las horas y lo sustituimos
    horas = ExpresionesRegulares.find_time(x) 
    for hora in horas:
        x = x.replace(hora, '[HORA]')
    #Obtenemos las fechas y lo sustituimos
    fechas = ExpresionesRegulares.find_dates(x)  
    for fecha in fechas:
        x = x.replace(fecha, '[FECHA]')
    #Devolvemos el valor de x
    return x

### -------------------------------------- ###
### --- Metodos para el algoritmo SLCT --- ###
#Funcion que separa las palabras por espacios en blanco y devulve una lista con estas palabras.
def tokenize(x):
    resultado = []
    #Posicion de la palabra en la linea
    posicion=1
    #Separa las palabras de la linea y las mete en una lista
    linea=x.split()
    #Cogemos cada palabra de la lista y lo modificamos 
    #anadiendo la posicion y un numero de que se encuentra una vez
    for palabra in linea:
        resultado.append(((posicion,palabra)))
        posicion=posicion+1
    #Devolvemos el resultado.
    return resultado


#Funcion que busca una palabra en cada linea.
def encontrarPalabras(palabra,linea):
    #Definimos dos arrays para almacenar las palabras que se repiten
    resultado=[]
    #Para cada palabra que hay en la linea comprobamos si 
    #dicha palabra se encuentra entre las palabras frecuentes.
    for p in linea:
        #Vemos si la p esta en palabras
        if p in palabra:
            #Anadimos la palabra que esta en la linea a la lista
            resultado.append(p)
    #Transformamos la lista a una tupla.
    resultado=tuple(resultado)
    return resultado


### -------------------------------------------- ###
### -------------- Algoritmo SLCT -------------- ###
### -------------------------------------------- ###
#Funcion que aplica el algoritmo SLCT
def algoritmoSLCT(soporte, path,sc,prepocesar=False):  
    #Definimos una variable tiempo para almacenar el tiempo en el que comienza.
    tiempoInicial=time()    
    #Cargamos el archivo de texto
    archivo=sc.textFile(path)
    #Contamos las lineas del archivo
    numLineas=archivo.count()
    #Modificamos el soporte
    soporte=(soporte/100.0)*numLineas
    #Transformar el texto si se quiere........................
    if (prepocesar==True):
        archivo=archivo.map(lambda x:transformarTexto(x))
    ####archivo=archivo.map(lambda x:transformarTexto(x))######
    #Tokenizamos los resultados para contar las palabras.
    #Por esta razon usamos la funcion flatMap para que todas las palabras esten afectadas.
    pasoTokenizar=archivo.flatMap(lambda x:tokenize(x))    
    #Anadimos un 1 a cada palabra para contar despues
    pasoContar=pasoTokenizar.map(lambda x:(x,1))  
    #Una vez tenemos las palabras y la posicion que ocupan en cada linea
    #Procedemos a contar dichas palabras, para esto hacemos uso de la funcion reduceByKey
    #Sumamos por clave valor
    sumaPalabras=pasoContar.reduceByKey(lambda x,y: x+y)
    #A continuacion filtramos las palabras por el soporte deseado
    filtradoPalabras=sumaPalabras.filter(lambda (x,y): y>soporte)
    #Una vez estan las palabras filtradas por el soporte almacenamos dichas palabras y su posicion
    almacenarPalabras=filtradoPalabras.map(lambda x: x[0])
    #Recogemos las palabras y su posicion
    palabras=almacenarPalabras.collect()
    if(palabras==[]):
        palabras=["Sin_palabras"]
    
    ### ------------------------------------------------------- ###
    ### Obtener los cluster ###
    #Ahora vamos a empezar a buscar los cluster, primero tokenizamos por linea    
    lineaTokenizada=archivo.map(lambda x:tokenize(x))
    
    #Buscamos las palabras frecuentes y su pocision en cada linea y lo almacenamos    
    obtenerPalabras=lineaTokenizada.map(lambda x:((encontrarPalabras(palabras,x))))
    
    #Anadimos un 1 en cada linea para obtener el soporte posteriormente.    
    obtenerPalabras=obtenerPalabras.map(lambda x: (x,1))
    
    #Agrupamos por su clave y asi tenemos el soporte
    agrupacion=obtenerPalabras.groupByKey().mapValues(len)  
    #Obtenemos el numero de posibles candidatos a cluster.
    numCandidatosCluster=agrupacion.count()    
    #Filtramos
    agrupacion=agrupacion.filter(lambda (x,y): y>=soporte)
    #Obtenemos el numero de cluster que superan el soporte
    numFinalesCluster=agrupacion.count()    
    #Sumamos
    numeroLineasClusterizadas=agrupacion.map(lambda x:(x[1])).sum()     
    #Obtenemos el porcentaje
    porcentajeLineasClusterizadas=round(float(numeroLineasClusterizadas)/float(numLineas)*100,2)
    
    #Cogemos solo las palabras para obtener los patrones.
    palabrasPatron=agrupacion.map(lambda x:x[0])
    soporteLineas=agrupacion.map(lambda x:x[1]).collect()
    # Lista para almacenar los patrones.
    patrones=[]
    #Escribimos los patrones
    for linea in palabrasPatron.collect():
        patron=""    
        aux=1
        #print("Linea:")
       # print (linea)
        for j in range(len(linea)):
            pp=linea[j][0]
            while (aux<pp):
                patron=patron+"*"+" "
                aux=aux+1
                
            aux=aux+1
            patron=patron+linea[j][1]+" "
        
        #Anadimos los patrones al array
        patrones.append(patron)

    #Calculamos el tiempo final
    tiempoFinal=time()
    #Restamos el tiempo y vemos lo que tarda
    tiempoEmpleado=tiempoFinal-tiempoInicial 
   
    #Almacenamos el resultado en un diccionario
    diccionario={'Patrones':patrones,'SoporteLineas':soporteLineas,'TiempoEmpleado':tiempoEmpleado,'NumCandidatosCluster':numCandidatosCluster,'NumFinalesCluster':numFinalesCluster,'LineasClusterizadas':porcentajeLineasClusterizadas}
    #Devolvemos el resultado    
    return(diccionario)