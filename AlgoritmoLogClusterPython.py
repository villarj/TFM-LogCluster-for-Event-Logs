#Paquetes de python que se van a utilizar.
from pyspark import SparkContext, SparkConf
from time import time
import matplotlib.pyplot as plt


#Función que separa las palabras por espacios en blanco y devulve una lista con estas palabras.
def tokenize(x):
  resultado = []
  #Separa las palabras de cada línea cuando hay un espacio en blanco.
  linea=x.split()
  #Para cada palabra le añadimos un uno, de esta forma podemos usarlo
  #como diccionario en un RDD.
  for palabra in linea:
    resultado.append(palabra)
  #Devolvemos el resultado.
  return resultado

#Función que busca una palabra en cada linea.
def encontrarPalabras(palabra,linea):
  #Definimos dos arrays para almacenar las palabras que se repiten
  tupla=[]
  varrs=[]
  resultado=[]
  i=0
  v=0
  linea2=linea.split()
  #Para cada palabra que hay en la linea comprobamos si 
  #dicha palabra se encuentra entre las palabras frecuentes.
  for p in linea2:
    #Vemos si la p esta en palabras
    if p in palabra:
      #Añadimos la palabra que esta en la linea a la lista
      tupla.append(p)
      #Añadimos el número que nos indica si hay alguna palabra antes de dicha palabra.
      varrs.append(v)
      i=i+1
      v=0
    else:
      #En caso de que no este sumamos 1 a la variable que nos dice el número de palabras.
      v=v+1
  varrs.append(v)
  #Añadimos un 1 a la última posición que nos servira como soporte.
  varrs.append(1)
  #Transformamos los arrays a tuplas.
  tupla=tuple(tupla)
  varrs=tuple(varrs)
  #Almacenamos en un array las variables tupla y varrs 
  resultado.append((tupla,varrs))
  #Devolvemos el resultado
  return (resultado)

def buscarMinMax(x1,x2):  
  #Lista que almacenara los resultados.
  resultado=[]
  for i in range(len(x1)):
    if(i<len(x1)-1):
      #Si la entrada son de tipo entero
      if(type(x2[i])==int and type(x1[i])==int):
        if(x1[i]<x2[i]):
          minimo=x1[i]
        else:
          minimo=x2[i]
        if(x1[i]>x2[i]):
          maximo=x1[i]
        else:
          maximo=x2[i]
      #Si la entrada es int y la otra una tupla
      if(type(x1[i])==int and type(x2[i])==tuple):
        if(x1[i]<x2[i][0]):
          minimo=x1[i]
        else:
          minimo=x2[i][0]
        if (x1[i]>x2[i][1]):
          maximo=x1[i]
        else:
          maximo=x2[i][1]
      #Si la variable x1 es de tipo tupla y x2 entero
      if(type(x1[i])==tuple and type(x2[i])==int):
        if(x1[i][0]<x2[i]):
          minimo=x1[i][0]
        else:
          minimo=x2[i]
        if (x1[i][1]>x2[i]):
          maximo=x1[i][1]
        else:
          maximo=x2[i]
        #Añadimos el resultado al array
        #resultado.append((minimo,maximo)) 
      #Si ambas variables son tuplas.   
      if(type(x1[i])==tuple and type(x2[i])==tuple):
        if(x1[i][0]<x2[i][0]):
          minimo=x1[i][0]
        else:
          minimo=x2[i][0]
        if (x1[i][1]>x2[i][1]):
          maximo=x1[i][1]
        else:
          maximo=x2[i][1]
      #Añadimos la información al array 
      resultado.append((minimo,maximo))
    else:
      soporte=x1[i]+x2[i]
      resultado.append((soporte))        
  #Transformamos el array a una tupla.
  resultado=tuple(resultado)
  #Devolvemos el resultado.
  return resultado


try:
  sc
except NameError:
  conf = SparkConf().setAppName("SLCT").setMaster("local[3]")
  #conf = SparkConf().setAppName("SLCT").setMaster("spark://vzzspark01.corp.zzircon.com:7077")
  sc = SparkContext(conf=conf)
print(sc)


def algoritmoLogCluster(soporte,path):
  #Almacenamos en una variable el archivo de logs
  archivo=sc.textFile(path)
  #Definimos una variable de tiempo inicial.
  tiempoInicial=time()
  #Contamos el número de líneas del archivo.
  numLineas=archivo.count()
  if (soporte>numLineas):
    mensaje="El soporte es mayor que el número de lineas"
    return mensaje
  #Contamos el número de palabras que hay en el archivo.
  #Primero separamos las palabras de las líneas
  pasoTokenizar=archivo.flatMap(lambda x:tokenize(x)) 
  #Añadimos un 1 a cada palabra para contar después
  pasoTokenizar=pasoTokenizar.map(lambda x:(x,1))  
  #Sumamos las palabras para ver que cantidad hay de cada una.
  sumaPalabras=pasoTokenizar.reduceByKey(lambda x,y: x + y)
  #Una vez tenemos las palabras, filtramos por el soporte.
  palabrasUmbral=sumaPalabras.filter(lambda (x,y): y >= soporte)  
  #Cogemos solo las palabras.
  palabras=palabrasUmbral.map(lambda x: x[0])  
  #Almacenamos las palabras en una variable lista.
  palabras=palabras.collect()  
  if(palabras==[]):
    mensaje="No hay palabras para este soporte"
    return mensaje 
  #Leemos las líneas y las indexamos.
  lineasIndexadas=archivo.zipWithIndex()
  #Vemos las palabras que han superado el umbral que hay por línea y obtenemos su
  #posición
  obtenerPalabras=lineasIndexadas.flatMap(lambda x:((encontrarPalabras(palabras,x[0]))))
  #Borra las lineas que no tiene ninguna palabra de las que se buscan
  lineasFiltradas=obtenerPalabras.filter(lambda x:x[1]!=())
  #Reducimos las lineas por sus keys y obtenemos los cluster.
  candidatosCluster=lineasFiltradas.reduceByKey(lambda v1,v2:buscarMinMax(v1,v2))
  for linea in candidatosCluster.collect():
    print (linea)
  #Filtramos los candidatos a cluster por el soporte
  clusterFinales=candidatosCluster.filter(lambda x: (x[1][(len(x[1])-1)])>=soporte)
  #Obtenemos el número de líneas que pertenecen a un cluster.
  numeroLineasClusterizadas=clusterFinales.map(lambda x:(x[1][(len(x[1])-1)])).sum()
  #Obtenemos el número de candidatos de cluster
  numeroCandidatosCluster=candidatosCluster.count()
  #Obtenemos el número de cluster finales.
  numeroClusterFinales=clusterFinales.count()
  #Una vez tenemos toda la información pasamos a escribir los patrones y los
  #soportes de cada cluster.
  #Variables para almacenar los patrones y soportes-
  patrones=[]
  soportes=[]
  for linea in clusterFinales.collect():
    patron=""
    l1=linea[0]
    l2=linea[1]
    longitud=len(l1)
    for i in range(longitud):
      if(l2[i]==(0, 0) or l2[i]==0):
        patron=patron+l1[i]+" "
      else:
        minimo=l2[i][0]
        maximo=l2[i][1]
        auxiliar="*{"+str(minimo)+","+str(maximo)+"}"
        patron=patron+auxiliar+" "+l1[i]+" "
    if(l2[longitud]!=(0, 0) and l2[longitud]!=0):
      minimo=l2[longitud][0]
      maximo=l2[longitud][1] 
      auxiliar="*{"+str(minimo)+","+str(maximo)+"}"
      patron=patron+auxiliar
    soporte=l2[longitud+1]
    patrones.append(patron)
    soportes.append(soporte)
  
  #Obtenemos el tiempo final
  tiempoFinal=time()
  #Restamos el tiempo y vemos lo que tarda
  tiempoEmpleado=tiempoFinal-tiempoInicial  
  #Almacenamos la información en un diccionario.  
  diccionario={'Patrones':patrones,'SoporteLineas':soportes,'LineasClusterizadas':numeroLineasClusterizadas,
                 'TiempoEmpleado':tiempoEmpleado,'NumLineas':numLineas, 'numCluster':numeroClusterFinales}
  #Devolvemos el diccionario.
  return diccionario





print(algoritmoLogCluster(2,"Informacion.txt"))

zz=[]
fig, ((ax1, ax2),(ax3,ax4)) = plt.subplots(2, 2)
for s in [650,800]:
  print("Calculando para el soporte:",s)
  resultado=algoritmoLogCluster(s,"syslog.txt")
  zz.append(resultado)
  ax1.plot(s, resultado['TiempoEmpleado'], 'ro',)
  ax2.plot(s, resultado['LineasClusterizadas'], 'go')
  ax3.plot(s, resultado['numCluster'], 'bo')
  ax4.plot(s, resultado['NumLineas']-resultado['LineasClusterizadas'], 'yo')

ax1.set_ylabel('t/s')
#ax1.set_xlabel('Soporte')
ax1.set_title('Tiempo')

ax2.set_ylabel('Lineas Clusterizadas')
ax2.set_xlabel('Soporte')
ax2.set_title('Lineas')

ax3.set_ylabel('num Cluster')
ax3.set_xlabel('Soporte')
ax3.set_title('Cluster')

ax4.set_title('Outliers')


plt.show()  

#Cargamos el archivo de log
#archivo=sc.textFile("Informacion.txt")
#umbral=2

#tiempoInicial=time()
#print("Número de líneas:")
#numLineas=archivo.count()
#print(archivo.count())

#Contamos el número de palabras que hay en el archivo.
#Primero separamos las palabras de las líneas
#pasoTokenizar=archivo.flatMap(lambda x:tokenize(x)) 
#Añadimos un 1 a cada palabra para contar después
#pasoTokenizar=pasoTokenizar.map(lambda x:(x,1))
#Sumamos las palabras para ver que cantidad hay de cada una.
#sumaPalabras=pasoTokenizar.reduceByKey(lambda x,y: x + y)
#Una vez tenemos las palabras, filtramos por el umbral.
#palabrasUmbral=sumaPalabras.filter(lambda (x,y): y >= umbral)
#print(palabrasUmbral.collect())
#Cogemos solo las palabras.
#palabras=palabrasUmbral.map(lambda x: x[0])

#Almacenamos las palabras en una variable.
#palabras=palabras.collect()
#if(palabras==[]):
#  print('Holaaa')
#print("Palabras que superan el umbral: ",palabras)
#Leemos las líneas y las tokenizamos.
#lineasTokenizadas=archivo.zipWithIndex()
#print("Paso2")
#print(lineasTokenizadas.collect())
#print("Paso 3")
#lll=lineasTokenizadas.flatMap(lambda x:((encontrarPalabras(palabras,x[0]))))
#print(lll.collect())
#Borra las lineas que no encuentra la palabra
#lll=lll.filter(lambda x:x[1]!=())
#----------------------------------------------
#-------------------------------------------
#aa=lll.groupByKey()
#print(aa.collect())
 


#bb=lll.reduceByKey(lambda v1,v2:buscarMinMax(v1,v2))
#print("Pintando bb")
#print(bb.collect())

#cc=bb.filter(lambda x: (x[1][(len(x[1])-1)])>=umbral)

#print("Pintando finallll")
#print(cc.collect())

#patrones=[]
#soportes=[]
#print("El error esta aqui")
#for linea in cc.collect():
 # patron=""
 # l1=linea[0]
 # l2=linea[1]
 # longitud=len(l1)
  #for i in range(longitud):
   # if(l2[i]==(0, 0) or l2[i]==0):
    #  patron=patron+l1[i]+" "
    #else:
     # minimo=l2[i][0]
      #maximo=l2[i][1]
      #auxiliar="*{"+str(minimo)+","+str(maximo)+"}"
      #patron=patron+auxiliar+" "+l1[i]+" "

  #if(l2[longitud]!=(0, 0) and l2[longitud]!=0):
   # minimo=l2[longitud][0]
    #maximo=l2[longitud][1] 
    #auxiliar="*{"+str(minimo)+","+str(maximo)+"}"
    #patron=patron+auxiliar
  #soporte=l2[longitud+1]
  #patrones.append(patron)
  #soportes.append(soporte)
  #print("Pintando patron y soporte:")
  #print(patron)
  #print(soporte)

#tiempoFinal=time()
#tiempoEmpleado=tiempoFinal-tiempoInicial
#print(tiempoEmpleado)

#numeroLineasClusterizadas=cc.map(lambda x:(x[1][(len(x[1])-1)])).sum()


#diccionario={'Patrones':patrones,'SoporteLineas':soportes,'LineasClusterizadas':numeroLineasClusterizadas,
#             'TiempoEmpleado':tiempoEmpleado,'NumLineas':numLineas}
#bb=lll.reduceByKey(lambda v1,v2:buscarMinimo(v1,v2))
#bb=lll.reduceByKey(lambda x:x1+x2)
#print(bb.collect())
#Pasar el filtro del umbral
#--------------------------------------------------------
#--------------------------------------------------------

#lineas2=lineasTokenizadas.map(lambda x: x[0])

#print(lineas2.collect())





import matplotlib.pyplot as plt
#plt.plot([1,2,3,4])
#plt.ylabel('some numbers')
#plt.show()

print("FIN")