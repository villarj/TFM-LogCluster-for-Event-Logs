#Paquetes de python que se van a utilizar.
from pyspark import SparkContext, SparkConf
from time import time

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
  #print("Pintando x1")
  #print(x1)
  #print("Pintando x2")
  #print(x2)
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

#Cargamos el archivo de log
archivo=sc.textFile("Informacion.txt")
umbral=7

tiempoInicial=time()
print("Número de líneas:")
numLineas=archivo.count()
print(archivo.count())

#Contamos el número de palabras que hay en el archivo.
#Primero separamos las palabras de las líneas
pasoTokenizar=archivo.flatMap(lambda x:tokenize(x)) 
#Añadimos un 1 a cada palabra para contar después
pasoTokenizar=pasoTokenizar.map(lambda x:(x,1))
#Sumamos las palabras para ver que cantidad hay de cada una.
sumaPalabras=pasoTokenizar.reduceByKey(lambda x,y: x + y)
#Una vez tenemos las palabras, filtramos por el umbral.
palabrasUmbral=sumaPalabras.filter(lambda (x,y): y >= umbral)
print(palabrasUmbral.collect())
#Cogemos solo las palabras.
palabras=palabrasUmbral.map(lambda x: x[0])

#Almacenamos las palabras en una variable.
palabras=palabras.collect()
if(palabras==[]):
  print('Holaaa')
print("Palabras que superan el umbral: ",palabras)
#Leemos las líneas y las tokenizamos.
lineasTokenizadas=archivo.zipWithIndex()
print("Paso2")
print(lineasTokenizadas.collect())
print("Paso 3")
lll=lineasTokenizadas.flatMap(lambda x:((encontrarPalabras(palabras,x[0]))))
print(lll.collect())
#Borra las lineas que no encuentra la palabra
lll=lll.filter(lambda x:x[1]!=())
#----------------------------------------------
#-------------------------------------------
#aa=lll.groupByKey()
#print(aa.collect())

def buscarMinimo(x1,x2):
  p=0
  if(x1>x2):
    p=x1
  else:
    p=x2
  return p
    


bb=lll.reduceByKey(lambda v1,v2:buscarMinMax(v1,v2))
print("Pintando bb")
print(bb.collect())

cc=bb.filter(lambda x: (x[1][(len(x[1])-1)])>=umbral)

print("Pintando finallll")
print(cc.collect())

patrones=[]
soportes=[]
print("El error esta aqui")
for linea in cc.collect():
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
  print("Pintando patron y soporte:")
  print(patron)
  print(soporte)

tiempoFinal=time()
tiempoEmpleado=tiempoFinal-tiempoInicial
print(tiempoEmpleado)

numeroLineasClusterizadas=cc.map(lambda x:(x[1][(len(x[1])-1)])).sum()


diccionario={'Patrones':patrones,'Soportes':soportes,'LineasClusterizadas':numeroLineasClusterizadas,
             'TiempoEmpleado':tiempoEmpleado,'NumLineas':numLineas}
#bb=lll.reduceByKey(lambda v1,v2:buscarMinimo(v1,v2))
#bb=lll.reduceByKey(lambda x:x1+x2)
#print(bb.collect())
#Pasar el filtro del umbral
#--------------------------------------------------------
#--------------------------------------------------------

#lineas2=lineasTokenizadas.map(lambda x: x[0])

#print(lineas2.collect())
def algoritmo(path):
  #Cargamos el archivo
  archivo=sc.textFile(path)  


#Vamos a crear una función que abra el archivo.
def open_input_file (rutaArchivo):
  import os.path
  existe=os.path.exists(rutaArchivo)  
  if(existe):
    archivo=open(rutaArchivo,"r")
  else:
    error=("No se puede abrir el archivo: "+rutaArchivo)
    print(error)
    #Salimos del programa
    exit()
  #Devolvemos la ruta
  return archivo

#Vamos a crear una funcion que nos devuelva las palabras frecuentes
#A esta funcion le pasamos como parametro el archivo abierto y el soporte
def encontrarPalabrasFrecuentes(archivo,soporte):
  #Abrimos el archivo
  fh=open_input_file(archivo)
  for linea in fh.readlines():
    print linea  

#tt=open_input_file("Informacion.txt")
#print(tt)
#encontrarPalabrasFrecuentes("Informacion.txt",2)
#print(tt.readlines())


import matplotlib.pyplot as plt
#plt.plot([1,2,3,4])
#plt.ylabel('some numbers')
#plt.show()

print("FIN")