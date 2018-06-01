#Paquetes de python que se van a utilizar.
from pyspark import SparkContext, SparkConf
from time import time
import matplotlib.pyplot as plt
import re


#Función que transforma las lineas en FECHA y HORA
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
  
  #Sustituimos palabras por los patrones
  pasoSustituir=archivo.map(lambda x:transformarTexto(x)) 
  
  #Contamos el número de palabras que hay en el archivo.
  #Primero separamos las palabras de las líneas
  pasoTokenizar=pasoSustituir.flatMap(lambda x:tokenize(x)) 
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
  #pasoSustituir=archivo.map(lambda x:transformarTexto(x)) 
  
  lineasIndexadas=pasoSustituir.zipWithIndex()  
  #Vemos las palabras que han superado el umbral que hay por línea y obtenemos su
  #posición
  obtenerPalabras=lineasIndexadas.flatMap(lambda x:((encontrarPalabras(palabras,x[0]))))
  #Borra las lineas que no tiene ninguna palabra de las que se buscan
  lineasFiltradas=obtenerPalabras.filter(lambda x:x[1]!=())
  #Reducimos las lineas por sus keys y obtenemos los cluster.
  candidatosCluster=lineasFiltradas.reduceByKey(lambda v1,v2:buscarMinMax(v1,v2))
  #for linea in candidatosCluster.collect():
   # print (linea)
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
        if (type(l2[i])==int):
          minimo=l2[i]
          maximo=l2[i]
        else:
          minimo=l2[i][0]
          maximo=l2[i][1]
        auxiliar="*{"+str(minimo)+","+str(maximo)+"}"
        patron=patron+auxiliar+" "+l1[i]+" "
    if(l2[longitud]!=(0, 0) and l2[longitud]!=0):
      if(type(l2[i])==int):
        minimo=l2[i]
        maximo=l2[i]
      else:
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
                 'TiempoEmpleado':tiempoEmpleado,'NumLineas':numLineas,'numCandidatosCluster':numeroCandidatosCluster ,'numCluster':numeroClusterFinales}
  #Devolvemos el diccionario.
  return diccionario





print(algoritmoLogCluster(5,"Informacion.txt"))

import sys
orig_stdout = sys.stdout
f = open('primerosResultados.txt', 'w')
sys.stdout = f
zz=[]
fig, ((ax1, ax2),(ax3,ax4)) = plt.subplots(2, 2)
for s in [10,30,50,70,100,200,300,500,800]:
  print("Calculando para el soporte:",s)
  resultado=algoritmoLogCluster(s,"syslog.txt")
  print("Número de cluster encontrados:")
  print(resultado['numCluster'])
  print("Patrones y num lineas")
  for i in range(len(resultado['Patrones'])):
    print(resultado['Patrones'][i],"",resultado['SoporteLineas'][i])
    #print(resultado['SoporteLineas'][i])
  zz.append(resultado)
  print("")
  ax1.plot(s, resultado['TiempoEmpleado'], 'ro',)
  ax2.plot(s, resultado['LineasClusterizadas'], 'go')  
  ax3.plot(s, resultado['numCandidatosCluster'], 'yo')
  ax4.plot(s, resultado['numCluster'], 'bo')


#sys.stdout = orig_stdout
#f.close()

ax1.set_ylabel('t/s')
#ax1.set_xlabel('Soporte')
ax1.set_title('Tiempo')

ax2.set_ylabel('Lineas Clusterizadas')
#ax2.set_xlabel('Soporte')
ax2.set_title('Lineas')

ax3.set_ylabel('num candidatos Cluster')
ax3.set_xlabel('Soporte')
ax3.set_title('Cluster candidatos')

ax4.set_title('num Cluster seleccionados')


plt.show()  







import ExpresionesRegulares
#matches = ExpresionesRegulares.find_dates(line2)



#for mat in matches:
  #print(type(str(mat)))
  #mat=str(mat)
 # line2 = line2.replace(mat, '[FECHA]')
  #print (mat)


#print(line2)



#https://code.tutsplus.com/es/tutorials/8-regular-expressions-you-should-know--net-6149



archivo1=sc.textFile("syslog.txt")

paso1=archivo1.map(lambda x:transformarTexto(x)) 

#expresion='\[FECHA\] \[HORA\] RUE3 kernel: \[(\s+)(\S+)(\s*)(\S*)(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?(\s*)?(\S*)?'

expresion='\[FECHA\] \[HORA\] RUE3 kernel: \[    0\.000000\](\s+\S+)?(\s+\S+)?(\s+\S+)?(\s+\S+)?\s+\[mem(\s*\S*)(\s*\S*)?(\s*\S*)?(\s*\S*)?'



expresion='\[FECHA\] \[HORA\] RUE3 kernel: \[(\s+\S+)\s+pci(\s+\S+\s+\S+\s+\S+\s+\S+)(\s+\S+)?(\s+\S+)?(\s+\S+)?'

patron = re.compile(expresion)



import rePatronesCluster
buscar=rePatronesCluster.findLineasCluster()



print("")
print("")
print("Parseando las lineas con los cluster de soporte =100")
print("")
for i in range(5):
  print(zz[4]['Patrones'][i])
  t=0
  j=0  
  for resul in paso1.collect():
    b=buscar.buscarLineasPatrones(resul,i)
    if (b!=None):
      t=t+1
      if(b==resul):
        j=j+1
  #a=patron.finditer(resul)
  #for r in a:
    #print("Linea:")
    #print(resul)
    #print(i)
    #print(r.group(0))
    #t=t+1
    #if(resul==r.group(0)):
      #j=j+1
      #print("coincidencia")

  print("Pintando lineas que tienen este patrón")
  print (t)

  print("Pintando lineas que coinciden exactamente con este patrón")
  print (j)


sys.stdout = orig_stdout
f.close()
print("Fin Expresiones regulares")
