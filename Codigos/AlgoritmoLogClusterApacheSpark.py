from time import time

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


#Funcion que busca una palabra en cada linea.
def encontrarPalabrasLogCluster(palabra,linea):
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
            #Anadimos la palabra que esta en la linea a la lista
            tupla.append(p)
            #Anadimos el numero que nos indica si hay alguna palabra antes de dicha palabra.
            varrs.append(v)
            i=i+1
            v=0
        else:
            #En caso de que no este sumamos 1 a la variable que nos dice el numero de palabras.
            v=v+1
    varrs.append(v)
    #Anadimos un 1 a la ultima posicion que nos servira como soporte.
    varrs.append(1)
    #Transformamos los arrays a tuplas.
    tupla=tuple(tupla)
    varrs=tuple(varrs)
    #Almacenamos en un array las variables tupla y varrs 
    resultado.append((tupla,varrs))
    #Devolvemos el resultado
    return (resultado)

#Funcion que busca el minimo y el maximo
def buscarMinMax(x1,x2):  
    #Lista que almacenara los resultados.
    resultado=[]
    for i in range(len(x1)):
        if(i<len(x1)-1):
            #Si la entrada es de tipo entero
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
                #Anadimos el resultado al array
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
            #Anadimos la informacion al array 
            resultado.append((minimo,maximo))
        else:
            soporte=x1[i]+x2[i]
            resultado.append((soporte))        
    #Transformamos el array a una tupla.
    resultado=tuple(resultado)
    #Devolvemos el resultado.
    return resultado

   
### -------------------------------------------- ###
### ---------- Algoritmo Log Cluster ----------- ###
### -------------------------------------------- ###
def algoritmoLogCluster(soporte,path,sc,prepocesar=False):
    #Definimos una variable tiempo para almacenar el tiempo en el que comienza.
    tiempoInicial=time()    
    #Almacenamos en una variable el archivo de logs
    archivo=sc.textFile(path)
    #Contamos el numero de lineas del archivo.
    numLineas=archivo.count()
    #Modificamos el soporte
    soporte=(soporte/100.0)*numLineas
    if (soporte>numLineas):
        mensaje="El soporte es mayor que el numero de lineas"
        return mensaje
    
    #Transformar el texto si se quiere........................
    if (prepocesar==True):
        archivo=archivo.map(lambda x:transformarTexto(x))
    ####archivo=archivo.map(lambda x:transformarTexto(x))######    
    #Contamos el numero de palabras que hay en el archivo.
    #Primero separamos las palabras de las lineas
    pasoTokenizar=archivo.flatMap(lambda x:x.split()) 
    #Anadimos un 1 a cada palabra para contar despues
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
    
    ## Obtenemos los cluster
    #Vemos las palabras que han superado el umbral que hay por linea y obtenemos su
    #posicion
    obtenerPalabras=archivo.flatMap(lambda x:((encontrarPalabrasLogCluster(palabras,x))))
    #Borra las lineas que no tiene ninguna palabra de las que se buscan
    lineasFiltradas=obtenerPalabras.filter(lambda x:x[1]!=())    
    #Reducimos las lineas por sus keys y obtenemos los cluster.
    candidatosCluster=lineasFiltradas.reduceByKey(lambda v1,v2:buscarMinMax(v1,v2))    
    #Obtenemos el numero de posibles candidatos a cluster.
    numCandidatosCluster=candidatosCluster.count()        
    #Filtramos los candidatos a cluster por el soporte
    clusterFinales=candidatosCluster.filter(lambda x: (x[1][(len(x[1])-1)])>=soporte)    
    #Obtenemos el numero de lineas que estan clusterizadas.
    numeroLineasClusterizadas=clusterFinales.map(lambda x:(x[1][(len(x[1])-1)])).sum()
    #Calculamos el porcentaje:
    porcentajeLineasClusterizadas=round(float(numeroLineasClusterizadas)/float(numLineas)*100,2)
    
    #Obtenemos el numero de cluster finales.
    numeroClusterFinales=clusterFinales.count()
    #Una vez tenemos toda la informacion pasamos a escribir los patrones y los
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
    #Almacenamos la informacion en un diccionario.  
    diccionario={'Patrones':patrones,'SoporteLineas':soportes,'LineasClusterizadas':porcentajeLineasClusterizadas,
                'TiempoEmpleado':tiempoEmpleado,'NumLineas':numLineas,'NumCandidatosCluster':numCandidatosCluster ,'NumFinalesCluster':numeroClusterFinales}
    #Devolvemos el diccionario.
    return diccionario