from time import time
import ExpresionesRegulares

def transformarTexto(x):
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


def logClusterSecuencial(nombrearchivo,soporte,preprocesar=False):
    #Definimos una variable tiempo para almacenar el tiempo en el que comienza.
    tiempoInicial=time()  
    #Vamos a contar las palabras de los archivos
    numLineasTotales=0

    with open(nombrearchivo) as archivo:  
        contador = {} # diccionario para guardar ocurrencia de palabras
        for linea in archivo:
            if (preprocesar==True):
                linea=transformarTexto(linea)
            numLineasTotales+=1
            palabras = linea.split() # separar linea en palabras, por espacio en blanco
            for palabra in palabras:
                #palabra = palabra.lower().strip(".,") # cambiar a minuscula y quitar puntuacion
                if palabra not in contador:
                    contador[palabra] = 1
                else:
                    contador[palabra] += 1
                
    # Cerrar archivo al finalizar
    archivo.close()

    soporte=(soporte/100.0)*numLineasTotales
    #Nos quedamos con las palabras que superan el soporte
    palabrasFrecuentes=[]
    for palabra in contador:
        if (contador[palabra]>=soporte):
            palabrasFrecuentes.append(palabra)
            #print(palabra)
            #print(contador[palabra])

    #print(palabrasFrecuentes)

    clusterCandidatos=[]

    #Una vez tenemos las palabras buscamos los patrones
    with open(nombrearchivo) as archivo:
        for linea in archivo:
            if (preprocesar==True):
                linea=transformarTexto(linea)
            tupla=[]
            varrs=[]
            i=0
            v=0
            palabras = linea.split()
            for palabra in palabras:
                if palabra in palabrasFrecuentes:
                    tupla.append(palabra)
                    varrs.append(v)
                    i +=1
                    v=0
                else:
                    v +=1
            varrs.append(v)
        
            #Determinamos el numero de elementos de la tupla
            k=len(tupla)
            if (k>0):
                existe = False
                for i in range(len(clusterCandidatos)):
                    if(clusterCandidatos[i]['tupla']==tupla):
                        cluster=clusterCandidatos[i]
                        cluster['soporte']+=1
                        clusterCandidatos[i]=cluster
                        for j in range(k+1):
                            if(cluster['minimo'][j]>varrs[j]):
                                cluster['minimo'][j]=varrs[j]
                            if(cluster['maximo'][j]<varrs[j]):
                                cluster['maximo'][j]=varrs[j]
                        existe=True
                        break
    
                if(existe==False):                            
                    varmin=[]
                    varmax=[]
                    for i in range(k+1):
                        varmin.append(varrs[i])
                        varmax.append(varrs[i])
                    cluster={'tupla':tupla,'soporte':1,'minimo':varmin,'maximo':varmax} 
                    clusterCandidatos.append(cluster)
                    

    #Obtenemos los patrones
    for i in range(len(clusterCandidatos)):
        patron=""
        cluster=clusterCandidatos[i]
        k=len(cluster['tupla'])
        for j in range(k):
            if(cluster['maximo'][j]>0):
                minimo=str(cluster['minimo'][j])
                maximo=str(cluster['maximo'][j])
                patron=patron+"*{"+minimo+","+maximo+"} "
            
            patron=patron+cluster['tupla'][j]+" "
        
        if(cluster['maximo'][k]>0):
            minimo=str(cluster['minimo'][k])
            maximo=str(cluster['maximo'][k])       
            patron=patron+"*{"+minimo+","+maximo+"}"
                        
        cluster['patron']=patron
        clusterCandidatos[i]=cluster


    #candidatos a cluster
    numCandidatosCluster=len(clusterCandidatos)

    #filtramos por soporte.
    patrones=[]
    soporteLineas=[]
    lineasClusterizadas=0
    numClusterFinales=0
    
    
    for i in range(len(clusterCandidatos)):
        if (clusterCandidatos[i]['soporte']>soporte):
            patrones.append(clusterCandidatos[i]['patron'])
            soporteLineas.append(clusterCandidatos[i]['soporte'])
            lineasClusterizadas=lineasClusterizadas+clusterCandidatos[i]['soporte']
            numClusterFinales=numClusterFinales+1
    
    #Obtenemos el tiempo final
    tiempoFinal=time()
    #Restamos el tiempo y vemos lo que tarda
    tiempoEmpleado=tiempoFinal-tiempoInicial 
    
    resultado={'Patrones':patrones,'SoporteLineas':soporteLineas,'LineasClusterizadas':lineasClusterizadas,
               'NumCandidatosCluster':numCandidatosCluster,'NumFinalesCluster':numClusterFinales,'TiempoEmpleado':tiempoEmpleado}

    
    return resultado