import AlgoritmoLogClusterSecuencial as logClusterSecuencial
import AlgoritmoLogClusterApacheSpark as logClusterApacheSpark
import AlgoritmoSlctApacheSpark as slctApacheSpark
import ObtenerExpresionRegular as obtenerModelos
import matplotlib.pyplot as plt
from matplotlib import gridspec
from pyspark import SparkContext, SparkConf

#Creamos el contexto para usas apache spark
try:
    sc
except NameError:
    conf = SparkConf().setAppName("SLCT").setMaster("local[3]")
    #conf = SparkConf().setAppName("SLCT").setMaster("spark://vzzspark01.corp.zzircon.com:7077")
    sc = SparkContext(conf=conf)   

#Listas para almacenar los resultados de las dos variables.
resultadoSlct=[]
resultadoLogClusterApacheSpark=[]
resultadoLogClusterSecuencial=[]

#Definimos una figura para representar el tiempo que tarda en ejecutarse
figTiempoLogCluster, ax7 = plt.subplots(1)

#Definimos una figura para representar el tiempo que tarda en ejecutarse
figTiempo, ax1 = plt.subplots(1)

#Definimos una figura para representar el numero de candidatos posibles
figCandidatosCluster, ax2 = plt.subplots(1)

#Definimos una figura para representar el numero de cluster que superan el soporte
figClusterFinales = plt.figure()
gs = gridspec.GridSpec(2, 2)
ax3 = figClusterFinales.add_subplot(gs[0,0])
ax4 = figClusterFinales.add_subplot(gs[0,1])
ax5 = figClusterFinales.add_subplot(gs[1,:])

#Definimos una figura para almacenar el porcentaje de lineas que pertenecen a algun cluster
figLineas, ax6 = plt.subplots(1)

rutaArchivo="ShipperServer.txt"
inicial=sc.textFile(rutaArchivo)
arranque=logClusterApacheSpark.algoritmoLogCluster(1,"Informacion.txt",sc)

import sys
orig_stdout = sys.stdout
f = open('DocumentosTexto/resultadosShipper.txt', 'w')
sys.stdout = f

datosIniciales='Nombre del fichero: {}, numero de lineas totales: {}'
print(datosIniciales.format(rutaArchivo,inicial.count()))
print("")


for i in [5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80]:
    logCluster_apache_spark=logClusterApacheSpark.algoritmoLogCluster(i,rutaArchivo,sc,True)
    slct_apache_spark=slctApacheSpark.algoritmoSLCT(i,rutaArchivo,sc,True)
    log_cluster_secuencial=logClusterSecuencial.logClusterSecuencial(rutaArchivo,i,True)
    
    ax7.plot(i, log_cluster_secuencial['TiempoEmpleado'], color='blue',marker='o')
    ax7.plot(i, logCluster_apache_spark['TiempoEmpleado'], color='olive',marker='o')    
    
    #Para tiempo de ejecucion
    ax1.plot(i, slct_apache_spark['TiempoEmpleado'], color='blue',marker='o')
    ax1.plot(i, logCluster_apache_spark['TiempoEmpleado'], color='olive',marker='o')
    
    
    #Para candidatos a cluster.
    ax2.plot(i, slct_apache_spark['NumCandidatosCluster'], color='red',marker='o')
    ax2.plot(i, logCluster_apache_spark['NumCandidatosCluster'], color='olive',marker='o')
    
    #Para cluster que superan el soporte
    ax3.plot(i, slct_apache_spark['NumFinalesCluster'], color='red',marker='o')
    ax4.plot(i, logCluster_apache_spark['NumFinalesCluster'], color='olive',marker='o')    
    ax5.plot(i, slct_apache_spark['NumFinalesCluster'], color='red',marker='o')
    ax5.plot(i, logCluster_apache_spark['NumFinalesCluster'], color='olive',marker='o')    
    
    #Para el porcentaje de lineas
    ax6.plot(i, slct_apache_spark['LineasClusterizadas'], color='red',marker='o')        
    ax6.plot(i, logCluster_apache_spark['LineasClusterizadas'], color='olive',marker='o')    
    
    #Anadimos los resultados a las listas.
    resultadoSlct.append(slct_apache_spark)
    resultadoLogClusterApacheSpark.append(logCluster_apache_spark)
    resultadoLogClusterSecuencial.append(log_cluster_secuencial)

    
    informacion='{}, Soporte: {} %, numCandidatosCluster: {}, numClusterFinales: {}'
    
    
    print (informacion.format('Simple logfile clustering tool (slct_apache_spark)',i,slct_apache_spark['NumCandidatosCluster'],slct_apache_spark['NumFinalesCluster']))
    print("Cluster y numero de lineas en el cluster:")    
    for j in range(len(slct_apache_spark['Patrones'])):
        print(slct_apache_spark['Patrones'][j],slct_apache_spark['SoporteLineas'][j])    
    
    print ("")
    
    print (informacion.format('Log cluster apache spark',i,logCluster_apache_spark['NumCandidatosCluster'],logCluster_apache_spark['NumFinalesCluster']))
    
    print("Cluster y numero de lineas en el cluster:")
    for j in range(len(logCluster_apache_spark['Patrones'])):
        print(logCluster_apache_spark['Patrones'][j],logCluster_apache_spark['SoporteLineas'][j])       
    
    print("")
    
    print (informacion.format('Log cluster secuencial',i,log_cluster_secuencial['NumCandidatosCluster'],log_cluster_secuencial['NumFinalesCluster']))
    print("Cluster y numero de lineas en el cluster:")
    for j in range(len(log_cluster_secuencial['Patrones'])):
        print(log_cluster_secuencial['Patrones'][j],log_cluster_secuencial['SoporteLineas'][j])

    print("")
    
#Cerramos el flujo.
sys.stdout = orig_stdout
f.close()


ax7.set_title(r'$Tiempo$ $de$ $ejecuci\acute{o}n$ $para$ $el$ $algoritmo$ $log$ $cluster$')
ax7.legend(["Algoritmo Log Cluster secuencial","Algoritmo Log Cluster apache spark"])
ax7.set_ylabel('Tiempo / segundos')
ax7.set_xlabel('Porcentaje Soporte / %')

#Guardamos la figura en la carpeta imagenes
figTiempoLogCluster.savefig('ImagenesShipper/TiempoEjecucionLogCluster.png')


#Damos formato a la figura de tiempo de ejecucion
ax1.set_title(r'$Tiempo$ $de$ $ejecuci\acute{o}n$ $para$ $diferente$ $porcentaje$ $de$ $soportes$')
ax1.legend(["Algoritmo SLCT","Algoritmo Log Cluster"])
ax1.set_ylabel('Tiempo / segundos')
ax1.set_xlabel('Porcentaje Soporte / %')

#Guardamos la figura en la carpeta imagenes
figTiempo.savefig('ImagenesShipper/TiempoEjecucion.png')

#Damos formato a la figura de numero de candidatos a cluster
ax2.set_title(r'$N\acute{u}mero$ $de$ $cluster$ $candidatos$')
ax2.legend(["Algoritmo SLCT","Algoritmo Log Cluster"])
ax2.set_ylabel('Num candidatos a cluster')
ax2.set_xlabel('Porcentaje Soporte / %')

#Guardamos la figura en la carpeta de imagenes
figCandidatosCluster.savefig('ImagenesShipper/CandidatosCluster.png')

#Damos formato a la figura de cluster finales
figClusterFinales.subplots_adjust(wspace=0.3 ,hspace = 0.4)
ax3.set_ylabel('Numero cluster')
ax3.set_xlabel('Porcentaje Soporte / %')
ax4.set_ylabel('Numero cluster')
ax4.set_xlabel('Porcentaje Soporte / %')
ax5.legend(["Algoritmo SLCT","Algoritmo Log Cluster"])
ax5.set_ylabel('Numero cluster')
ax5.set_xlabel('Porcentaje Soporte / %')
figClusterFinales.suptitle(r'$N\acute{u}mero$ $de$ $cluster$ $frecuentes$')

#Guardamos la figura en la carpeta imagenes.
figClusterFinales.savefig('ImagenesShipper/ClusterFinales.png')

#Damos formato a la figura de numero de candidatos a cluster
ax6.set_title(r'Porcentaje de lineas que estan clusterizadas')
ax6.legend(["Algoritmo SLCT","Algoritmo Log Cluster"])
ax6.set_ylabel('Lineas clusterizadas / %')
ax6.set_xlabel('Porcentaje Soporte / %')

#Guardamos la figura en la carpeta imagenes.
figLineas.savefig('ImagenesShipper/LineasClusterizadas.png')




plt.show()
#figCandidatosCluster.savefig('Imagenes/CandidatosClusterPosibles.png')
#print ("Obteniendo los modelos")
#figClusterFinales.show()
orig_stdout = sys.stdout
f = open('DocumentosTexto/modelosShipper.txt', 'w')
sys.stdout = f

soportes=[80]
print("ALGORITMO LOG CLUSTER")
for j in range(len(resultadoLogClusterApacheSpark)):
    print("Modelos para los patrones obtenidos con el algoritmo LogCluster y con soporte "+str(soportes[j])+"%")
    modelosLogCluster=obtenerModelos.obtenerExpresionRegularLogCluster(resultadoLogClusterApacheSpark[j]['Patrones'])
    for i in range(len(modelosLogCluster)):
        print("Patron:")
        print(resultadoLogClusterApacheSpark[j]['Patrones'][i])
        print("Modelo:")
        print(modelosLogCluster[i])
    print("")

print("ALGORITMO SLCT")
for j in range(len(resultadoSlct)):
    print("Modelos para los patrones obtenidos con el algoritmo Slct y con soporte "+str(soportes[j])+"%")    
    modelosSlct=obtenerModelos.obtenerExpresionRegularLogCluster(resultadoSlct[j]['Patrones'])
    for i in range(len(modelosSlct)):
        print("Patron:")
        print(resultadoSlct[j]['Patrones'][i])
        print("Modelo:")
        print(modelosSlct[i])
    print("")        

#Cerramos el flujo.
sys.stdout = orig_stdout
f.close()

print("FIN")