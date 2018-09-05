import re

def obtenerExpresionRegularLogCluster(x):
    #Expresion regular que busca el formato de las palabras desconocidas
    patron = re.compile('\*\{\d*,\d*\}') 
    #Expresion regular que busca los numeros para obtener el min y max
    patronMinMax=re.compile('\d*')     
    patrones=[]
    for j in range(len(x)):
        linea=x[j]
        expresion="^"
        palabras=linea.split()
        for i in range(len(palabras)):
            #Utilizando una expresion regular comprobamos si cada palabra de la linea tiene el formato de las palabras sueltas
            palabra=palabras[i]
            busquedaPatron=patron.match(palabra)                  
            if (busquedaPatron==None):
                expresion=expresion+"\s*"
                for letra in palabra:
                    if (letra=="$" or letra=="|" or letra=="^" or letra=="+" or letra=="-" or letra=="*" or letra=="?" or letra=="." or letra=="\\" or letra=="[" or letra=="]" or letra=="{" or letra=="}" or letra=="(" or letra==")"):
                        expresion=expresion+"\\"+letra
                    else:
                        expresion=expresion+letra       
            else:
                cc=patronMinMax.findall(palabra)
                minimo=int(cc[2])
                maximo=int(cc[4])
                resultado=maximo-minimo
                for t in range(minimo):
                    expresion=expresion+"(\s*\S*)"
                for t in range(resultado):
                    expresion=expresion+"(\s*\S*)?"  
        patrones.append(expresion)
    return patrones


def obtenerExpresionRegularSlct(x):
    patron = re.compile('\*') 
    patrones=[]
    for j in range(len(x)):
        linea=x[j]
        expresion="^"
        palabras=linea.split()
        for i in range(len(palabras)):
            palabra=palabras[i]
            busquedaPatron=patron.match(palabra)                  
            if (busquedaPatron==None):               
                for letra in palabra:
                    if (letra=="$" or letra=="|" or letra=="^" or letra=="+" or letra=="-" or letra=="*" or letra=="?" or letra=="." or letra=="\\" or letra=="[" or letra=="]" or letra=="{" or letra=="}" or letra=="(" or letra==")"):
                        expresion=expresion+"\\"+letra
                    else:
                        expresion=expresion+letra                           
            else:
                #expresion=expresion+"\s*"
                expresion=expresion+"\S*"
            expresion=expresion+"\s*"
        patrones.append(expresion)
    return(patrones)


