import regex as re


#Vamos a crear una clase que nos encontrara los patrones deseados.
class findPattern():
    #Definimos los patrones que se quieren buscar.
    MONTHS_PATTERN = 'january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec'
    DAYS_PATTERN = 'monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|tues|wed|thur|thurs|fri|sat|sun'    
    DIGITS_PATTERN = '[^A-Za-z|\.]\d+'
    DELIMITERS_PATTERN = '[\-\,\s\_\+\@]+'
    TIME_PERIOD_PATTERN = 'a\.m\.|am|p\.m\.|pm'    
    
    ##Caracteres que se pueden eliminar de los extremos cuando hay cadenas combinadas.    
    STRIP_CHARS = ' \n\t:-.,_'
    
    #Patron definido para encontrar la hora.
    TIME_PATTERN = """
    (?P<time>
        ## Captures in format XX:YY(:ZZ)
        (
            (?P<hours>\d{{2,2}})
            \:
            (?P<minutes>\d{{1,2}})
            \:
            (?P<seconds>\d{{1,2}})
            \.
            (?P<miliseconds>\d{{3,3}})
            \s*\+
            (?P<resto>\d{{2,2}})
            \:
            (?P<resto2>\d{{2,2}})
        )
        |
        (
           (?P<hours>\d{{1,2}})
           \:
           (?P<minutes>\d{{1,2}})
           (\:(?<seconds>\d{{1,2}}))
           \s*
           (?P<time_periods>{time_periods})?
        )


    )
    """.format(
        time_periods=TIME_PERIOD_PATTERN
    )
    
    DATES_PATTERN = """
    (
        ## Grab any digits
        (
           (?P<months>{months})
           (?P<delimiters>{delimiters})
           (?P<days>\d{{1,2}})
           (?P<delimiters>{delimiters})?
           (?P<year>\d{{4}})?
        )
        |
        (
           (?P<days>\d{{1,2}})
           (?P<delimiters>{delimiters})
           (?P<months>{months})
        )
        |
        (
           (?P<year>\d{{2,4}})
           (?P<delimiters>{delimiters})
           (?P<months>\d{{1,2}})
           (?P<delimiters>{delimiters})
           (?P<days>\d{{1,2}})
        )
    )
    """

    DATES_PATTERN = DATES_PATTERN.format(
        digits=DIGITS_PATTERN,
        months=MONTHS_PATTERN,
        delimiters=DELIMITERS_PATTERN
    )    
    
    #Compilamos para hacer las expresiones regulares.
    #Para fechas
    DATE_REGEX = re.compile(DATES_PATTERN, re.IGNORECASE | re.MULTILINE | re.UNICODE | re.DOTALL | re.VERBOSE)
    #Para horas
    TIME_REGEX = re.compile(TIME_PATTERN, re.IGNORECASE | re.MULTILINE | re.UNICODE | re.DOTALL | re.VERBOSE)


    #Creamos el constructor de la clase
    def __init__(self, base_date=None):
        self.base_date = base_date
        
    #Creamos un metodo que busque las fechas.
    def find_dates(self, text, source=False, index=False, strict=False):
        for date_string, indices, captures in self.extract_date_strings(text, strict=strict):
            #as_dt = self.parse_date_string(date_string, captures)
            if date_string is None:
                ## Dateutil couldn't make heads or tails of it
                ## move on to next
                continue

            returnables = (date_string,)
            if source:
                returnables = returnables + (date_string,)
            if index:
                returnables = returnables + (indices,)

            if len(returnables) == 1:
                returnables = returnables[0]
            yield returnables        
    
    
    #Funcion que escane el texto buscando formatos de fechas
    def extract_date_strings(self, text, strict=False):
        #Buscamos en el texto las posibles cadenas candidatas a ser una fecha.        
        for match in self.DATE_REGEX.finditer(text):
            match_str = match.group(0)
            indices = match.span(0)

            ## Get individual group matches
            captures = match.capturesdict()
            months = captures.get('months')[0]
            days = captures.get('days')[0]
            if(len(months)<=2):
                months=int(months)
            if(len(days)<=2):
                days=int(days)
            if(type(months)==unicode):
                months=str(months)            
            if((type(months)==int and months<=12) or type(months)==str):
                if((type(days)==int and days<=31) or type(days)==str):


                    ## sanitize date string
                    ## replace unhelpful whitespace characters with single whitespace
                    match_str = re.sub('[\n\t\s\xa0]+', ' ', match_str)
                    match_str = match_str.strip(self.STRIP_CHARS)

                    ## Save sanitized source string
                    yield match_str, indices, captures
            
    def find_time(self, text, source=False, index=False, strict=False):
        for date_string, indices, captures in self.extract_time_strings(text, strict=strict):
            if date_string is None:
                ## Dateutil couldn't make heads or tails of it
                ## move on to next
                continue

            returnables = (date_string,)
            
            if source:
                returnables = returnables + (date_string,)
            if index:
                returnables = returnables + (indices,)

            if len(returnables) == 1:
                returnables = returnables[0]
            yield returnables   
        
    #Funcion que busca formato de fechas por el texto y lo extrae.
    def extract_time_strings(self, text, strict=False):
        #Buscamos en el texto las posibles cadenas candidatas a ser una hora.
        for match in self.TIME_REGEX.finditer(text):
            #Almacenamos en una variable la cadena candidata encontrada.
            match_str = match.group(0)
            #Almacenamos en la variable indices (tupla) los indices en el cual empieza la cadena y 
            #en el que termina.
            indices = match.span(0)

            #Obtenemos los resultados de forma indidual para realizar las comprobaciones.
            captures = match.capturesdict()
            horas = int(captures.get('hours')[0])
            minutos=int(captures.get('minutes')[0])
            segundos = captures.get('seconds')
            if(len(segundos)>0):
                segundos=int(segundos[0])
            #Comprobamos que las horas, minutos y segundos tienen valores correctos.
            if(horas >=0 and horas<24 and minutos>=0 and minutos<=59 and((type(segundos)==int and segundos>=0 and segundos<60) or type(segundos)==list)):
                #Limpiamos el string de fechas
                match_str = re.sub('[\n\t\s\xa0]+', ' ', match_str)
                #Reemplazamos el conjunto de espacios en blanco inutiles por espacios en blanco individuales.
                match_str = match_str.strip(self.STRIP_CHARS)

                ## Save sanitized source string
                yield match_str, indices, captures    
    
#Buscamos las fechas
def find_dates(text,source=False,index=False,strict=False,base_date=None):
    date_finder = findPattern(base_date=base_date)
    return date_finder.find_dates(text, source=source, index=index, strict=strict)


def find_time(text,source=False,index=False,strict=False,base_date=None):
    """
    Extract datetime strings from text
    :param text:
        A string that contains one or more natural language or literal
        datetime strings
    :type text: str|unicode
    :param source:
        Return the original string segment
    :type source: boolean
    :param index:
        Return the indices where the datetime string was located in text
    :type index: boolean
    :param strict:
        Only return datetimes with complete date information. For example:
        `July 2016` of `Monday` will not return datetimes.
        `May 16, 2015` will return datetimes.
    :type strict: boolean
    :param base_date:
        Set a default base datetime when parsing incomplete dates
    :type base_date: datetime
    :return: Returns a generator that produces :mod:`datetime.datetime` objects,
        or a tuple with the source text and index, if requested
    """
    time_finder = findPattern(base_date=base_date)
    return time_finder.find_time(text, source=source, index=index, strict=strict)