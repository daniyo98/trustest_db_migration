import pymysql
def filtrarDatosColumnas(datosColumnas):
    datosFiltrados=[]
    lista = []
    datosFiltradosTupla = ()
    numeroCampos = len(datosColumnas)
    for i in range(numeroCampos):
        numeroItems = len(datosColumnas[i])
        for e in range(numeroItems):
            item = ""
            for letra in datosColumnas[i][e]:
                    item = item + str(letra)
                    pass
#####################AÑADE UN NUEVO ELEMENTO DE LA TUPLA PARA RECONOCER POR EJEMPLO "PRI" = "PRIMARY KEY"######################
            if item == "PRI":
                item = "PRIMARY KEY"
            if item == "MUL":
                item = "MULTIFIED"
            datosFiltrados.append(item)
            if e == numeroItems-1:
                lista.append(datosFiltrados)
                datosFiltrados = []
    datosFiltradosTupla = tuple(lista)
    print(datosFiltradosTupla)
    print(datosColumnas)
    print("\n")
    return datosFiltradosTupla

def eliminarCaracteres(tupla, noTablas):
    lista=[]
    for i in range(noTablas):
        palabra=tupla[i]
        for letra in palabra:
            campoTabla=letra
        lista.append(campoTabla)
    return lista
def crearBasedeDatos():
    try:
        print("Aaccediendo a base de datos en el destino...")
        connDB = pymysql.connect(host=hostNameDes, port=puertoNoDes, user=usuarioDes, passwd=contraseñaDes)
        print("Conectado :D")
        print("Creando base de datos llamada \"" + baseDeDatosDes + "\" con sus campos correspondientes...")
        queryPruebas = connDB.cursor()
        queryPruebas.execute("CREATE DATABASE " + baseDeDatosDes)
        queryPruebas.execute("USE " + baseDeDatosDes)
        print("Creada con exito")
    except:
        print(Exception)
        print("Algo fallo, verifica si la base de datos fue creada")
    queryPruebas.close()
    connDB.close()


def crearTablas(listaTablas, datosColumnas, valor):
    connTab = pymysql.connect(host=hostNameDes, port=puertoNoDes, user=usuarioDes, passwd=contraseñaDes)
    query=connTab.cursor()
    datosColumnas=filtrarDatosColumnas(datosColumnas)
    numeroCampos = len(datosColumnas)
    Texto = "CREATE TABLE " + listaTablas[valor] + " ("
    for con in range(numeroCampos):
        numeroItems=len(datosColumnas[con])
        for item in range(numeroItems):
            campo = datosColumnas[con][item]
            Texto = Texto+" "+campo+""
        Texto=Texto+","
    Texto = Texto.rstrip(Texto[-1])
    Texto = Texto + ")"
    query.execute("USE "+baseDeDatosDes+";")
    print(Texto)
    #query.execute(Texto)




hostNameOri = "localhost" ####Direccion del host
puertoNoOri= 3306 ####Numero del puerto
usuarioOri= "root"   ####Usuario
contraseñaOri= "root" ####Contraseña
baseDeDatosOri = "world" ####Base de datos del cual extraerlos
#######################################################################
hostNameDes= "localhost" ####Direccion del host
puertoNoDes= 3306 ####Numero del puerto
usuarioDes= "root"   ####Usuario
contraseñaDes= "root" ####Contraseña
baseDeDatosDes="Pruebas" ###Base de datos destino a crear
###############################DATOS IMPORTANTES#########################
conn = pymysql.connect(host=hostNameOri, port=puertoNoOri, user=usuarioOri, passwd=contraseñaOri, db=baseDeDatosOri)
print("Conectando a ..."+hostNameOri)
cursor = conn.cursor()
print("Buscando DB..."+baseDeDatosOri)
cursor.execute("USE "+ str(baseDeDatosOri)+";")
print("Conectado :D")
cursor.execute("SHOW TABLES")
noTablas = len(cursor.fetchall())
print("Numero de tablas originales: "+ str(noTablas)+"")
crearBasedeDatos()
for i in range(noTablas):
    cursor.execute("SHOW TABLES")
    query=conn.cursor()
    tupla = cursor.fetchall()
    listaTablas = eliminarCaracteres(tupla, noTablas)
    query.execute("SELECT COLUMN_NAME ,COLUMN_TYPE, COLUMN_KEY, EXTRA FROM Information_Schema.Columns WHERE TABLE_NAME= \""+listaTablas[i]+"\" And TABLE_SCHEMA = \""+baseDeDatosOri+"\" ORDER BY ORDINAL_POSITION;")
    datosColumnas=query.fetchall()
    crearTablas(listaTablas,datosColumnas,i)
print("Finalizo exitosamente")
query.close()
cursor.close()