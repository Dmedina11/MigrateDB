#!/usr/bin/python

# NewInsertTables,py, script que permite la insercion de datos a las tablas que presentan alguna relacion entre ellas
# en la base de datos, estas son usuarios, equipos, macs y usuariosequipos, comienza con el llenado de la tabla usuario
# para luego continuar con los equipos y las macs, en general hace una migracion completa de los usuarios y equipos
# y solo asocia las macs que se encuentran en el csv de entrada, no obstante hace una eliminacion de los usuarios que
# no presentan equipos asociados y tambien de aquellos equipos que no presentan macs asociada, ademas elimina de lleno
# aquellos equipos que presentan mas de 2 macs asociadas y genera un listado con la informacion correspondiente de esos
# usuarios en formato csv, para ello se apoya del script RemoveElementsConflictiv.py
#
# Copyright (C) 30/07/2015 David Alfredo Medina Ortiz  dmedina11@alumnos.utalca.cl
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA

#modulos
import sys
import os
import psycopg2
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
                        FileTransferSpeed, FormatLabel, Percentage, \
                        ProgressBar, ReverseBar, RotatingMarker, \
                        SimpleProgress, Timer

#funcion que lee un archivo csv y obtiene la informacion de la conexion a la base de datos, retorna
#un diccionario con esta informacion, teniendo como key si es import o export dependiendo sea el caso
#y como se establezca en los input del script
#name_file = archivo con la informacion a leer
def ReadCSV (name_file):
	
	information_DB = {}#declaracion del diccionario
	logs = open("FilesOutput/TablesRelations_Logs_error.txt", 'a')#abrimos archivo log
	
	try:#manejo de errores	
		cont=0#para contar las lineas
		file_read = open(name_file)#abrimos archivo
		line = file_read.readline()#leemos primera linea
		while line:#ciclo de lectura
		
			new_line = line.replace("\n", "")#quitamos el enter
			split_line = new_line.split(';')#separamos por ;

			key=''#asigamos key segun linea
			if cont==0:
				key = 'import'
			else:
				key='export'

			information_DB[key] = split_line#asignacion a diccionario
			line = file_read.readline()
			cont+=1#aumentamos lineas
		file_read.close()
	except:#manejo de exception
		logs.write("No es posible abrir el archivo con la informacion de la conexion a la base de datos\n")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: TablesRelations_Logs_error.txt")

	logs.close()
	return information_DB#se retorna el diccionario..

#funcion que permite la conexion con la base de datos, los parametros representan:
#host => el host a conectarse, descrito por la ip
#dbname => nombre de la base de datos
#user => usuario con el cual se accedera a la base de datos
#password => clave para el usuario que se conectara a la base de datos
def ConnectDB (host, dbname, user, password):

	logs = open("FilesOutput/TablesRelations_Logs_error.txt", 'a')#abrimos archivo log
	connection = "host= %s dbname= %s user=%s password=%s" % (host, dbname, user, password)#conexion con la informacion
	var = "Connecting to database\n	->%s" % (connection)#se muestra en el log la informacion 
	logs.write(var+"\n")

	try: #manejo de try/exception
		conn = psycopg2.connect(connection)#se obtiene la conexion
		cursor_db = conn.cursor()#se recupera un cursor
		logs.write("Connected!\n")
	except:#manejo de exception
		logs.write("No es posible conectarse a la base de datos")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: TablesRelations_Logs_error.txt")
	logs.close()
	return conn, cursor_db#se retorna la conexion y el cursor de la conexion

#funcion que permite la creacion de los outputs que se generan en la ejecucion del script
def CreateFileOuputs():

	logs = open("FilesOutput/TablesRelations_Logs_error.txt", 'w')#archivo para el aviso de errores
	logs.close()
	
#funcion que crea un diccionario con los datos y su clave es la clave primaria, hace una consulta y obtiene la informacion
#cursor_db => representa la conexion con la base de datos 
#table => representa la tabla bajo la cual se hara la consulta...
#index => la posicion en el arreglo que contiene la respuesta que posee la informacion que deseo
def MakeDictionaryTablesIndex (cursor_db, table, index):

	logs = open("FilesOutput/TablesRelations_Logs_error.txt", 'a')#abrimos archivo log

	try:#manejo de errores
		dictionary = {}#creacion de diccionario
		query = "SELECT * FROM %s" % table#desarrollo de consulta
		cursor_db.execute(query)#se hace la consulta
		for row in cursor_db:#insercion de elementos en el diccionario, dependiendo del valor de indice---
			if index == 0:
				new_row = row[0].upper()
				dictionary[new_row] = row[1]
			else:
				new_row = row[1].upper()
				dictionary[new_row] = row[0]
	except:#manejo de errores
		logs.write("No es posible realizar la consulta a la base de datos (MakeDictionaryTablesIndex)\n")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: TablesRelations_Logs_error.txt")
	logs.close()
	return dictionary#se retorna el diccionario...

#funcion que permite buscar en el diccionario y entrega informacion sobre el valor de la clave...
#dictionary_import => diccionario que permite buscar la informacion en base a la existente en la base de datos import
#id_compare => el id a comparar para obtener el valor...
def SearchInformationInDictionary(dictionary_import, id_compare):

	key = ''
	for element in dictionary_import:#se recorre cada elemento del diccionario
		if dictionary_import[element] == id_compare:#hacemos la comparacion
			return element#retornamos el elemento si lo encontramos...
			break

	#ahora insertamos los equipos de los usuarios conflictivos, es decir los equipos de los usuarios cachos...
#funcion que permite hacer la lectura y obtener todas las posibles macs a ser insertadas en la base de datos export, las cuales provienen del csv
#name_file => representa el nombre del archivo
def ReadText (name_file):

	logs = open("FilesOutput/TablesRelations_Logs_error.txt", 'a')#abrimos archivo log
	try:
		list_macs = []#estructura de almacenamiento
		file_read = open(name_file)#abrimos archivo
		line = file_read.readline()#leemos la primera linea
		while line:#ciclo de lectura...
			new_line = line.replace('\n', '')#reemplazamos el enter y agregamos a la lista
			list_macs.append(new_line)
			line = file_read.readline()
		file_read.close()
	except:#manejo de exceptions
		logs.write("No es posible abrir el archivo con la informacion de las macs")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: TablesRelations_Logs_error.txt")
	logs.close()
	return list_macs#se retorna la lista con las macs capturadas...

#funcion que permite la insercion de elementos en la tabla macs...
#cursor_db => cursor de la conexion a la base de datos que se tiene acceso
#connection => conexion a la base de datos
#sequence => representa una secuencia en el sql cargado y que hace autoincrementable al id
#mac => campo de la tabla macs
#tipo_interfaz => campo de la tabla macs
#id_equipo => campo de la tabla macs
def InsertElementIntoMacs(cursor_db, connection, sequence, mac, tipo_interfaz, id_equipo):

	logs = open("FilesOutput/TablesRelations_Logs_error.txt", 'a')#file to logs and errors in execution of script
	
	try:
		#if (CheckExistenceInDB(cursor_db, 'macs', 'mac', mac)==0):#revisamos si existe el elemento
		new_mac = mac.upper()#cambiamos a mayusculas
		split_interfaz = tipo_interfaz.split(' ')
		new_interfaz = split_interfaz[0].upper()#tipo_interfaz must be alambrica or inalambrica
		query = "INSERT INTO macs VALUES (nextval('%s'), '%s', '%s', %d)" % (sequence, new_interfaz, new_mac, id_equipo)#consulta
		cursor_db.execute(query)#ejecucion
		connection.commit()#actualizacion
		#else:#aviso en el caso de que ya existiese...
		#	var =  "mac %s exists in table macs" % mac
		#	logs.write(var+"\n")
	except:#manejo de exception
		logs.write("No es posible realizar la insercion de datos en la tabla macs (InsertElementIntoMacs)")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: TablesRelations_Logs_error.txt")
	logs.close()

#funcion que permite obtener la informacion asociada a las macs que posea un equipo con una serial en particular...
#cursor_db => cursor de la conexion a la base de datos a la cual se tiene acceso
#serial => campo en la tabla macs en la base de datos import el cual asocia la mac al dispositivo
def GetInfoOfMacsEquipo(cursor_db, serial):

	logs = open("FilesOutput/TablesRelations_Logs_error.txt", 'a')#archivo log
	information_macs = []#almacenara la informacion de las mac
	
	try:
		query = "select * from macs where macs.serial = '%s'" % serial#hacemos la consulta
		cursor_db.execute(query)#ejecutamos
		for row in cursor_db:#obtenemos todas las macs asociadas al equipo en particular...
			information_macs.append(row)
	except:#manejo de exceptions
		logs.write("No es posible realizar la consulta en la base de datos (GetInfoOfMacsEquipo)")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: TablesRelations_Logs_error.txt")
	logs.close()
	return information_macs#retornamos la informacion recopilada...

#funcion que permite crear un diccionario en el cual se obtendra el id del equipo y 
#el nombre que corresponde a la serial del dispositivo...
#cursor_db_export => cursor de la conexion a la base de datos
def CreateDiccitionarySerialEquipo(cursor_db):

	logs = open("FilesOutput/TablesRelations_Logs_error.txt", 'a')#archivo log

	serial_dictionary = {}#declaracion del diccionario
	try:
		query = "select equipo.id, equipo.nombreequipo from equipo"#consulta
		cursor_db.execute(query)#ejecucion
		for row in cursor_db:#obtenemos la informacion serial (nombre actual del equipo) => id equipo
			serial_dictionary[row[1]] = row[0]
	except:
		logs.write("No es posible realizar la consulta en la base de datos (CreateDiccitionarySerialEquipo)")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: TablesRelations_Logs_error.txt")
	logs.close()
	return serial_dictionary

#funcion que permite poder obtener el tipo de interfaz...
#cursor_db_export => cursor de la conexion a la base de datos
#id_interfaz => campo bajo el cual se hara la comparacion
def GetTipoInterfaz(cursor_db, id_interfaz):

	interfaz = ''#contendra la respuesta
	try:
		query = "select tipo_interfaz.nom_tipo_int from tipo_interfaz where id_tipo_int = %d" % id_interfaz#consulta
		cursor_db.execute(query)#ejecucion
		for row in cursor_db:#obtenemos la respuesta
			interfaz = row[0]
			break
	except:#manejo de exception
		logs.write("No es posible realizar la consulta a la base de datos (GetTipoInterfaz)")
		sys.exit("ejecucion del script interrumpida, por favor revisar el archivo: TablesRelations_Logs_error.txt")
	interfaz_modificada = interfaz.split(' ')#hacemos el split
	return interfaz_modificada[0].upper()#retornamos la primera posicion en mayusculas

#funcion que permite insertar la informacion en la tabla macs....
#cursor_db_export => cursor de la conexion a la base de datos export
#connection_db_export => conexion a la base de datos export
#cursor_db_import => cursor de la conexion a la base de datos import 
#serial_dictionary => diccionario creado con las seriales y los ids de los dispositivos
def InsertMacs(cursor_db_export, connection_db_export, cursor_db_import, serial_dictionary):

	list_macs = ReadText("FilesOutput/GetMacslist_macs_not_exists.txt")#obtenemos la lista de macs existentes en el csv que se forma en el GetMac.py
	print "Insertando datos en tabla Macs"
	for serial in serial_dictionary:#para cada equipo obtenemos todas las macs asociadas...
		info_macs_por_serial = GetInfoOfMacsEquipo(cursor_db_import, serial)#obtenemos la info de las macs que estan asociadas a este equipo
		for info_mac in info_macs_por_serial:#buscamos si las macs que vienen en esta lista existen en el csv...
			#if str(info_mac[0]) == str(split_line[0]):#la mac existe...
			#if serial == info_mac[1]:#comparo si las seriales son iguales...
			#obtenemos la informacion y agregamos...
			tipo_interfaz = GetTipoInterfaz(cursor_db_import, info_mac[2])
			id_equipo = serial_dictionary[serial]
			#hacemos la insercion...
			InsertElementIntoMacs(cursor_db_export, connection_db_export, 'macs_id_seq', info_mac[0], tipo_interfaz, id_equipo)

#funcion principal				
def main ():

	CreateFileOuputs()#creamos los archivos de salida
	information_DB = ReadCSV(sys.argv[1])#obtenemos la informacion de la base de datos...
	data_connected_import = ConnectDB(information_DB['import'][0], information_DB['import'][1], information_DB['import'][2], information_DB['import'][3])#conexion db import
	data_connected_export = ConnectDB(information_DB['export'][0], information_DB['export'][1], information_DB['export'][2], information_DB['export'][3])#conexion db export
	#creacion de los diccionarios
	kind_user_import = MakeDictionaryTablesIndex(data_connected_import[1], "tipo_usuario", 0)
	kind_user_export = MakeDictionaryTablesIndex(data_connected_export[1], "tipousuario", 1)	
	kind_device_import = MakeDictionaryTablesIndex(data_connected_import[1], "tipo_dispositivo", 1)
	kind_device_export = MakeDictionaryTablesIndex(data_connected_export[1], "tipodispositivo", 1)
	state_device_import = MakeDictionaryTablesIndex(data_connected_import[1], "estado", 1)
	state_device_export = MakeDictionaryTablesIndex(data_connected_export[1], "estadodispositivo", 1)
	trademark_import = MakeDictionaryTablesIndex(data_connected_import[1], "marca", 1)
	trademark_export = MakeDictionaryTablesIndex(data_connected_export[1], "marca", 1)
	estado_usuario_export = MakeDictionaryTablesIndex(data_connected_export[1], "estadousuario", 1)
	print "Migrando datos..."
	serial_dictionary= CreateDiccitionarySerialEquipo(data_connected_export[1])#hacemos un diccionario nombreequipo -> id para hacer la busqueda por serial
	#con el serial_dictionary es posible trabajar con la serial asociada a la mac como key y el indice del equipo asociado...
	InsertMacs(data_connected_export[1], data_connected_export[0], data_connected_import[1], serial_dictionary)
	
	return 0

#llamada a la funcion principal...
if __name__ == '__main__':
	main()