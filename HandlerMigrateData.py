#!/usr/bin/python

# HandlerMigrateData.py, script que permite mantener el control de la ejecucion de los
# scripts por separado, es quien determina si el script se ejecuta o no y evalua el
# siguiente paso a seguir, recibe como argumento el csv con la informacion de las macs
# y el archivo con la informacion de las conexiones, entrega un log con lo que ocurre
# en cada etapa, ademas de los logs que se generan por separado, crea un directorio en
# el cual se depositan los outputs.
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

#funcion que crea los archivos de salida
def CreateFileOuputs():

	logs = open("FilesOutput/logs_error_Handler.txt", 'w')#archivo log
	logs.close()

#funcion que chequea los argumentos que se reciben por linea de comando, en caso de que no sean correctos
def CheckArguments():

	logs = open("FilesOutput/logs_error_Handler.txt", 'a')#archivo log
	name_file = ""

	if (len(sys.argv)!=3):#se pregunta por el largo
		logs.write("Entrega un numero de argumentos correctos\n")
		sys.exit("script interrumpido, favor revisar el archivo FilesOutput/logs_error_Handler.txt")

#funcion que por medio de la lectura de los archivos de salida de los scripts determina si se ejecuta el siguiente paso
#correspondiente en el pipe line establecido
#name_file => representa el nombre del archivo a extraer la informacion
def CheckExecuteNextStep(name_file):

	logs = open("FilesOutput/logs_error_Handler.txt", 'a')#abrimos archivo log
	option=0#para determinar si se continua o no...
	
	try:	
		
		file_read = open(name_file)#abrimos archivo
		line = file_read.readline()#leemos primera linea
		while line:

			if line == "ok":#buscamos la linea que diga ok
				option=1
				break
			line = file_read.readline()

		file_read.close()#cerramos el archivo de texto

	except:#manejo de errores
		logs.write("No es posible determinar la continuacion del script debido a problemas con el archivo log del script anterior\n")
		sys.exit("script interrumpido, favor revisar el archivo FilesOutput/logs_error_Handler.txt")

	logs.close()
	return option

#funcion que permite revisar si la informacion para las conexiones a la base de datos proveniente del csv recibido es correcta
#o no, es decir, si vienen todos los elementos, si esta bien formado, y cosas por el estilo.
#name_file => es el nombre del archivo que se recibe por la linea de comando 
def CheckFileDataDB(name_file):

	logs = open("FilesOutput/logs_error_Handler.txt", 'a')#abrimos archivo log
	long_data = 0#largo de los datos
	cont=0#cantidad de lineas

	try:	
		file_read = open(name_file)#abrimos..
		line = file_read.readline()#leemos primera linea
		last_line =""#para almacenar la ultima linea => conexion a base de datos import
		first_line = line#para almacenar la primera => conexion a base de datos export

		while line:#ciclo de lectura

			new_line = line.replace("\n", "")#quitamos el enter
			cont+=1#aumentamos la cantidad de lineas
			elements = new_line.split(';')#hacemos el split para determinar la cantidad de elementos, estos deben ser 4
			if len(elements)!=4:
				logs.write("cantidad de elementos es distinta a lo requerido (4)")
				sys.exit("script interrumpido, favor revisar el archivo FilesOutput/logs_error_Handler.txt")
			if cont>3:#chequeamos la cantidad de lineas
				logs.write("cantidad de lineas en archivo es distinta a la requerida (2)")
				sys.exit("script interrumpido, favor revisar el archivo FilesOutput/logs_error_Handler.txt")
			
			last_line = line#actualizamos la ultima linea y seguimos leyendo				
			line = file_read.readline()

		file_read.close()
	except:#manejo de errores
		logs.write("no es posible evaluar la informacion del csv\n")
		sys.exit("script interrumpido, favor revisar el archivo FilesOutput/logs_error_Handler.txt")

	logs.close()
	
	last_line = last_line.replace('\n', '')#a la ultima linea le quitamos el enter y formamos un arreglo con sus elementos, lo mismo para la primera
	elements_connection = last_line.split(';')
	first_line = first_line.replace('\n', '')
	elements_connection_import = first_line.split(';')
	
	return elements_connection, elements_connection_import#se retornan informacion sobre las conexiones de bases de datos de import y export

def main ():#funcion principal

	os.system('mkdir FilesOutput')#creamos directorio de salida de archivos...
	
	CreateFileOuputs()#creamos los archivos de salida

	logs = open("FilesOutput/logs_error_Handler.txt", 'a')#abrimos archivo de escritura
	CheckArguments()#se revisan los argumentos
	information_dataexport =CheckFileDataDB(sys.argv[2])#revisa los datos del csv con la informacion de la conexion y genera un arreglo con informacion
	#de la conexion de export e import de las bases de datos...
	
	#ejecucion del primer paso
	csv_information = sys.argv[1]#obtenemos csv
	new_execute = sys.argv[2]#y las conexiones
	csv_information = csv_information.replace(' ', '\ ')#reemplazamos los valores de la ruta si es que existe algun caracter especial
	new_execute = new_execute.replace(' ', '\ ')#lo mismo para el caso de la ruta hacia el csv de la conexion a la base de datos
	var = "python GetMacs.py %s %s %s %s %s" % (csv_information, information_dataexport[0][0], information_dataexport[0][1], information_dataexport[0][2], information_dataexport[0][3])#ejecutamos el primer paso
	print "execute ", var
	os.system(var)

	#chequear para la ejecucion del siguiente paso
	if (CheckExecuteNextStep("FilesOutput/GetMacslogs_error.txt")==1):

		var = "python CompleteTables.py %s" % new_execute
		print "execute ", var
		os.system(var)
		#chequear para la ejecucion del siguiente paso
		if (CheckExecuteNextStep("FilesOutput/CompleteTables_Logs_error.txt")==1):

			var = "python CompleteUsuarios.py %s %s" % (new_execute, csv_information)
			print "execute ", var
			os.system(var)
			var = "python CompleteEquipos.py %s %s" % (new_execute, csv_information)
			print "execute ", var
			os.system(var)
			var = "python CompleteMacs.py %s %s" % (new_execute, csv_information)
			print "execute ", var
			os.system(var)
			var = "python RemoveElementsConflictiv.py %s" % new_execute
			print "execute ", var
			os.system(var)
			var = "python EstadisticasFinales.py %s" % new_execute
			print "excute", var
			os.system(var)
		else:
			logs.write("Script presenta problemas en paso TablesRelations.py\n")	

	else:
		logs.write("Script presenta problemas en paso GetMacs\n")

	#se ejecuta el script que permite obtener la informacion que existe en la base de datos y no en la del csv
	var = "python GenerateListUser.py %s %s %s %s %s" % (csv_information, information_dataexport[1][0], information_dataexport[1][1], information_dataexport[1][2], information_dataexport[1][3])
	print "excute ", var
	os.system(var)
	logs.write("Operations satisfactorily completed\n")
	logs.close()

	return 0

if __name__ == '__main__':#llamada a la funcion principal
	main()