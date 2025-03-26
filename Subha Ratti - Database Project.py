"""
Description: A database program that imports a CSV or Text File from the user and executes the requested SQL commands
Programmer: Subha Ratti
Date: October 20, 2023
Version: 1.0

Pre-condition: User inputs the file name, delimeter, and table name. Once table is imported to database, user enters SQL commands
Post-condition: Program imports table to created database and executed requested SQL commands by user (unless error occurs, in which error message is printed)
"""
import sqlite3
import csv

def create_database(file_name, table_name, delimiter, file_type): #This function created the database depending on the file type by calling on the required function
                                                                  #and getting the folder path to ensure the database is in the input file's folder
    try:
        db_file = get_folder_path(file_name)
        if file_type == "csv":
            conn = create_database_from_csv(file_name, table_name, delimiter, db_file)
        elif file_type == "text":
            conn = create_database_from_text(file_name, table_name, delimiter, db_file)
        return conn #Returns the connection address to do used for executed SQL commands
    
    except FileNotFoundError: #Error if the file does not exist (incorrect address, etc.)
        print("Problem reading input file.")
    except: #Error for any other issues when created and inserting table into database
        print("Invalid CSV/Text File Format.")

def get_folder_path(file_name): #This function gets the folder path of the inputted file by splitting the file_name inputted, checking if it contains more than one part (one part means
                                #user inputted no folder path (same folder as program)) and if it does, it extracts the folder path by removing the last item (which is the input file)
    folder_parts = file_name.split("\\")
    if len(folder_parts) > 1:
        folder_parts.pop() #Removes the last item
        folder_path = "\\".join(folder_parts) + "\\" #Joins the list using a backslash (using two backslahes due to Python's rule on how to read a backslash)
    else:
        folder_path = "" #No folder path when there is nowhere to split the file_type (inputted file in same place as program)
    db_file = folder_path + table_name + ".db"
    return db_file

def create_database_from_csv(file_name, table_name, delimiter, db_file): #This function creates the database if there is a CSV file inputted
    with open(file_name, newline="") as csv_file:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("drop table if exists " + table_name)
        csv_reader = csv.reader(csv_file, delimiter=delimiter)

        header_line = ""
        for row in csv_reader:
            header_line = row #Extracts first row of inputted file (containing header) and breaks
            break
        headers = header_line

        headers_stripped = []
        data_types = []

        for i in range(len(headers)): 
            header_split = headers[i].split("-")
            data_types.append(header_split[1]) #Assigns data type to list
            headers_stripped.append(header_split[0]) #Assigns header name to list

        table_create_sql = "create table " + table_name + " (" #Creates a string with the headers and their data types using a for loop
        for i in range(len(headers)):
            table_create_sql += headers_stripped[i].strip() + " " + data_types[i].strip()
            if i < len(headers) - 1:
                table_create_sql += ", " #Adds after every element but the last (less than length - eg. if len(headers) = 8, and on the last iteration, i=7, len(headers)-1 = 7, not less than)
        table_create_sql += ")"
        cursor.execute(table_create_sql)
        conn.commit()
        
        for row in csv_reader:
            insert_sql = "insert into " + table_name + " (" #Creates a string with the headers and then the placeholders to hold the values
            for i in range(len(headers)):
                insert_sql += headers_stripped[i].strip()
                if i < len(headers) - 1:
                    insert_sql += ", "
            insert_sql += ") values ("
            for i in range(len(headers)):
                insert_sql += "'" + str(row[i]) + "'" # Convert the value to a string and enclose it in quotations
                if i < len(headers) - 1:
                    insert_sql += ", "
            insert_sql += ")"
            cursor.execute(insert_sql)
            conn.commit()

        return conn

def create_database_from_text(file_name, table_name, delimiter, db_file): #This function creates the database if there is a Text file inputted
    with open(file_name, newline="") as text_file:
        lines = text_file.readlines()
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute("drop table if exists " + table_name)
        
        headers = lines[0].strip().split(delimiter) #Extracts the header by taking the first line from the text file

        for i in range(len(headers)):
            header = headers[i]
            headers[i] = header.replace("-", " ")

        table_create_sql = "create table " + table_name + " (" #Creates table by adding each header seperated with comma
        for i in range(len(headers)):
            table_create_sql += headers[i].strip()
            if i < len(headers) - 1:
                table_create_sql += ", "
        table_create_sql += ")"
        cursor.execute(table_create_sql)
        conn.commit()

        for line in lines[1:]:
            data = line.strip().split(delimiter) #Extracts all other lines in the text file after header

            insert_sql = "insert into " + table_name + " ("
            for i in range(len(headers)): 
                header_split = headers[i].strip().split(" ") #Adds header and then the column header at the 0 index (because the data type would be the 1 index)
                insert_sql += header_split[0].strip()
                if i < len(headers) - 1 :
                    insert_sql += ", "
            insert_sql += ") values ("
            for i in range(len(headers)):
                insert_sql += "'" + str(data[i]).strip() + "'" # Convert the value to a string and enclose it in quotations to avoid error
                if i < len(headers) - 1:
                    insert_sql += ", "
            insert_sql += ")"
            
            cursor.execute(insert_sql)
            conn.commit()

        return conn

def execute_sql(conn, sql_command): #This function executed the sql command inputted by the user by first making sure it is correct, then fetches the info, and prints it + num. of rows
    try:
        cursor = conn.cursor()
        cursor.execute(sql_command)
        if sql_command.strip().lower() == "quit": #Exits if quit entered
            conn.close()
        elif sql_command.strip().lower().startswith("select"): #First makes sure it starts with select
            results = cursor.fetchall() #If, at any time, the function realized that their is an error if the way the SQL command was inputted, it jumps to except error block
            for row in results:
                print(row)
            print(str(len(results)) + " row(s) are returned." + "=" * 34)
        else:
            conn.commit()
    except: #Prints if any error with reading SQL command occurs
        print("Invalid SQL command")

while True:
    print("Welcome to DBViewer")
    user_input = input("Enter CSV/Text File to open. Type quit to terminate the program: ").strip()
    if user_input.lower() == "quit": #Breaks out of main while loop
        break

    file_name = user_input
    file_type = "csv" if file_name.lower().endswith(".csv") else "text" #Finds file type using list comprehension to see the ending (.csv or .txt) inputted user file ends with
    delimiter = input("Enter the delimiter: ").strip()
    table_name = input("Enter Table Name: ")
    
    conn = create_database(file_name, table_name, delimiter, file_type)
    
    if conn:
        while True:
            sql_command = input("Enter a valid SQL command to execute. Type quit to exit: ")
            if sql_command.lower() == "quit": #Breaks out of SQL loop
                conn.close()
                break
            execute_sql(conn, sql_command)

print("Thanks for using DBViewer. See you next time.")
