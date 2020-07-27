# Help Documentation
Welcome to the "Help Desk" for the NC Data Dashboard on GitHub.  If you have a question about the code, it will (hopefully) be answered here.  Have a question that you cannot find an answer for? Please submit and issue and we will get to it as soon as we can.<br /><br />If you have questions about the [NC Data Dashboard](https://www.wcu.edu/engage/regional-development/data-dashboard.aspx) or [Western Carolina University](wcu.edu), please select one to learn more.
*** 
### Table of Contents
1. [Modules](#Modules)
2. [Backups](#Backups)
3. [Loading Data](#Loading-Data)
4. [Cleaning Data](#Cleaning-Data)
5. [Loading to the Database](#Loading-to-the-Database)
6. [More Information](#More-Information)

Note: Anything directly in [ ] square brackets denotes a user input field and must be changed to match user settings before code will run successfully.
***
### Modules
<details><summary><a href=https://www.geeksforgeeks.org/python-urllib-module/>urllib</a>: the URL handling module for Python.</summary><br>Installation:

    py -m pip install urllib --upgrade
</details>


<details><summary><a href=>io</a>: the io module provides Python’s main facilities for dealing with various types of I/O (input/output).</summary><br>Usage:

    from io import BytesIO

    zip_file = ZipFile(BytesIO(response.content))

</details>


<details><summary><a href=https://docs.python.org/3/library/zipfile.html>zipfile</a>: this module provides tools to create, read, write, append, and list a ZIP file.</summary><br>Usage:

    from zipfile import ZipFile

    zip_file = ZipFile[io import](response.content))
    files = zip_file.namelist()
    with zip_file.open(files[number of file in list]) as [filetype1]:
        data_frame = pd.read_[filetype2]("[filetype1]", encoding="[type]", sep="[type]")
</details>


<details><summary><a href=https://pypi.org/project/pandas/>pandas</a>: a Python package providing fast, flexible, and expressive data structures designed to make working with structured (tabular, multidimensional, potentially heterogeneous) and time series data both easy and intuitive.</summary><br>Installation:

    py -m pip install pandas --upgrade
<br>
Usage:
    
    import pandas as pd
    
    data_frame = pd.read_csv("[filename]", encoding="[type]")
</details>


<details><summary><a href=https://pypi.org/project/requests/>requests</a>: allows you to send HTTP/1.1 requests extremely easily. </summary><br>Installation:

    py -m pip install requests --upgrade
<br>
Usage:

    response = requests.get("[download_link]")
</details>

<details><summary><a href=https://pypi.org/project/pyodbc/>pyodbc</a>: an open source Python module that makes accessing ODBC databases simple.</summary><br>Installation:

    py -m pip install pyodbc --upgrade
<br>
Usage:

    import pyodbc
    
    con = pyodbc.connect("Driver={SQL Server}:"
                         "Server=[server_name];"
                         "Database=[database_name];"
                         "Trusted_Connection=yes;",
                         autocommit=True)
<br>                         
After the connection to the database has been set up, we have to create a cursor to run SQL commands in a Python file.
    
    c = con.cursor()
<br>
When the cursor has been created, we can now use it so perform SQL queries:
    
    c.execute("[sql query]")
</details>


<details><summary><a href=https://pypi.org/project/SQLAlchemy/>sqlalchemy</a>: the Python SQL toolkit and Object Relational Mapper that gives application developers the full power and flexibility of SQL.</summary><br>Installation:

    py -m pip install SQLAlchemy --upgrade
<br>   
Usage:

    from sqlalchemy import create_engine
</details>


<details><summary><a href=https://pypi.org/project/numpy/>numpy</a>: the fundamental package for array computing with Python.</summary><br>Installation:

    py -m pip install numpy --upgrade
<br>
Usage:

    import numpy as np

    data_frame["column1"] = data_frame["column1"].replace(np.nan, "", regex=True)
</details>
<br>

### Other Modules
<details><summary><a href=https://pypi.org/project/pylint/>pylint</a>: a Python static code analysis tool which looks for programming errors, helps enforcing a coding standard, sniffs for code smells and offers simple refactoring suggestions.</summary><br>Installation:

    py -m pip install pylint --upgrade
<br>
Usage (in command line):

    pylint [filename]
</details>


<details><summary><a href=https://pypi.org/project/black/#:~:text=Black%20is%20the%20uncompromising%20Python%20code%20formatter.%20By,time%20and%20mental%20energy%20for%20more%20important%20matters.>black</a>: the uncompromising Python code formatter.</summary><br>Installation:

    py -m pip install black --upgrade
<br>
Usage (in command line):

    black [filename]
</details>
<br>

Notes: 
* In order to keep methods uniform for easy use and application in the future, many, if not all scripts use the same modules. 
* Modules should be added in order they are listed in this doc to follow current Python PEP8 guidelines for readiblity and function.
***
### Backups
***
### Loading Data
***
### Cleaning Data
***
### Loading to the Database
***
### More Information
To view Python Script Documentation, [click here](https://github.com/NC-Data-Dashboard/DataDashboard_Public/blob/master/PythonScriptsDocumentation.md).
***
Last updated: 07.27.2020
