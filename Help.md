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

Note: 
+ Anything directly in [ ] square brackets denotes a user input field and must be changed by user before code will run successfully.
***
### Modules
<details><summary><a href=https://www.geeksforgeeks.org/python-urllib-module/>urllib</a>: the URL handling module for Python.</summary><br>Installation:

    python -m pip install urllib --upgrade
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

    python -m pip install pandas --upgrade
<br>
Usage:
    
    import pandas as pd
    
    data_frame = pd.read_csv("[filename]", encoding="[type]")
</details>


<details><summary><a href=https://pypi.org/project/requests/>requests</a>: allows you to send HTTP/1.1 requests extremely easily. </summary><br>Installation:

    python -m pip install requests --upgrade
<br>
Usage:

    response = requests.get("[download_link]")
</details>

<details><summary><a href=https://pypi.org/project/pyodbc/>pyodbc</a>: an open source Python module that makes accessing ODBC databases simple.</summary><br>Installation:

    python -m pip install pyodbc --upgrade
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

    python -m pip install SQLAlchemy --upgrade
<br>   
Usage:

    from sqlalchemy import create_engine
</details>


<details><summary><a href=https://pypi.org/project/numpy/>numpy</a>: the fundamental package for array computing with Python.</summary><br>Installation:

    python -m pip install numpy --upgrade
<br>
Usage:

    import numpy as np

    data_frame["column1"] = data_frame["column1"].replace(np.nan, "", regex=True)
</details>
<br>

### Other Modules
<details><summary><a href=https://pypi.org/project/pylint/>pylint</a>: a Python static code analysis tool which looks for programming errors, helps enforcing a coding standard, sniffs for code smells and offers simple refactoring suggestions.</summary><br>Installation:

    python -m pip install pylint --upgrade
<br>
Usage (in command line):

    pylint [filename]
</details>


<details><summary><a href=https://pypi.org/project/black/#:~:text=Black%20is%20the%20uncompromising%20Python%20code%20formatter.%20By,time%20and%20mental%20energy%20for%20more%20important%20matters.>black</a>: the uncompromising Python code formatter.</summary><br>Installation:

    python -m pip install black --upgrade
<br>
Usage (in command line):

    black [filename]
</details>
<br>

Note: 
+ To install all modules, please use 
+ In order to keep methods uniform for easy use and application in the future, many, if not all scripts use the same imports. 
+ Import should be added in order they are listed in this doc to follow current Python [PEP8 guidelines](https://pep8.org/) for readiblity and function.
***
### Backups
In every script, there are two places where backups are completed: in the beginning and before the data is uploaded into the database.  

The backup section at the beginning of the script pulls the file created with the previous update from the 'Updates' folder for the respective script that is being run.  If there is new data, after reading and renaming the old file from 'Updates', the file will be moved to the 'Backups' folder.  If there is no new data, no files will be moved or renamed.

<details><summary>Read file from "Updates" folder:</summary><br>Using pandas:

    data_frame_backup = pd.read_csv("./Updates/[filename].txt", sep="\t")
<br>
And save file to Backups folder with '_backup' added at the end:

    data_frame_backup.to_csv("./Backups/[filename]_backup.txt", sep="\t")

</details>

<br>Before the new data is uploaded to the database, the tables in the database need to be prepared.  The table with the same name of the file being updated needs to be renamed with '_BACKUP' tacked on at the end of the table name so that new data can be uploaded with the same table name.  That can be done after the connection has been set created (see `pyodbc` in [Modules](#Modules)).

<details><summary>After creating a connection:</summary><br>We can drop the existing backup table from the database using SQL:
    
    c.execute("drop table [table_name]")
<br>
Then rename the table created in the previous update to add "_BACKUP" at the end: 

    c.execute(
        """sp_rename '[table_name]',
        '[table_name]_BACKUP';""")
</details>
<br>Now the new data can be loaded into the database because the table name is no longer being used.

<br>Note:
+ Notice the 'sep' argument in the pandas. This is used to tell a csv file to use tabs instead of commas to separate data. 
+ Remeber to import the respective [modules](#Modules) before running this code!
</details>

***
### Loading Data
There are multiple soucres for our data.  In each section's ([Demographics](https://github.com/NC-Data-Dashboard/DataDashboard_Greenspan/tree/master/Demographics), [Earnings](https://github.com/NC-Data-Dashboard/DataDashboard_Greenspan/tree/master/Earnings), [Health](https://github.com/NC-Data-Dashboard/DataDashboard_Greenspan/tree/master/Health), [Labor](https://github.com/NC-Data-Dashboard/DataDashboard_Greenspan/tree/master/Labor), [Land](https://github.com/NC-Data-Dashboard/DataDashboard_Greenspan/tree/master/Land), [Natural Products](https://github.com/NC-Data-Dashboard/DataDashboard_Greenspan/tree/master/Natural%20Products)) folder there is a README file that displays where we get our data, providing a link to the source when available.  

Unless the data is in a zipped folder, we load our data using pandas (see `pandas` in [modules](#Modules)).  Most, if not all the data is in [.csv](https://en.wikipedia.org/wiki/Comma-separated_values#:~:text=Rules%20typical%20of%20these%20and%20other%20%22CSV%22%20specifications,at%20a%20line%20terminator.%20...%20More%20items...%20) format.  Instead of navigating to a website, downloading, and then getting the data from somewhere on the computer, the scripts will pull the data directly from the respective website utilizing the download link used to download the data as mentioned previously.

<details><summary>After imports and backups are done:</summary><br>Load our data into a dataframe.  If encoding is not involved, don't add that parameter:

    data_frame = pd.read_csv("[download_link_for_data]", encoding="[type]")

</details>

<br>

Note: 
+ If the data is in a zipped format, view `zipfile` in [modules](#Modules) for code.    

***
### Cleaning Data
Cleaning data is arguably one of the most important steps in analysis and for the NC Data Dashboard it is no different.  Much of the data we collect is on a national level and thus needs to be filtered down to state level, specifically North Carolina.  

After reading the data to a dataframe, we need to examine what makes up the data.  [Jupyter](https://jupyter.org/) notebooks is a great resource hence the 'Notebooks' folders present in the repository. 

<details><summary>To access Jupyter notebooks:</summary><br>After opening command prompt, type:

    cd [path/to/notebooks/folder]
    
<br>
Then:

    jupyter notebook

This should open a new window in the default browser and should display all the contents in whatever directory you navigated to previously.  
</details>

<br>Once in Jupyter, you can access any file in the directory, but notebook files are depicted by the `.ipynb` file extension.  In this file type, code can be run in cells, allowing for easier, more efficient analysis.
<br>

One of the first steps we take is to narrow down the data to only show North Carolina.  
<details><summary>This can be done using filters:</summary><br> Example using .str.contains:

    filter_namme = data_frame["[columnName]"].str.contains(", NC")
    data_frame_nc = data_frame[filter_name]
<br>
Example not using .str.contains:

    filter_name = data_frame["[columnName]"] == "NC"
    data_frame_nc = data_frame[filter_name]
</details>
<br>

Once the data has been filtered to only display data we want, we have to ensure the data types we have are what we want.  

<details><summary>First we have to check what data types we have:</summary><br>To view data types in a data frame:

    data_frame_nc.dtypes
<br></details>
<details><summary>After getting the data types, we can format columns to get the data types we want:</summary><br>Here is an example from the Zhvi script used earlier:

    data_frame_nc.loc[:, "[columnName]"] = data_frame_nc["columnName"].astype(str)

First we have to only edit the column we want and not the entire data set, thus the `.loc`.  The `:` colon skips all the columns before the column we want.  The `.astype()` allows the column to change data type based on the value entered into the `()` parenthesis.  Details on values that can go there can be found [here](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html).
</details><br>

If and/or when the data types are what we want, sometimes we have to add back leading 0's. (This is common in data from Zillow)

<details><summary>To do this:</summary><br>We use .loc again:

    data_frame_nc.loc[:, "[columnName]"] = data_frame_nc["[columnName]"].str.zfill(3)

Again, `.loc` lets us only edit the column we want.  The `.str.zfill()` allows us to fill a string value to any number of digits.  In the case of MunicipalCodeFIPS, the values are strings ranging from 1 digit to 3 digits.  The example forces every value to have 3 digits thus 1 becomes 001.
</details><br>

Once those steps have been completed, the data is almost ready to be loaded into the database, however, it needs to be published into a text file as a step to decrease loss of information in the event that the database crashes. 
<details><summary>Before creating the text file:</summary><br>You might have noticed, if you are displaying the data frame as you work, that there is a column with numbers marking the rows.  Pandas adds this to make the data easier to read, however, we do not want this in our database as it is unneccesary and could cause confusion. To set different columns as our index:

    data_frame_nc.set_index(data_frame_nc["[columnName]"], inplace=True)

Note: 
+ `inplace=True` is important.  Without it, the index will not change.

<br>
After setting the index, you may notice there are now two columns that are the same.  Setting a new index does not remove the column that is being set as the new index so we need to drop the column from it's old location:

    data_frame_nc.drop("[columnName]", axis=1, inplace=True)

Note:
+ `axis=1` tells the code the column name can be found in the top row of the data frame. 
+ Again, `inplace=True` is important as the change will not take place unless it is True.
</details><br>

Note:
+ 'Notebooks' folder has been removed from public repo
+ Testing can be done in the notebooks as they do not change the scripts which are used by the batch file to complete the job
***
### Loading to the Database
If you have been reading this doc in order, now data can be loaded into the database.

 <details><summary>Before we load it to the database, we need to save a text file to the Updates folder:</summary><br>Similar to creating the backup text file:

    data_frame_nc.to_csv("./Updates/[filename].txt", sep="\t")

</details><br>

Once that is complete, we can connect to the database (see `pyodbc` in [modules](#Modules)).  After connection has been created, we can create the backup tables in the database (see second part of [Backups](#Backups)).  Once that is complete, we can use `urllib` and pandas to load data to database:

<details><summary>Using urllib:</summary><br>We have to create parameters for the upload.  This is similar to estaablishing the connection:

    params = urllib.parse.quote_plus(
        r"Driver={SQL Server};"
        r"Server=[server];"
        r"Database=[database];"
        r"Trusted_Connection=yes;"
    )
<br>
Then create the engine based on the parameter we just created:

    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

<br>
Finally, upload to the database:

    data_frame.to_sql(
    "[table]", con=engine, if_exists="replace", index=False
    )

Note:
+ `if_exists="replace"` will replace a table with the name speicified if it exists in the database prior to the upload.

</details>

***
### More Information
To view Python Script Documentation, [click here](https://github.com/NC-Data-Dashboard/DataDashboard_Public/blob/master/PythonScriptsDocumentation.md).
***
Last updated: 07.29.2020
