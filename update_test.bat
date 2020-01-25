python -m pip install --upgrade pip --user
python -m pip install jupyter --upgrade --user
python -m pip install jupyter lab --upgrade --user 
python -m pip install pandas --upgrade --user
python -m pip install tensorflow --upgrade --user 
python -m pip install ZipFile --upgrade --user
python -m pip install Requests --upgrade -user 
python -m pip install watermark --upgrade --user
python -m pip install xlrd --upgrade --user

REM git config --global user.name "NCDataDashboard"

cd Land
call land.bat
cd..

cd Labor
call labor.bat
cd..

cd Earnings
call earnings.bat
cd..

REM cd Demographics
REM start Demographics.bat
REM cd..

REM cd Health
REM start health.bat
REM cd..

REM cd Natural Products
REM start natproducts.bat
REM cd..

REM git status

exit
