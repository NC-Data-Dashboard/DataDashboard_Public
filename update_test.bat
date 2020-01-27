python -m pip install --upgrade pip --user
python -m pip install jupyter --upgrade --user
python -m pip install pandas --upgrade --user
python -m pip install Requests --upgrade --user
python -m pip install watermark --upgrade --user
python -m pip install xlrd --upgrade --user
python -m pip install pyodbc --upgrade --user

REM git config --global user.name "NCDataDashboard"

cd Land
call land_test.bat
cd..

cd Labor
call labor_test.bat
cd..

cd Earnings
call earnings_test.bat
cd..

cd Demographics
call Demographics_test.bat
cd..

REM cd Health
REM start health_test.bat
REM cd..

REM cd Natural Products
REM start natproducts_test.bat
REM cd..

REM git status

pause
exit
