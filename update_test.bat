pip install --upgrade pip --user
pip install jupyter --upgrade --user
pip install pandas --upgrade --user
pip install Requests --upgrade --user
pip install watermark --upgrade --user
pip install xlrd --upgrade --user
pip install pyodbc --upgrade --user
pip install sqlalchemy --upgrade --user
pip install numpy --upgrade --user

REM git config --global user.name "NCDataDashboard"

cd Land
call land_test.bat
cd..

cd Labor
call labor.bat
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
