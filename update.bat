python -m pip install --upgrade pip --user
python -m pip install jupyter --upgrade --user
python -m pip install pandas --upgrade --user
python -m pip install Requests --upgrade --user
python -m pip install watermark --upgrade --user
python -m pip install xlrd --upgrade --user
python -m pip install pyodbc --user
python -m pip install sqlalchemy --upgrade --user
python -m pip install numpy --upgrade --user

:start

git config --global user.name "NCDataDashboard"

cd Land
call land.bat
cd..

cd Labor
call labor.bat
cd..

cd Earnings
call earnings.bat
cd..

cd Demographics
call Demographics.bat
cd..

REM cd Health
REM start health.bat
REM cd..

REM cd Natural Products
REM start natproducts.bat
REM cd..

git status

goto start

pause