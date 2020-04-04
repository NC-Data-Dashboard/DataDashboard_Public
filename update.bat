python -m pip install --upgrade pip --user
python -m pip install jupyter --upgrade --user
python -m pip install pandas --upgrade --user
python -m pip install Requests --upgrade --user
python -m pip install watermark --upgrade --user
python -m pip install xlrd --upgrade --user
python -m pip install pyodbc --user
python -m pip install sqlalchemy --upgrade --user
python -m pip install numpy --upgrade --user

git config --global user.name "NCDataDashboard"
git config --global user.email "nayoung1@catamount.wcu.edu"

cls

cd Land
call land.bat
cd..

cls

cd Labor
call labor.bat
cd..

cls

cd Earnings
call earnings.bat
cd..

cls

cd Demographics
call Demographics.bat
cd..

cls

REM cd Health
REM start health.bat
REM cd..

cls

REM cd Natural Products
REM start natproducts.bat
REM cd..

cls

git status

pause