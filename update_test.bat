python -m pip install --upgrade pip --user
pip install jupyter --upgrade
pip install jupyter lab --upgrade
pip install pandas --upgrade
pip install tensorflow --upgrade
pip install Requests --upgrade
pip install watermark --upgrade
pip install xlrd --upgrade

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

REM cd Demographics
REM start Demographics_test.bat
REM cd..

REM cd Health
REM start health_test.bat
REM cd..

REM cd Natural Products
REM start natproducts_test.bat
REM cd..

REM git status

pause
exit
