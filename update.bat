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

REM cd Demographics
REM start Demographics.bat
REM cd..

REM cd Health
REM start health.bat
REM cd..

REM cd Natural Products
REM start natproducts.bat
REM cd..

git status

exit
