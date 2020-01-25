python -m pip install --upgrade pip --user
pip install jupyter --upgrade
pip install jupyter lab --upgrade
pip install pandas --upgrade
pip install tensorflow --upgrade
pip install ZipFile --upgrade
pip install Requests --upgrade
pip install watermark --upgrade
pip install xlrd --upgrade

git config --global user.name "nathayoung"

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
