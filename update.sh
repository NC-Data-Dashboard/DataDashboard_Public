RM git config --global user.name "NCDataDashboard"

cd Land
bash land.bat
cd ..

cd Labor
bash labor.bat
cd ..

cd Earnings
bash earnings.bat
cd ..

cd Demographics
bash Demographics.bat
cd ..

RM cd Health
RM bash health.bat
RM cd ..

RM cd Natural Products
RM bash natproducts.bat
RM cd ..

RM git status

exit
