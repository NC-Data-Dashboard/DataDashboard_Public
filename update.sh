# git config --global user.name "NCDataDashboard"

echo Updating Land:
cd Land
bash land.sh
cd ..
clear
echo Land update complete.
echo Updating Labor:
cd Labor
bash labor.sh
cd ..
clear
echo Land and Labor updates complete.
echo Updating Earnings...
cd Earnings
bash earnings.sh
cd ..
clear
echo Land, Labor, and Earnings updates complete.
echo Updating Demographics...
cd Demographics
bash Demographics.sh
cd ..
echo Land, Labor, Earnings, and Demographics updates complete.

# cd Health
# bash health.sh
# cd ..

# cd Natural Products
# bash natproducts.sh
# cd ..

# git status

sleep 5
exit
