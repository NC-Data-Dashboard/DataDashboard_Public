cd Scripts

echo Updating Median Listing Price...
python -W ignore County_MedianListingPrice_AllHomes.py
echo Update complete!

echo Updating Median Listing Price Per Sqft...
python -W ignore County_MedianListingPricePerSqft_AllHomes.py
echo Update complete!

echo Updating Median Value Per Sqft...
python -W ignore County_MedianValuePerSqft_AllHomes.py
echo Update complete!

echo Updating ZHVI...
python -W ignore County_Zhvi_AllHomes.py
echo Update complete!

echo Updating House Price Index...
python -W ignore GeoFRED_All_Transactions_House_Price_Index.py
echo Update complete!

echo Updating Homeownership Rate...
python -W ignore GeoFRED_Homeownership_Rate_by_County.py
echo Update complete!

echo Updating Private Housing Structures...
python -W ignore GeoFRED_New_Private_Housing_Structures.py
echo Update complete!

#cd Updates
#git status

#git commit -a -m "Land Update %date%"
#git status

#git push

cd ..