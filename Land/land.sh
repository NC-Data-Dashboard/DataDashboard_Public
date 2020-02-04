echo Updating Median Listing Price...
python -W ignore Scripts\County_MedianListingPrice_AllHomes.py
echo Update complete!

echo Updating Median Listing Price Per Sqft...
python -W ignore Scripts\County_MedianListingPricePerSqft_AllHomes.py
echo Update complete!

echo Updating Median Value Per Sqft...
python -W ignore Scripts\County_MedianValuePerSqft_AllHomes.py
echo Update complete!

echo Updating ZHVI...
python -W ignore Scripts\County_Zhvi_AllHomes.py
echo Update complete!

echo Updating House Price Index...
python -W ignore Scripts\GeoFRED_All_Transactions_House_Price_Index.py
echo Update complete!

echo Updating Homeownership Rate...
python -W ignore Scripts\GeoFRED_Homeownership_Rate_by_County.py
echo Update complete!

echo Updating Private Housing Structures...
python -W ignore Scripts\GeoFRED_New_Private_Housing_Structures.py
echo Update complete!

RM cd Updates
RM git status

RM git commit -a -m "Land Update %date%"
RM git status

RM git push

RM cd..


