cd Scripts
#Python -W ignore CAINC5N_NC.py
#Python -W ignore CAINC6N_NC.py
echo Updating Civilian Labor Force...
Python -W ignore GeoFRED_Civilian_Labor_Force_by_County_Persons.py
echo Update complete!

echo Updating Education Status...
Python -W ignore GeoFRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.py
echo Update complete!

echo Updating Resident Population...
Python -W ignore GeoFRED_Resident_Population_by_County_Thousands_of_Persons.py
echo Update complete!

echo Updating Unemployment Rate...
Python -W ignore GeoFRED_Unemployment_Rate_by_County_Percent.py
echo Update complete!

#cd Updates
#git status

#git commit -a -m "Labor Update %Date%"
#git status

#git push

cd ..