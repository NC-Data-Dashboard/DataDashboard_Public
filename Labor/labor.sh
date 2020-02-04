RM Python -W ignore Scripts\CAINC5N_NC.py
RM Python -W ignore Scripts\CAINC6N_NC.py
echo Updating Civilian Labor Force...
Python -W ignore Scripts\GeoFRED_Civilian_Labor_Force_by_County_Persons.py
echo Update complete!

echo Updating Education Status...
Python -W ignore Scripts\GeoFRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.py
echo Update complete!

echo Updating Resident Population...
Python -W ignore Scripts\GeoFRED_Resident_Population_by_County_Thousands_of_Persons.py
echo Update complete!

echo Updating Unemployment Rate...
Python -W ignore Scripts\GeoFRED_Unemployment_Rate_by_County_Percent.py
echo Update complete!

RM cd Updates
RM git status

RM git commit -a -m "Labor Update %Date%"
RM git status

RM git push

RM cd..