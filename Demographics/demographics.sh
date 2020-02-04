cd Scripts
echo Updating Civilian Labor Force...
Python -W ignore GeoFRED_Civilian_Labor_Force_by_County_Persons.py
echo Update complete!

echo Updating EQFXSUBPRIME...
Python -W ignore GeoFRED_EQFXSUBPRIME.py
echo Update complete!

echo Updating Educated Peoples...
Python -W ignore GeoFRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.py
echo Update complete!

echo Updating Resident Population...
Python -W ignore GeoFRED_Resident_Population_by_County_Thousands_of_Persons.py
echo Update complete!

#cd Updates
#git status

#git commit -m "Demographics Update date +"%D""
#git status

#git push
cd ..