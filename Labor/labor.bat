Python -W ignore Scripts\CAINC5N_NC.py
Python -W ignore Scripts\CAINC6N_NC.py
Python -W ignore Scripts\GeoFRED_Civilian_Labor_Force_by_County_Persons.py
Python -W ignore Scripts\GeoFRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.py
Python -W ignore Scripts\GeoFRED_Resident_Population_by_County_Thousands_of_Persons.py
Python -W ignore Scripts\GeoFRED_Unemployment_Rate_by_County_Percent.py
Python -W ignore Scripts\Publish_Labor_Data_Series.py

cd Updates
git status

git commit -a -m "Labor Update %Date%"
git status

git push

cd..