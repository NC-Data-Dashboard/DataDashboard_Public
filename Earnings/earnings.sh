echo Updating CAINC5N
Python -W ignore Scripts/CAINC5N_NC.py
echo Update complete!

RM cd Updates
RM git status

RM it commit -a -m "Earnings Update" date +"%m-%d-%Y"
RM git status

RM git push

RM cd ..