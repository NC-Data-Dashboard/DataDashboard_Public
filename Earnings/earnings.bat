Python -W ignore Scripts/CAINC5N_NC.py
python -W ignore Scripts/NCDOR_MSALESUSETAX_0001.py
python -W ignore Scripts/NCDOR_MSALESUSETAX_0002.py

cd Updates
git status

git commit -a -m "Earnings Update %Date%"
git status

git push

cd..