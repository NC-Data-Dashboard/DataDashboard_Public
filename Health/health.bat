cd Data
cd covid-19-Data
git fetch
git pull

cd..
cd..

python Scripts\NC_COVID_Cases.py
python Scripts\NC_COVID_Deaths.py
python Scripts\National_COVID_Cases.py
python Scripts\National_COVID_Deaths.py
python Scripts\Publish_Health_Data_Series.py

cd Updates
git status

git commit -a -m "COVID Update %date%-%time%"
git status

git push

cd..
