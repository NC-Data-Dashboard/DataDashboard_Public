python -m pip install --upgrade pip --user
pip install jupyter --upgrade --user
pip install jupyter lab --upgrade --user
pip install pandas --upgrade --user
pip install Requests --upgrade --user
pip install xlrd --upgrade --user
pip install pyodbc --upgrade --user
pip install sqlalchemy --upgrade --user
pip install numpy --upgrade --user

git config --global push.default matching

cls 

call DataUpdate.py

git status
git add *
git commit -a -m "NC Data Dashboard Data Update %date%"
git push
git status
pause