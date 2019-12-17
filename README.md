# Welcome to the NC Data Dashboard!
This repository contains work student employees created as it relates to Data Analyst roles at Western Carolina University.
__________________________________________________________________________________________________________________________________________
### [Demographics](https://github.com/nathayoung/NCDataDashboard/tree/master/Demographics)
The Demographics folder includes scripts used to update data from [GeoFRED](https://geofred.stlouisfed.org/map/) for the Demographics workbook.

### [Earnings](https://github.com/nathayoung/NCDataDashboard/tree/master/Earnings)
The Earnings folder includes scripts that are used to pull data from [BEA](https://apps.bea.gov/regional/downloadzip.cfm) to update our Earnings Workbook.

### [Labor](https://github.com/nathayoung/NCDataDashboard/tree/master/Labor)
The Labor folder includes scripts used to update data from [GeoFRED](https://geofred.stlouisfed.org/map/) and [BEA](https://apps.bea.gov/regional/downloadzip.cfm) for the Labor workbook.

### [Land](https://github.com/nathayoung/NCDataDashboard/tree/master/Land)
The Land folder contains Python scripts that are used to pull data from [Zillow](https://www.zillow.com/research/data/) and [GeoFRED](https://geofred.stlouisfed.org/map/) to update our Land Workbook.


** Workbooks are available to view and download at the [NC Data Dashboard site](https://www.wcu.edu/engage/regional-development/data-dashboard.aspx). **
******************************************************************************************************************************************

### Folder Breakdown
Each folder will have the following:
* Backups
  * Here the data from the previous update will be saved until the next update.
* Notebooks
  * The Jupyter Notebooks upon which the Python scripts are based are located here. 
    * Note: They may not match their respective Python script.  If that is the case, please let me know so I can fix it!
* Scripts
  * Python Scripts that the .bat file runs are located here.
* Updates
  * The data that was pulled from the latest update will be here, waiting to be moved to the Dashboard.
  
All folders contain or will contain Windows Batch Files (.bat) to simplify and expedite update process. The update.bat file will run each categories .bat file at the same time, providing a one stop place to update data.
 
  
##### Last updated: 12.17.2019
