### Storage space on HPC

- CGA has a lab share on one of FASRC's lustre file systems:
/n/holylfs/LABS/cga
with a link at /n/cga
/n/cga -> /n/holylfs/LABS/cga
The quota per your group is set to 4T which is their initial allocation.

- Additional storage can be purchased at 50$/T/year

- Also, we have a 50T allocation on their global scratch filesystem, to be used
not to store data long term, but as scratch space for our computation. Please
note that they purge data which is older than 90 days on that filesystem. Please note that they have two storage pools for that scratch filesystem , one that is backed by nlsas and one by ssd:
/n/scratchlfs/cga and /n/scratchssdlfs/cga

Refer to overview of storage options on FASRC for details: https://www.rc.fas.harvard.edu/resources/cluster-storage/


### New CGA Accounts on HPC

- Go to https://portal.rc.fas.harvard.edu/request/account/new  
and request your account specifying Wendy as PI (see image below)
- Refer to below to get started:
  - Access and login on FASRC: https://www.rc.fas.harvard.edu/resources/access-and-login/
  - Quickstart guide on FASRC: https://www.rc.fas.harvard.edu/resources/quickstart-guide/






![image](https://github.com/cga-harvard/GIS_Apps_on_HPC/blob/master/cga_account_request.png)



### Merged Geotweet Storage

Please follow the instruction here to upload merged geotweets to Harvard HPC:

- Login to FASRC using the login: ssh user@login.rc.fas.harvard.edu
- Go to CGA's storage space: cd  /n/cga
- Go to merged geotweets stroage folder: cd  /n/cga/geotweets_merged
- Store the data here according to most appropriate structure you seem fit

### References:

- Job submission FASRC: https://www.rc.fas.harvard.edu/resources/running-jobs/
- Overview of storage options on FASRC: https://www.rc.fas.harvard.edu/resources/cluster-storage/
