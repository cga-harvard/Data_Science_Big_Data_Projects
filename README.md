### Storage space on HPC

- CGA has a lab share on one of FASRC's lustre file systems:

/n/holylfs/LABS/cga

with a link at /n/cga

/n/cga -> /n/holylfs/LABS/cga

The quota per your group is set to 4T which is their initial allocation.

$> lfs quota -g cga -h /n/holylfs

Disk quotas for grp cga (gid 5103):

Filesystem used quota limit grace files quota limit grace

/n/holylfs 4k 4T 4T - 1 0 0 -


- Additional storage can be purchased at 50$/T/year.

- Also, we have a 50T allocation on their global scratch filesystem, to be used
not to store data long term, but as scratch space for our computation. Please
note that they purge data which is older than 90 days on that filesystem. 

$> ls -ld /n/scratchlfs/cga

drwxrws---+ 2 root cga 4096 Sep 17 13:46 /n/scratchlfs/cga

$> lfs quota -g cga -h /n/scratchlfs

Disk quotas for grp cga (gid 5103):

Filesystem used quota limit grace files quota limit grace

/n/scratchlfs 8k 50T 50T - 2 0 0 -

Please note that they have two storage pools for that scratch filesystem , one
that is backed by nlsas and one by ssd.

/n/scratchlfs/cga and /n/scratchssdlfs/cga

$> lfs getstripe /n/scratchssdlfs/cga/

/n/scratchssdlfs/cga/

stripe_count: 1 stripe_size: 1048576 stripe_offset: -1 pool: ssd

$> lfs getstripe /n/scratchlfs/cga/

/n/scratchlfs/cga/

stripe_count: 1 stripe_size: 1048576 stripe_offset: -1 pool: nlsas


The quota is 50T cumulative across the two pools. 

Refer to Overview of storage options on FASRC for details: https://www.rc.fas.harvard.edu/resources/cluster-storage/


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
