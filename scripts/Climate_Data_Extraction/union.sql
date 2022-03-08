Create table tmaxunion as (Select  * from tmax1981
UNION ALL
Select * from tmax1982
UNION ALL
Select * from tmax1983
UNION ALL
Select * from tmax1984
UNION ALL
Select * from tmax1985
UNION ALL
Select * from tmax1986
UNION ALL
Select * from tmax1987
UNION ALL
Select * from tmax1988
UNION ALL
Select * from tmax1989
UNION ALL
Select * from tmax1990
UNION ALL
Select * from tmax1991
UNION ALL
Select * from tmax1992
UNION ALL
Select * from tmax1993
UNION ALL
Select * from tmax1994
UNION ALL
Select * from tmax1995
UNION ALL
Select * from tmax1996
UNION ALL
Select * from tmax1997
UNION ALL
Select * from tmax1998
UNION ALL
Select * from tmax1999
UNION ALL
Select * from tmax2000
UNION ALL
Select * from tmax2001
UNION ALL
Select * from tmax2002
UNION ALL
Select * from tmax2003
UNION ALL
Select * from tmax2004
UNION ALL
Select * from tmax2005
UNION ALL
Select * from tmax2006
UNION ALL
Select * from tmax2007
UNION ALL
Select * from tmax2008
UNION ALL
Select * from tmax2009
UNION ALL
Select * from tmax2010
UNION ALL
Select * from tmax2011
UNION ALL
Select * from tmax2012
UNION ALL
Select * from tmax2013
UNION ALL
Select * from tmax2014
UNION ALL
Select * from tmax2015
UNION ALL
Select * from tmax2016
UNION ALL
Select * from tmax2017
UNION ALL
Select * from tmax2018
UNION ALL
Select * from tmax2019
);
Alter table tmaxunion add column filedate date;
Update tmaxunion set filedate=TO_DATE(substring(filename,19,8),'YYYYMMDD');
