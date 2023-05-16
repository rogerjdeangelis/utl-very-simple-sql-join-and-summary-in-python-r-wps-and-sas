%let pgm=utl-very-simple-sql-join-and-summary-in-python-r-wps-and-sas;

Very simple sql join and summary in python r wps and sas

  SQL is the most robust solution in SAS WPS  R and Python?

  SOLUTIONS

      1. sas sql
      2. wps sql
      3. wpr r sql
      4. python sql
      5. native python
         SOAPBOX ON
         Unable to create a general solution like sql solution - just works for this special case.
         A lot of issues with native python (wanted to use a join or merge)
         Merge and join produce output which is a series not a dataframe
         Difficult to coerce a series into a panda data frame.
         Even stacking and aggregating causes data structure issues.
         The issue with python is that it does not integrate with other languages,
         however python sqllite fixes most issues.
         R also has this issue but to a lesser extent and sql works better in R.
         SOAPBOX OFF
      6. WPS native R two solutions
         a. Stack and aggregate
         b. dplyr inner join
           https://stackoverflow.com/users/16087142/yomi-blaze93
           https://stackoverflow.com/users/14137004/julian
      7. SAS/WPS no sql datastep merge


github
https://tinyurl.com/s2rjbpny
https://github.com/rogerjdeangelis/utl-very-simple-sql-join-and-summary-in-python-r-wps-and-sas

StackOverflow R
https://tinyurl.com/2vzvd6wu
https://stackoverflow.com/questions/76253633/merging-two-datasets-summing-a-shared-column

proc datasets lib=work kill nodetails nolist;
run;quit;

options validvarname=upcase;
libname sd1 "d:/sd1";

data havRyt sd1.havRyt;
  length fro too $1;
  input fro$ too$ cnt;
cards4;
a a 2
a b 3
;;;;
run;quit;

data havLft sd1.havLft;
  length fro too $1;
  input fro$ too$ cnt;
cards4;
a a 3
a b 4
;;;;
run;quit;

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

/**************************************************************************************************************************/
/*                                 |                      |  RULES  Add HavLft.CNT to HavRyt.Cnt                          */
/*                                 |                      |                                                               */
/*          HAVRYT                 |    WORK.HAVLFT       |         WANT                                                  */
/*                                 |                      |                                                               */
/*  Obs    FRO    TOO    CNT       |   FRO    TOO    CNT  |    FRO    TOO    CNT                                          */
/*                                 |                      |                                                               */
/*   1      a      a      2        |    a      a      3   |     a      a      5    2+3                                    */
/*   2      a      b      3        |    a      b      4   |     a      b      7    3+4                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                               _
/ |    ___  __ _ ___   ___  __ _| |
| |   / __|/ _` / __| / __|/ _` | |
| |_  \__ \ (_| \__ \ \__ \ (_| | |
|_(_) |___/\__,_|___/ |___/\__, |_|
                              |_|
*/

proc sql;
  select
    l.fro
   ,l.too
   ,l.cnt + r.cnt as cnt
  from
    havRyt as l, havLft as r
  where
        l.fro = r.fro
    and l.too = r.too
;quit;

/*___                                     _
|___ \    __      ___ __  ___   ___  __ _| |
  __) |   \ \ /\ / / `_ \/ __| / __|/ _` | |
 / __/ _   \ V  V /| |_) \__ \ \__ \ (_| | |
|_____(_)   \_/\_/ | .__/|___/ |___/\__, |_|
                   |_|                 |_|
*/

%let _pth=%sysfunc(pathname(work));

%utl_submit_wps64('

libname wrk "&_pth";

options validvarname=any;
proc sql;
  select
    l.fro
   ,l.too
   ,l.cnt + r.cnt as cnt
  from
    wrk.havRyt as l, wrk.havLft as r
  where
        l.fro = r.fro
    and l.too = r.too
;quit;

');

/*____                                          _
|___ /   __      ___ __  ___   _ __   ___  __ _| |
  |_ \   \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
 ___) |   \ V  V /| |_) \__ \ | |    \__ \ (_| | |
|____(_)   \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                  |_|                        |_|

%let _pth=%sysfunc(pathname(work));

%utl_submit_wps64('

libname wrk "&_pth";
options validvarname=any;

proc r;

export data=wrk.havlft r=havlft;
export data=wrk.havryt r=havryt;

submit;
library(sqldf);
want<-sqldf("
  select
    l.fro
   ,l.too
   ,l.cnt + r.cnt as cnt
  from
    havryt as l, havlft as r
  where
        l.fro = r.fro
    and l.too = r.too
");
endsubmit;
import data=wrk.want_r_sql r=want;
run;quit;
');

proc print data=want_r_sql;
run;quit;

/*  _                  _   _                             _
| || |     _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| || |_   | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
|__   _|  | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
   |_|(_) | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
          |_|    |___/                                |_|
*/

options validvarname=any;
proc datasets lib=work nodetails nolist;
 delete res;
run;quit;

%utlfkil(d:/xpt/res.xpt);

%utl_pybegin;
parmcards4;
from os import path
import pandas as pd
import xport
import xport.v56
import pyreadstat
import numpy as np
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
havlft, meta = pyreadstat.read_sas7bdat("d:/sd1/havlft.sas7bdat")
havryt, meta = pyreadstat.read_sas7bdat("d:/sd1/havryt.sas7bdat")
print(havlft);
print(havryt);
res = pdsql("""
  select
    l.fro
   ,l.too
   ,l.cnt + r.cnt as cnt
  from
    havryt as l inner join havlft as r
  where
        l.fro = r.fro
    and l.too = r.too
""")
print(res);
ds = xport.Dataset(res, name='res')
with open('d:/xpt/res.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
;;;;
%utl_pyend;

libname pyxpt xport "d:/xpt/res.xpt";

proc contents data=pyxpt._all_;
run;quit;

proc print data=pyxpt.res;
run;quit;

data res;
   set pyxpt.res;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  Up to 40 obs from RES total obs=2 16MAY2023:08:39:49                                                                  */
/*                                                                                                                        */
/*  Obs    FRO    TOO    CNT                                                                                              */
/*                                                                                                                        */
/*   1      a      a      5                                                                                               */
/*   2      a      b      7                                                                                               */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                 _   _                         _   _
| ___|    _ __   __ _| |_(_)_   _____   _ __  _   _| |_| |__   ___  _ __
|___ \   | `_ \ / _` | __| \ \ / / _ \ | `_ \| | | | __| `_ \ / _ \| `_ \
 ___) |  | | | | (_| | |_| |\   /  __/ | |_) | |_| | |_| | | | (_) | | | |
|____(_) |_| |_|\__,_|\__|_| \_/ \___| | .__/ \__, |\__|_| |_|\___/|_| |_|
                                       |_|    |___/
*/

/*----
  USE SQLLITE3 INSTEAD
  when forcing the pd.merge series to a panda dataframe we loose the keys
  this solution only work for one copy of keys in both datasets
  cheated and grabbed the keys from the javLft
----*/

options validvarname=any;
proc datasets lib=work nodetails nolist;
 delete want_py;
run;quit;

%let _pth=%sysfunc(pathname(work));

%utl_submit_wps64("

libname wrk '&_pth';
options validvarname=any;

proc python;

export data=wrk.havlft python=havlft;
export data=wrk.havryt python=havryt;

submit;

import pandas as pd;
want_py = pd.DataFrame(pd.merge(havlft, havryt, on=['FRO', 'TOO']).sum(axis=1));
havlft.drop('CNT', axis=1, inplace=True);
want_py = pd.concat([havlft, want_py], axis=1);
print(want_py);

endsubmit;
import data=wrk.want_py python=want_py;
run;quit;
");

options validvarname=any;
proc print data=want_py;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* up to 40 obs from WANT_PY total obs=2 16MAY2023:08:26:31                                                               */
/*                                                                                                                        */
/* Obs   FRO    TOO    0 ++> odd                                                                                          */
/*                                                                                                                        */
/* 1      a      a     5                                                                                                  */
/* 2      a      b     7                                                                                                  */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                                _   _
__      ___ __  ___   _ __   __ _| |_(_)_   _____   _ __
\ \ /\ / / `_ \/ __| | `_ \ / _` | __| \ \ / / _ \ | `__|
 \ V  V /| |_) \__ \ | | | | (_| | |_| |\ V /  __/ | |
  \_/\_/ | .__/|___/ |_| |_|\__,_|\__|_| \_/ \___| |_|
         |_|
*/

proc datasets lib=work nodetails nolist;
 delete want_inr want;
run;quit;

%let _pth=%sysfunc(pathname(work));
libname wrk "&_pth";

%utl_submit_wps64("

libname wrk '&_pth';
options validvarname=any;

proc r;

export data=wrk.havlft r=havlft;
export data=wrk.havryt r=havryt;

submit;
library(dplyr);

want <- bind_rows(havlft, havryt) %>%
  group_by(FRO, TOO) %>%
  summarise(CNT = sum(CNT));

print(want);

want_inr<-inner_join(havlft, havryt, by = c('FRO','TOO')) %>%
  mutate(CNT = CNT.x + CNT.y);
want_inr = want_inr[c('FRO','TOO','CNT')];

print(want_inr);
endsubmit;

import data=wrk.want     r=want;
import data=wrk.want_inr r=want_inr;

run;quit;
");

proc print data=want;
title "Stack and aggregate";
run;quit;

proc print data=want_inr;
title "Non SQL inner join";
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  Stack and aggregate                                                                                                   */
/*                                                                                                                        */
/*  Obs    FRO    TOO    CNT                                                                                              */
/*                                                                                                                        */
/*   1      a      a      5                                                                                               */
/*   2      a      b      7                                                                                               */
/*                                                                                                                        */
/*  Non SQL inner join                                                                                                    */
/*                                                                                                                        */
/*  Obs    FRO    TOO    CNT                                                                                              */
/*                                                                                                                        */
/*   1      a      a      5                                                                                               */
/*   2      a      b      7                                                                                               */
/*                                                                                                                        */
/**************************************************************************************************************************/

 /*               __                                             _
 ___  __ _ ___   / /_      ___ __  ___   _ __   ___    ___  __ _| |
/ __|/ _` / __| / /\ \ /\ / / `_ \/ __| | `_ \ / _ \  / __|/ _` | |
\__ \ (_| \__ \/ /  \ V  V /| |_) \__ \ | | | | (_) | \__ \ (_| | |
|___/\__,_|___/_/    \_/\_/ | .__/|___/ |_| |_|\___/  |___/\__, |_|
                            |_|                               |_|
*/

%let _pth=%sysfunc(pathname(work));

%utl_submit_wps64('

libname wrk "&_pth";

data wrk.want_nosql(drop=cntRyt);
  merge wrk.havLft wrk.havRyt(rename=cnt=cntRyt);
  by fro too;
  cnt = cnt + cntRyt;
run;quit;

');

proc print data=want_nosql;
run;quit;

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
