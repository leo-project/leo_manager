leo_manager
===========

Overview
--------

* "leo_manager" is one of the core component of [LeoFS](https://github.com/leo-project/leofs). Main roles are described below.
  * "leo_manager" distribute routing-table (RING) which is always monitored by it.
  * In order to continue operation of the storage system, "leo_manager" always monitor nodes in the cluster.
  * "leo_manager" provide a centralized control function such as ATTACH, START, SUSPEND and RESUME.
*  Detail document is [here](http://www.leofs.org/docs/).

* "leo_manager" uses the "rebar" build system. Makefile so that simply running "make" at the top level should work.
  * [rebar](https://github.com/basho/rebar)
* "leo_manager" requires Erlang R14B04 or later.
