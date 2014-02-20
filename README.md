leo_manager
===========

[![Build Status](https://secure.travis-ci.org/leo-project/leo_manager.png?branch=master)](http://travis-ci.org/leo-project/leo_manager)

Overview
--------

* "leo_manager" is one of the core component of [LeoFS](https://github.com/leo-project/leofs). Main roles are described below.
  * "leo_manager" distribute routing-table (RING) which is always monitored by it.
  * In order to continue operation of the storage system, "leo_manager" always monitor nodes in the cluster.
  * "leo_manager" provide a centralized control function such as ATTACH, START, SUSPEND and RESUME.
*  Detail document is [here](http://www.leofs.org/docs/).
* "leo_manager" uses [rebar](https://github.com/rebar/rebar) build system. Makefile so that simply running "make" at the top level should work.
* "leo_manager" requires Erlang R15B03-1 or later.
