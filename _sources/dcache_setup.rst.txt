dCache setup with CTA
=====================


Pool
----

Deploy dCache-CTA driver on pool node::

 wget https://download.dcache.org/nexus/repository/dcache-cta/dcache-cta-0.8.0-1.noarch.rpm
 rpm -Uvh --force dcache-cta-0.8.0-1.noarch.rpm


Define hsm on pool::

 hsm create cta cta dcache-cta -cta-user=adm -cta-group=eosusers -cta-instance-name=eosdev -cta-frontend-addr=ctahost:17017 -io-port=1094

Each pool on the pool node has to have dedicated port.

Define queue on pool::

 queue define class cta * -pending=100 -total=1 -expire=7200 -open=true

CTA
---

On CTA end define storage class and archive route::

 cta-admin sc add -n test.cta@cta -c 1 --vo vo -m dcachetest
 cta-admin ar add -s test.cta@cta -c 1 -t ctasystest -m dcachetest

PoolManager
-----------

In PoolManager define example dedicated CTA pool group::

 psu create unit -store test.cta@cta
 psu create ugroup CtaSelGrp
 psu addto ugroup CtaSelGrp test.cta@cta

 psu create pgroup CtaPoolGroup
 psu addto pgroup CtaPoolGroup rw-stkendca28a-1

 psu create link CtaLink CtaSelGrp any-protocol world-net
 psu  set link  CtaLink -readpref=10 -writepref=10 -cachepref=10 -section=default
 psu addto link CtaLink CtaPoolGroup
