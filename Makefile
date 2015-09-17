PROJECT = riak_core

COMPILE_FIRST = riak_core_broadcast_handler riak_core_gen_server

DEPS = lager poolboy basho_stats riak_sysmon riak_ensemble pbkdf2 eleveldb exometer_core clique folsom

dep_lager= git git://github.com/basho/lager.git 2.2.0
dep_poolboy= git git://github.com/basho/poolboy.git 0.8.1p3
dep_basho_stats= git git://github.com/basho/basho_stats.git 1.0.3
dep_riak_sysmon= git git://github.com/basho/riak_sysmon.git th/correct-dependencies
dep_riak_ensemble= git git://github.com/basho/riak_ensemble.git th/dynamic-start
dep_pbkdf2= git git://github.com/basho/erlang-pbkdf2.git 2.0.0
dep_eleveldb= git git://github.com/basho/eleveldb.git th/correct-dependencies
dep_exometer_core= git git://github.com/basho/exometer_core.git th/correct-dependencies
dep_clique= git git://github.com/basho/clique.git RIAK-2125/correct-dependencies
dep_folsom= git git://github.com/basho/folsom.git 0.7.4p5


include erlang.mk

#COMPILE_FIRST += gen_nb_server.erl riak_core_gen_server.erl riak_core_stat_xform

ERLC_OPTS := $(filter-out -Werror,$(ERLC_OPTS))
