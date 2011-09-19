RELEASABLE:=true
DEPS:=rabbitmq-erlang-client erlando
STANDALONE_TEST_COMMANDS:=rabbitmq_ha_test_tests:test()
ERL_OPTS=-sname hat
