RELEASABLE:=false
DEPS:=rabbitmq-erlang-client
STANDALONE_TEST_COMMANDS:=rabbitmq_ha_test_tests:run()
ERL_OPTS=-sname hat