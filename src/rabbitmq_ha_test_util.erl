-module(rabbitmq_ha_test_util).

-export([rabbitmqctl/3,rabbitmqctl/4, rabbitmqctl/5]).

rabbitmqctl(RabbitDir, Node, Command) ->
    rabbitmqctl(RabbitDir, Node, Command, false).
rabbitmqctl(RabbitDir, Node, Command, Quiet) ->
    rabbitmqctl(RabbitDir, Node, Command, Quiet, []).
rabbitmqctl(RabbitDir, Node, Command, Quiet, Args) ->
    QuietFlag = case Quiet of
                    true -> "-q";
                    false -> ""
                end,

    os:cmd(RabbitDir ++ "/scripts/rabbitmqctl " ++ QuietFlag
           ++ " -n " ++ atom_to_list(Node) ++ " " ++ Command ++ " "
           ++ lists:flatten([Arg ++ " " || Arg <- Args])).
