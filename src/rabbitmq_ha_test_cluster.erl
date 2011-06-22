%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbitmq_ha_test_cluster).

-export([start/1, stop/1]).
-export([kill_node/1]).

-include("rabbitmq_ha_test_cluster.hrl").

-define(PING_WAIT_INTERVAL, 1000).
-define(PING_MAX_COUNT, 3).
-define(RABBITMQ_SERVER_DIR, "../rabbitmq-server").

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start(NodeSpecs) ->
    #cluster{nodes =
                 lists:reverse(
                   lists:foldl(
                     fun(Spec, AccN) ->
                             Node = start_node(Spec),
                             add_node_to_cluster(Spec, AccN),
                             [Node | AccN]
                     end, [], NodeSpecs))}.

stop(#cluster{nodes = Nodes}) ->
    [stop_node(Node) || Node <- Nodes],
    ok.

kill_node(#node{pid = Pid}) ->
    os:cmd("kill -9 " ++ integer_to_list(Pid)).

%%------------------------------------------------------------------------------
%% Node Interaction
%%------------------------------------------------------------------------------
start_node({Name, Port}) ->
    ErlPort = open_port({spawn, start_command(Name, Port)}, []),
    {ok, Name} = wait_for_node_start(Name),
    #node{name = Name, port = Port, erl_port = ErlPort,
          pid = find_os_pid(Name)}.

add_node_to_cluster(_Spec, []) ->
    ok;
add_node_to_cluster({Name, _Port}, [#node{name = Master} | _]) ->
    rabbitmqctl(Name, "stop_app"),
    rabbitmqctl(Name, "reset"),
    rabbitmqctl(Name, "cluster", false, [atom_to_list(Master),
                                         atom_to_list(Name)]),
    rabbitmqctl(Name, "start_app"),
    rabbitmqctl(Name, "wait"),
    ok.

stop_node(#node{name = Name}) ->
    rabbitmqctl(Name, "stop"),
    {ok, Name} = wait_for_node_stop(Name),
    ok.

%%------------------------------------------------------------------------------
%% Commands and rabbitmqctl
%%------------------------------------------------------------------------------

start_command(Name, Port) ->
    Headless = true,
    {Prefix, Suffix} = case Headless of
                           true -> {"", ""};
                           false -> {"xterm -e \"", "\""}
                       end,

    Prefix ++ "make RABBITMQ_NODENAME='" ++ atom_to_list(Name) ++
        "' RABBITMQ_NODE_PORT=" ++ integer_to_list(Port) ++
        " -C " ++ ?RABBITMQ_SERVER_DIR ++ " cleandb run" ++ Suffix.

rabbitmqctl(Name, Command) ->
    rabbitmq_ha_test_util:rabbitmqctl(?RABBITMQ_SERVER_DIR, Name, Command).

rabbitmqctl(Name, Command, Quiet) ->
    rabbitmq_ha_test_util:rabbitmqctl(?RABBITMQ_SERVER_DIR, Name,
                                      Command, Quiet).

rabbitmqctl(Name, Command, Quiet, Args) ->
    rabbitmq_ha_test_util:rabbitmqctl(?RABBITMQ_SERVER_DIR, Name,
                                      Command, Quiet, Args).

%%------------------------------------------------------------------------------
%% Util
%%------------------------------------------------------------------------------

find_os_pid(Node) ->
    Status = rabbitmqctl(Node, "status", true),
    {ok, Scanned, _} = erl_scan:string(Status ++ "."),
    {ok, Term} = erl_parse:parse_term(Scanned),
    proplists:get_value(pid, Term).

wait_for_node_start(NodeName) ->
    rabbitmqctl(NodeName, "wait"),
    {ok, NodeName}.

wait_for_node_stop(NodeName) ->
    wait_for_node_state(NodeName, pang, ?PING_MAX_COUNT, node_not_stopped).

wait_for_node_state(NodeName, _StateMsg, 0, Error) ->
    {error, {Error, NodeName}};
wait_for_node_state(NodeName, StateMsg, PingCount, Error) ->
    case net_adm:ping(NodeName) of
        StateMsg ->
            {ok, NodeName};
        _ ->
            receive
            after
                ?PING_WAIT_INTERVAL ->
                    wait_for_node_state(NodeName, StateMsg,
                                        PingCount - 1, Error)
            end
    end.

