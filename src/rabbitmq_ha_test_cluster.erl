%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ HA Tests.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011-2011 VMware, Inc.  All rights reserved.
%%
-module(rabbitmq_ha_test_cluster).

-export([start/1, stop/1]).
-export([kill_node/1]).

-include("rabbitmq_ha_test_cluster.hrl").

-define(PING_WAIT_INTERVAL, 1000).
-define(PING_MAX_COUNT, 3).
-define(RABBITMQ_SERVER_DIR, "../rabbitmq-server").
-define(TMP_DIR,             "/tmp/rabbitmq-test").
-define(HEADLESS, true).

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
    os:cmd("rm -rf " ++ mnesia_dir(Name)),
    filelib:ensure_dir(mnesia_dir(Name) ++ "/a"),
    filelib:ensure_dir(plugins_dir() ++ "/a"),
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
    rabbitmqctl(Name, "wait " ++ pid_file(Name)),
    ok.

mnesia_dir(Name) -> ?TMP_DIR ++ "/" ++ atom_to_list(Name) ++ "-mnesia".
pid_file(Name)   -> ?TMP_DIR ++ "/" ++ atom_to_list(Name) ++ ".pid".
plugins_dir()    -> ?TMP_DIR ++ "/no-plugins".
log_dir()        -> ?TMP_DIR ++ "/log".

stop_node(#node{name = Name}) ->
    rabbitmqctl(Name, "stop"),
    {ok, Name} = wait_for_node_stop(Name),
    ok.

%%------------------------------------------------------------------------------
%% Commands and rabbitmqctl
%%------------------------------------------------------------------------------

start_command(Name0, Port0) ->
    {Prefix, Suffix} = case ?HEADLESS of
                           true  -> {"", " &\""};
                           false -> {"xterm -e \'", "\"\'"}
                       end,
    Name = atom_to_list(Name0),
    Port = integer_to_list(Port0),
    Prefix ++
        "sh -c \"RABBITMQ_MNESIA_BASE=" ++ mnesia_dir(Name0) ++
        " RABBITMQ_LOG_BASE=" ++ log_dir() ++
        " RABBITMQ_NODENAME=" ++ Name ++
        " RABBITMQ_NODE_PORT=" ++ Port ++
        " RABBITMQ_PID_FILE=" ++ pid_file(Name0) ++
        " RABBITMQ_PLUGINS_DIR=" ++ plugins_dir() ++
        " " ++ ?RABBITMQ_SERVER_DIR ++ "/scripts/rabbitmq-server" ++ Suffix.

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
    rabbitmqctl(NodeName, "wait " ++ pid_file(NodeName)),
    {ok, NodeName}.

wait_for_node_stop(NodeName) ->
    wait_for_node_stop(NodeName, ?PING_MAX_COUNT, node_not_stopped).

wait_for_node_stop(NodeName, 0, Error) ->
    {error, {Error, NodeName}};
wait_for_node_stop(NodeName, PingCount, Error) ->
    case net_adm:ping(NodeName) of
        pang -> {ok, NodeName};
        _    -> timer:sleep(?PING_WAIT_INTERVAL),
                wait_for_node_stop(NodeName, PingCount - 1, Error)
    end.
