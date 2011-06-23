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
