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
-module(rabbitmq_ha_test_tests).

-compile([export_all]).

-include("rabbitmq_ha_test_cluster.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile({parse_transform, import_as}).
-import_as({rabbitmq_ha_test_cluster, [{kill_node/1,  kill},
                                       {stop_node/1,  stop},
                                       {start_node/1, start},
                                       {add_node_to_cluster/2,
                                            add_to_cluster}]}).

-define(RABBITMQ_SERVER_DIR, "../rabbitmq-server").

-define(SIMPLE_CLUSTER, [{rabbit_misc:makenode(a), 5672},
                         {rabbit_misc:makenode(b), 5673},
                         {rabbit_misc:makenode(c), 5674}]).

test() ->
    test:test([{?MODULE, [test_send_consume,
                          test_producer_confirms,
                          test_multi_kill,
                          test_restarting_master]}],
               [report, {name, ?MODULE}]).

test_send_consume() -> test_send_consume(false).

test_send_consume(NoAck) ->
    with_simple_cluster(
      fun(_Cluster,
          [{Master, _MasterConnection, MasterChannel},
           {_Producer, _ProducerConnection, ProducerChannel},
           {_Slave, _SlaveConnection, SlaveChannel}]) ->

              %% Test the nodes policy this time.
              Nodes = [rabbit_misc:makenode(a),
                       rabbit_misc:makenode(b),
                       rabbit_misc:makenode(c)],

              %% declare the queue on the master, mirrored to the two slaves
              #'queue.declare_ok'{queue = Queue} =
                  amqp_channel:call(
                    MasterChannel,
                    #'queue.declare'{auto_delete = false,
                                     arguments   = mirror_args(Nodes)}),

              Msgs = 200,

              %% start up a consumer
              ConsumerPid = create_consumer(SlaveChannel,
                                            Queue, self(), NoAck, Msgs),

              %% send a bunch of messages from the producer
              ProducerPid = create_producer(ProducerChannel,
                                            Queue, self(), false, Msgs),

              %% create a killer for the master
              create_killer(Master, 50),

              %% verify that the consumer got all msgs, or die
              ok = wait_for_consumer_ok(ConsumerPid),

              ok = wait_for_producer_ok(ProducerPid),

              ok
      end).

test_multi_kill() ->
    with_cluster_connected(
      ?SIMPLE_CLUSTER ++
          [{rabbit_misc:makenode(d), 5675},
           {rabbit_misc:makenode(e), 5676},
           {rabbit_misc:makenode(f), 5677}],
      fun(_Cluster,
          [{Master, _MasterConnection, MasterChannel},
           {Slave1, _Slave1Connection, _Slave1Channel},
           {Slave2, _Slave2Connection, _Slave2Channel},
           {Slave3, _Slave3Connection, _Slave3Channel},
           {_Slave4, _Slave4Connection, Slave4Channel},
           {_Producer, _ProducerConnection, ProducerChannel}
          ]) ->

              %% declare the queue on the master, mirrored to the two slaves
              #'queue.declare_ok'{queue = Queue} =
                  amqp_channel:call(MasterChannel,
                                    #'queue.declare'{auto_delete = false,
                                                     arguments   =
                                                         mirror_args([])}),

              Msgs = 5000,

              %% start up a consumer
              ConsumerPid = create_consumer(Slave4Channel,
                                            Queue, self(), false, Msgs),

              %% send a bunch of messages from the producer
              ProducerPid = create_producer(ProducerChannel,
                                            Queue, self(), false, Msgs),

              %% create a killer for the master and the first 3 slaves
              [create_killer(Node, Time) || {Node, Time} <-
                                                [{Master, 50},
                                                 {Slave1, 100},
                                                 {Slave2, 200},
                                                 {Slave3, 300}
                                                ]],

              %% verify that the consumer got all msgs, or die
              ok = wait_for_consumer_ok(ConsumerPid),

              ok = wait_for_producer_ok(ProducerPid),

              ok
      end).

test_producer_confirms() ->
    with_simple_cluster(
      fun(_Cluster,
          [{Master, _MasterConnection, MasterChannel},
           {_Producer, _ProducerConnection, ProducerChannel},
           {_Slave, _SlaveConnection, _SlaveChannel}]) ->

              %% declare the queue on the master, mirrored to the two slaves
              #'queue.declare_ok'{queue = Queue} =
                  amqp_channel:call(MasterChannel,
                                    #'queue.declare'{auto_delete = false,
                                                     arguments   =
                                                         mirror_args([])}),

              Msgs = 2000,

              %% send a bunch of messages from the producer
              ProducerPid = create_producer(ProducerChannel,
                                            Queue, self(), true, Msgs),

              %% create a killer for the master
              create_killer(Master, 50),

              ok = wait_for_producer_ok(ProducerPid),

              ok
      end).

%% the queue state must be preserved if a master rejoins the cluster and
%% becomes the only node
test_restarting_master() ->
    with_simple_cluster(
      fun(_Cluster,
          [{Master,   _MasterConnection,   MasterChannel},
           {Producer, _ProducerConnection, _ProducerChannel},
           {Slave,    _SlaveConnection,    _SlaveChannel}]) ->

              Nodes = [rabbit_misc:makenode(a),
                       rabbit_misc:makenode(b),
                       rabbit_misc:makenode(c)],

              Queue = <<"ha-test-restarting-master">>,

              #'queue.declare_ok'{} =
                  amqp_channel:call(
                    MasterChannel,
                    #'queue.declare'{queue       = Queue,
                                     auto_delete = false,
                                     arguments   = mirror_args(Nodes)}),

              %% restart master
              stop(Master),
              start({Master#node.name, 5672}),
              add_to_cluster(Master#node.name, rabbit_misc:makenode(b)),

              %% retire other members of the cluster
              stop(Producer),
              stop(Slave),

              MasterConnection1 = open_connection(#node{port = 5672}),
              MasterChannel1 = open_channel( MasterConnection1 ),

              %% the master must refuse redeclaration with different parameters
              try
                  amqp_channel:call( MasterChannel1,
                                     #'queue.declare'{queue = Queue}) of
                  #'queue.declare_ok'{} -> throw({exception_expected,
                                                  ?PRECONDITION_FAILED})
              catch
                  exit:{{shutdown, {server_initiated_close,
                                    ?PRECONDITION_FAILED, _Bin}}, _Rest} -> ok
              end,

              ok
      end).

create_killer(Node, TimeMs) ->
    timer:apply_after(TimeMs, ?MODULE, kill, [Node]).

%%------------------------------------------------------------------------------
%% Consumer
%%------------------------------------------------------------------------------

wait_for_consumer_ok(ConsumerPid) ->
    ok = receive
             {ConsumerPid, ok}    -> ok;
             {ConsumerPid, Other} -> Other
         after
             60000 ->
                 {error, lost_contact_with_consumer}
         end.

create_consumer(Channel, Queue, TestPid, NoAck, ExpectingMsgs) ->
    ConsumerPid = spawn(?MODULE, consumer, [TestPid, Channel, Queue, NoAck,
                                            ExpectingMsgs + 1, ExpectingMsgs]),
    amqp_channel:subscribe(Channel,
                           #'basic.consume'{queue    = Queue,
                                            no_local = false,
                                            no_ack   = NoAck},
                           ConsumerPid),
    ConsumerPid.

consumer(TestPid, _Channel, _Queue, _NoAck, _LowestSeen, 0) ->
    consumer_reply(TestPid, ok);
consumer(TestPid, Channel, Queue, NoAck, LowestSeen, MsgsToConsume) ->
    receive
        #'basic.consume_ok'{} ->
            consumer(TestPid, Channel, Queue, NoAck, LowestSeen, MsgsToConsume);
        {Delivery = #'basic.deliver'{ redelivered = Redelivered },
         #amqp_msg{payload = Payload}} ->
            MsgNum = list_to_integer(binary_to_list(Payload)),

            maybe_ack(Delivery, Channel, NoAck),

            %% we can receive any message we've already seen and,
            %% because of the possibility of multiple requeuings, we
            %% might see these messages in any order. If we are seeing
            %% a message again, we don't decrement the MsgsToConsume
            %% counter.
            if
                MsgNum + 1 == LowestSeen ->
                    consumer(TestPid, Channel, Queue,
                             NoAck, MsgNum, MsgsToConsume - 1);
                MsgNum >= LowestSeen ->
                    true = Redelivered, %% ASSERTION
                    consumer(TestPid, Channel, Queue,
                             NoAck, LowestSeen, MsgsToConsume);
                true ->
                    %% We received a message we haven't seen before,
                    %% but it is not the next message in the expected
                    %% sequence.
                    consumer_reply(TestPid,
                                   {error, {unexpected_message, MsgNum}})
            end;
        #'basic.cancel'{} ->
            resubscribe(TestPid, Channel, Queue, NoAck,
                        LowestSeen, MsgsToConsume)
    after
        2000 ->
            consumer_reply(TestPid,
                           {error, {expecting_more_messages, MsgsToConsume}})
    end.

resubscribe(TestPid, Channel, Queue, NoAck, LowestSeen, MsgsToConsume) ->
    amqp_channel:subscribe(Channel,
                           #'basic.consume'{queue    = Queue,
                                            no_local = false,
                                            no_ack   = NoAck},
                           self()),

    ok = receive #'basic.consume_ok'{} -> ok
         after 200 -> missing_consume_ok
         end,

    consumer(TestPid, Channel, Queue, NoAck, LowestSeen, MsgsToConsume).

maybe_ack(_Delivery, _Channel, true) ->
    ok;
maybe_ack(#'basic.deliver'{delivery_tag = DeliveryTag}, Channel, false) ->
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
    ok.


consumer_reply(TestPid, Reply) ->
    TestPid ! {self(), Reply}.

%%------------------------------------------------------------------------------
%% Producer
%%------------------------------------------------------------------------------

wait_for_producer_ok(ProducerPid) ->
    ok = receive
             {ProducerPid, ok}    -> ok;
             {ProducerPid, Other} -> Other
         after
             60000 ->
                 {error, lost_contact_with_producer}
         end.

wait_for_producer_start(ProducerPid) ->
    ok = receive
             {ProducerPid, started} -> ok
         after
             10000 ->
                 {error, producer_not_started}
         end.

create_producer(Channel, Queue, TestPid, Confirm, MsgsToSend) ->
    ProducerPid = spawn(?MODULE, start_producer, [Channel, Queue, TestPid,
                                                  Confirm, MsgsToSend]),
    ok = wait_for_producer_start(ProducerPid),
    ProducerPid.



start_producer(Channel, Queue, TestPid, Confirm, MsgsToSend) ->
    ConfirmState =
        case Confirm of
            true ->
                amqp_channel:register_confirm_handler(Channel, self()),
                #'confirm.select_ok'{} =
                    amqp_channel:call(Channel, #'confirm.select'{}),
                gb_trees:empty();
            false ->
                none
        end,
    TestPid ! {self(), started},
    producer(Channel, Queue, TestPid, ConfirmState, MsgsToSend).

producer(_Channel, _Queue, TestPid, ConfirmState, 0) ->
    ConfirmState1 = drain_confirms(ConfirmState),

    case ConfirmState1 of
        none -> TestPid ! {self(), ok};
        ok   -> TestPid ! {self(), ok};
        _    -> TestPid ! {self(), {error, {missing_confirms,
                                            lists:sort(
                                              gb_trees:keys(ConfirmState1))}}}
    end;
producer(Channel, Queue, TestPid, ConfirmState, MsgsToSend) ->
    Method = #'basic.publish'{exchange    = <<"">>,
                              routing_key = Queue,
                              mandatory   = false,
                              immediate   = false},

    ConfirmState1 = maybe_record_confirm(ConfirmState, Channel, MsgsToSend),

    amqp_channel:call(Channel, Method,
                      #amqp_msg{payload = list_to_binary(
                                            integer_to_list(MsgsToSend))}),

    producer(Channel, Queue, TestPid, ConfirmState1, MsgsToSend - 1).

maybe_record_confirm(none, _, _) ->
    none;
maybe_record_confirm(ConfirmState, Channel, MsgsToSend) ->
    SeqNo = amqp_channel:next_publish_seqno(Channel),
    gb_trees:insert(SeqNo, MsgsToSend, ConfirmState).

drain_confirms(none) ->
    none;
drain_confirms(ConfirmState) ->
    case gb_trees:is_empty(ConfirmState) of
        true ->
            ok;
        false ->
            receive
                #'basic.ack'{delivery_tag = DeliveryTag,
                             multiple     = IsMulti} ->
                    ConfirmState1 =
                        case IsMulti of
                            false ->
                                gb_trees:delete(DeliveryTag, ConfirmState);
                            true ->
                                multi_confirm(DeliveryTag, ConfirmState)
                        end,
                    drain_confirms(ConfirmState1)
            after
                15000 ->
                    ConfirmState
            end
    end.

multi_confirm(DeliveryTag, ConfirmState) ->
    case gb_trees:is_empty(ConfirmState) of
        true ->
            ConfirmState;
        false ->
            {Key, _, ConfirmState1} = gb_trees:take_smallest(ConfirmState),
            case Key =< DeliveryTag of
                true ->
                    multi_confirm(DeliveryTag, ConfirmState1);
                false ->
                    ConfirmState
            end
    end.

%%------------------------------------------------------------------------------
%% Utility
%%------------------------------------------------------------------------------

with_cluster(ClusterSpec, TestFun) ->
    Cluster = rabbitmq_ha_test_cluster:start(ClusterSpec),
    try TestFun(Cluster)
    after rabbitmq_ha_test_cluster:stop(Cluster)
    end.

with_cluster_connected(ClusterSpec, TestFun) ->
    with_cluster(
      ClusterSpec,
      fun(Cluster = #cluster{nodes = Nodes}) ->
              Connections = [open_connection(Node) || Node <- Nodes],
              Channels = [open_channel(Connection)
                          || Connection <- Connections],

              Args = lists:zip3(Nodes, Connections, Channels),

              try TestFun(Cluster, Args)
              after [close(Arg) || Arg <- Args]
              end
      end).

close({_Node, Connection, Channel}) ->
    close_channel(Channel),
    close_connection(Connection).

with_simple_cluster(TestFun) ->
    with_cluster_connected(?SIMPLE_CLUSTER, TestFun).

%%------------------------------------------------------------------------------
%% Connection/Channel Utils
%%------------------------------------------------------------------------------

open_connection(#node{port = NodePort}) ->
    {ok, Connection} =
        amqp_connection:start(#amqp_params_network{port = NodePort}),
    Connection.

open_channel(Connection) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Channel.

close_connection(Connection) ->
    rabbit_misc:with_exit_handler(
      rabbit_misc:const(ok), fun () -> amqp_connection:close(Connection) end).

close_channel(Channel) ->
    rabbit_misc:with_exit_handler(
      rabbit_misc:const(ok), fun () -> amqp_channel:close(Channel) end).

%%------------------------------------------------------------------------------
%% General Utils
%%------------------------------------------------------------------------------

mirror_args([]) ->
    [{<<"x-ha-policy">>, longstr, <<"all">>}];
mirror_args(Nodes) ->
    [{<<"x-ha-policy">>, longstr, <<"nodes">>},
     {<<"x-ha-policy-params">>, array,
      [{longstr, list_to_binary(atom_to_list(N))} || N <- Nodes]}].
