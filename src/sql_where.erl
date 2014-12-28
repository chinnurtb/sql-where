-module(sql_where).
-compile([native]).

-export([
    evaluate/2,
    parse/1
]).

-define(NULL, undefined).

%% public
evaluate({'and', A, B}, Vars) ->
    evaluate(A, Vars) andalso evaluate(B, Vars);
evaluate({'or', A, B}, Vars) ->
    evaluate(A, Vars) orelse evaluate(B, Vars);
evaluate({comp, Comp, Var, Value}, Vars) ->
    compare(Comp, Var, Value, Vars);
evaluate({in, Var, List}, Vars) ->
    lists:member(lookup(Var, Vars), List);
evaluate({notin, Var, List}, Vars) ->
    not lists:member(lookup(Var, Vars), List);
evaluate({null, Var}, Vars) ->
    lookup(Var, Vars) =:= ?NULL;
evaluate({notnull, Var}, Vars) ->
    lookup(Var, Vars) =/= ?NULL.


parse(String) when is_binary(String) ->
    parse(binary_to_list(String));
parse(String) when is_list(String) ->
    case sql_where_lexer:string(String) of
        {ok, Tokens, _} ->
            case sql_where_parser:parse(Tokens) of
                {ok, Tree} -> {ok, Tree};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} -> {error, Reason}
    end.

%% private
compare(Comp, Var, Value, Vars) ->
    comp(Comp, lookup(Var, Vars), Value).

comp('=', Var, Value) ->
    Var =:= Value;
comp('<', Var, Value) ->
    Var < Value;
comp('<=', Var, Value) ->
    Var =< Value;
comp('>=', Var, Value) ->
    Var >= Value;
comp('>', Var, Value) ->
    Var > Value;
comp('<>', Var, Value) ->
    Var =/= Value.

lookup(Key, List) ->
    case lists:keyfind(Key, 1, List) of
        false -> ?NULL;
        {_, Value} -> Value
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% tests
benchmark_test() ->
    {ok, Tree} = parse("WHERE exchange_id = 1 AND exchange_seller_id = 181 AND bidder_id IN (1, 5) AND buyer_spend > 150"),

    Vars = [
        {exchange_id, 1},
        {exchange_seller_id, 181},
        {bidder_id, 1},
        {buyer_spend, 200}
    ],

    FunEvaluate = fun() -> evaluate(Tree, Vars) end,
    benchmark(evaluate, FunEvaluate, 10000000).

evaluate_test() ->
    % comp predictate
    ?assert(evaluate({comp, '=', bidder_id, 1}, [{bidder_id, 1}])),
    ?assertNot(evaluate({comp, '=', bidder_id, 1}, [{bidder_id, 2}])),
    ?assert(evaluate({comp, '<', price, 100}, [{price, 60}])),
    ?assertNot(evaluate({comp, '<', price, 100}, [{price, 160}])),
    ?assert(evaluate({comp, '<=', price, 100}, [{price, 100}])),
    ?assertNot(evaluate({comp, '<=', price, 100}, [{price, 160}])),
    ?assert(evaluate({comp, '>=', price, 100}, [{price, 100}])),
    ?assertNot(evaluate({comp, '>=', price, 160}, [{price, 100}])),
    ?assert(evaluate({comp, '>', price, 100}, [{price, 160}])),
    ?assertNot(evaluate({comp, '>', price, 100}, [{price, 60}])),
    ?assert(evaluate({comp, '<>', price, 100}, [{price, 160}])),
    ?assertNot(evaluate({comp, '<>', price, 100}, [{price, 100}])),

    % in predictate
    ?assert(evaluate({in, exchange_id, [1 , 2]}, [{exchange_id, 2}])),
    ?assertNot(evaluate({in, exchange_id, [1 , 2]}, [{exchange_id, 3}])),
    ?assert(evaluate({notin, exchange_id, [1 , 2]}, [{exchange_id, 3}])),
    ?assertNot(evaluate({notin, exchange_id, [1 , 2]}, [{exchange_id, 2}])),

    % null predictate
    ?assert(evaluate({null, exchange_id}, [])),
    ?assertNot(evaluate({null, exchange_id}, [{exchange_id, 3}])),
    ?assert(evaluate({notnull, exchange_id}, [{exchange_id, 3}])),
    ?assertNot(evaluate({notnull, exchange_id}, [{exchange_id, ?NULL}])),

    % and
    ?assert(evaluate({'and', {comp, '=', bidder_id, 1}, {comp, '=', bidder_id, 1}},
        [{bidder_id, 1}])),
    ?assertNot(evaluate({'and', {comp, '=', bidder_id, 1}, {comp, '=', exchange_id, 1}},
        [{bidder_id, 1}, {exchange_id, 2}])),

    % or
    ?assert(evaluate({'or', {comp, '=', bidder_id, 2}, {comp, '=', bidder_id, 1}},
        [{bidder_id, 1}])),
    ?assertNot(evaluate({'or', {comp, '=', bidder_id, 2}, {comp, '=', bidder_id, 3}},
        [{bidder_id, 1}])).

parse_test() ->
    assert_parse({comp, '=', bidder_id, 1}, "WHERE bidder_id = 1"),
    assert_parse({comp, '=', domain, <<"ebay.ca">>}, "WHERE domain = 'ebay.ca'"),
    assert_parse({comp, '=', domain, <<"ebay.ca">>}, "WHERE domain = \"ebay.ca\""),
    assert_parse({in, exchange_id, [1, 2, 3]}, "WHERE exchange_id IN (1, 2, 3)"),
    assert_parse({'and', {comp, '=', bidder_id, 1}, {'or', {notin, exchange_id, [1 , 2]},
      {comp, '=', domain, <<"ebay.ca">>}}}, "WHERE bidder_id = 1 AND (exchange_id NOT IN (1, 2) OR domain = 'ebay.ca')").

parse_select_test() ->
    assert_parse({{select,'*'}, {from, 'bids'},{where,{comp, '=', bidder_id, 1}}}, "select * from bids WHERE bidder_id = 1"),
    assert_parse({{select,'id'}, {from, 'bids'},{where,{comp, '=', bidder_id, 1}}}, "select id from bids WHERE bidder_id = 1"),
    assert_parse({{select,['pacing','enabled']},
                  {from,['ads','users']},
                  {where,{'and', {comp, '=', 'ads.os', <<"ios">>},
                          {comp, '=', 'users.id', 1}}}},
                 "select pacing, enabled from ads,users WHERE ads.os='ios' AND users.id = 1").


%% test_utils
assert_parse(Expected, Expression) ->
    {ok, Tree} = parse(Expression),
    ?assertEqual(Expected, Tree).

benchmark(Name, Fun, N) ->
    Timestamp = os:timestamp(),
    ok = loop(Fun, N),
    Time = timer:now_diff(os:timestamp(), Timestamp) / N,
    ?debugFmt("~p: ~p microseconds", [Name, Time]).

loop(_, 0) ->
    ok;
loop(Fun, N) ->
    Fun(),
    loop(Fun, N - 1).

-endif.
