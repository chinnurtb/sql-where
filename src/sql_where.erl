% TODO: complete unit tests

-module(sql_where).

-export([
    evaluate/2,
    parse/1
]).

%% public
evaluate({'and', A, B}, Proplist) ->
    evaluate(A, Proplist) andalso evaluate(B, Proplist);
evaluate({'or', A, B}, Proplist) ->
    evaluate(A, Proplist) orelse evaluate(B, Proplist);
evaluate({comp, Comp, Var, Value}, Proplist) ->
    compare(Comp, Var, Value, Proplist);
evaluate({in, Var, List}, Proplist) ->
    lists:member(lookup(Var, Proplist), List);
evaluate({notin, Var, List}, Proplist) ->
    not lists:member(lookup(Var, Proplist), List).

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
compare(Comp, Var, Value, Proplist) ->
    comp(Comp, lookup(Var, Proplist), Value).

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
        false -> undefined;
        {_, Value} -> Value
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% tests
benchmark_test() ->
    {ok, Tree} = parse("WHERE exchange_id = 1 AND exchange_seller_id = 181 AND bidder_id IN (1, 5) AND buyer_spend > 150"),

    Proplist = [
        {exchange_id, 1},
        {exchange_seller_id, 181},
        {bidder_id, 1},
        {buyer_spend, 200}
    ],

    FunEvaluate = fun() -> evaluate(Tree, Proplist) end,
    benchmark(evaluate, FunEvaluate, 100000).

evaluate_test() ->
    ?assert(evaluate({comp, '=', bidder_id, 1}, [{bidder_id, 1}])),
    ?assertNot(evaluate({comp, '=', bidder_id, 1}, [{bidder_id, 2}])),
    ?assert(evaluate({comp, '>', price, 100}, [{price, 160}])),
    ?assertNot(evaluate({comp, '>', price, 100}, [{price, 60}])),
    ?assert(evaluate({in, exchange_id, [1 , 2]}, [{exchange_id, 2}])).

parse_test() ->
    assert_parse({comp, '=', bidder_id, 1}, "WHERE bidder_id = 1"),
    assert_parse({comp, '=', domain, <<"ebay.ca">>}, "WHERE domain = 'ebay.ca'"),
    assert_parse({comp, '=', domain, <<"ebay.ca">>}, "WHERE domain = \"ebay.ca\""),
    assert_parse({in, exchange_id, [1, 2, 3]}, "WHERE exchange_id IN (1, 2, 3)"),
    assert_parse({'and', {comp, '=', bidder_id, 1}, {'or', {notin, exchange_id, [1 , 2]},
      {comp, '=', domain, <<"ebay.ca">>}}}, "WHERE bidder_id = 1 AND (exchange_id NOT IN (1, 2) OR domain = 'ebay.ca')").

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
