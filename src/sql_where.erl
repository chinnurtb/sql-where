% TODO: complete unit tests

-module(sql_where).

-export([
    evaluate/2,
    parse/1
]).

%% public
% -spec(tuple(), list(tuple())) -> boolean().
evaluate(Tree, Proplist) ->
    try eval(Tree, Proplist)
    catch
        E:R ->
            % debug
            io:format("~p:~p", [E, R])
    end.

% -spec parse(binary() | list()) -> {ok, tuple()} | {error, atom()}.
parse(String) when is_binary(String) ->
    parse(binary_to_list(String));
parse(String) when is_list(String) ->
    try
        {ok, Tokens, _} = sql_where_lexer:string(String),
        {ok, Tree} = sql_where_parser:parse(Tokens),
        {ok, Tree}
    catch
        E:R ->
            % debug
            io:format("~p:~p", [E, R])
    end.

%% private
compare(Comp, Var, Value, Proplist) ->
    Var2 = lookup(Var, Proplist),
    comp(Comp, Var2, Value).

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

eval({'and', A, B}, Proplist) ->
    eval(A, Proplist) andalso eval(B, Proplist);
eval({'or', A, B}, Proplist) ->
    eval(A, Proplist) orelse eval(B, Proplist);
eval({comp, Comp, Var, Value}, Proplist) ->
    compare(Comp, Var, Value, Proplist);
eval({in, Var, List}, Proplist) ->
    Var2 = lookup(Var, Proplist),
    list:member(Var2, List);
eval({notin, Var, List}, Proplist) ->
    Var2 = lookup(Var, Proplist),
    not list:member(Var2, List).

lookup(Key, List) ->
    case lists:keyfind(Key, 1, List) of
        false -> undefined;
        {_, Value} -> Value
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% tests
benchmark_test() ->
    ?debugFmt("~p seconds", [10]),
    ok.

evaluate_test() ->
    ?assert(evaluate({comp, '=', bidder_id, 1}, [{bidder_id, 1}])),
    ?assertNot(evaluate({comp, '=', bidder_id, 1}, [{bidder_id, 2}])),
    ?assert(evaluate({comp, '>', price, 100}, [{price, 160}])),
    ?assertNot(evaluate({comp, '>', price, 100}, [{price, 60}])).

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

-endif.
