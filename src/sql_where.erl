-module(sql_where).

-export([
  eval/1,
  parse/1
]).

%% public
eval(_Tree) ->
  ok.

parse(String) when is_binary(String) ->
  parse(binary_to_list(String));
parse(String) when is_list(String) ->
  {ok, T, _} = sql_where_lexer:string(String),
  sql_where_parser:parse(T).

%% tests
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

parse_test() ->
  assert_parse({'=', bidder_id, 1}, "WHERE bidder_id = 1"),
  assert_parse({'=', domain, <<"ebay.ca">>}, "WHERE domain = 'ebay.ca'"),
  assert_parse({'=', domain, <<"ebay.ca">>}, "WHERE domain = \"ebay.ca\""),
  assert_parse({in, exchange_id, [1, 2, 3]}, "WHERE exchange_id IN (1, 2, 3)"),

  % TODO: complete unit tests

  assert_parse({'and', {'=', bidder_id, 1},{'or', {notin, exchange_id, [1 , 2]},
    {'=', domain, <<"ebay.ca">>}}}, "WHERE bidder_id = 1 AND (exchange_id NOT IN (1, 2) OR domain = 'ebay.ca')").

assert_parse(Expected, Expression) ->
  {ok, Tree} = parse(Expression),
  ?assertEqual(Expected, Tree).

-endif.