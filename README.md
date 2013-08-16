sql-where
=========

Building:
    
    rebar compile

Parsing the expression:

    {ok, ExpTree} = sql_where:parse("WHERE bidder_id = 1 AND (exchange_id IN (1, 2) OR domain = 'ebay.ca')")
    
Evaluating the expression tree:

    true = sql_where:evaluate(ExpTree, [{bidder_id, 1}, {exchange_id, 1}])
    true = sql_where:evaluate(ExpTree, [{bidder_id, 1}, {domain, <<"ebay.ca">>}])
    false = sql_where:evaluate(ExpTree, [{bidder_id, 2}])
    false = sql_where:evaluate(ExpTree, [{bidder_id, 1}, {exchange_id, 3}])
    false = sql_where:evaluate(ExpTree, [{bidder_id, 1}, {domain, <<"google.com">>}])
    
Running tests:

    rebar eunit
