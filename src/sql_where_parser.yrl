Nonterminals where_clause search_cond predicate comparsion_pred in_pred
atom_commalist scalar_exp column_ref atom literal.

Terminals comp int '(' ')' '=' ',' string 'and' in 'not' 'or' where var.

Rootsymbol where_clause.

where_clause -> where search_cond : '$2'.

search_cond -> search_cond 'or' search_cond : {'or', '$1', '$3'}.
search_cond -> search_cond 'and' search_cond : {'and', '$1', '$3'}.
search_cond -> '(' search_cond ')' : '$2'.
search_cond -> predicate : '$1'.

predicate -> comparsion_pred : '$1'.
predicate -> in_pred : '$1'.

comparsion_pred -> scalar_exp comp scalar_exp : {comp, value('$2'), '$1', '$3'}.
comparsion_pred -> scalar_exp '=' scalar_exp : {comp, '=', '$1', '$3'}.

in_pred -> scalar_exp 'not' in '(' atom_commalist ')' : {notin, '$1', '$5'}.
in_pred -> scalar_exp in '(' atom_commalist ')' : {in, '$1', '$4'}.

scalar_exp -> atom : '$1'.
scalar_exp -> column_ref : '$1'.
scalar_exp -> '(' scalar_exp ')' : '$2'.

atom -> literal : '$1'.

column_ref -> var : value('$1').

literal -> int : value('$1').
literal -> string : value('$1').

atom_commalist -> atom : ['$1'].
atom_commalist -> atom_commalist ',' atom : flatten(['$1', '$3']).

Erlang code.

flatten(List) -> lists:flatten(List).
value({_, _, Value}) -> Value.
