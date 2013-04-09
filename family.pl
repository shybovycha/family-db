:- use_module(library(odbc)).

connect :-
	odbc_connect(prolog_lab4, C, [user(root), password(abcABC123), alias(mydb), open(once)]).

assoc_children(FamilyId, [Head|Tail]) :-
	connect,
	odbc_prepare(mydb,
		'insert into children (family_id, child_name) values (?, ?);',
		[integer, varchar],
		Statement),
	odbc_execute(Statement, [FamilyId, Head]),
	assoc_children(FamilyId, Tail).

fam(Surname, MaleName, FemaleName, Children) :-
	connect,
	odbc_prepare(mydb,
		'insert into families (surname, male_name, female_name) values (?, ?, ?);',
		[varchar, varchar, varchar],
		Statement),
	odbc_execute(Statement, [Surname, MaleName, FemaleName]),
	odbc_query(mydb, 'select max(id) from families;', FamilyIdRow),
	arg(1, FamilyIdRow, FamilyId),
	assoc_children(FamilyId, Children).

fam_without_children :-
	fam_with_children(0).

fam_with_children(N) :-
	connect,
	odbc_prepare(mydb, 'select * from (select A.id as family_id, A.surname as surname, A.male_name as male_name, A.female_name as female_name, count(B.id) as children_cnt from families as A left join children as B on (A.id = B.family_id) group by A.id) as M where M.children_cnt = ?;', [integer], Statement),
	odbc_execute(Statement, [N], X),
	% arg(1, X, X1), % family id
	arg(2, X, X2), % surname
	arg(3, X, X3), % male_name
	arg(4, X, X4), % female_name
	write('('), write(X3), write(', '), write(X4), write(') '), writeln(X2).

fam_with_children_more_than(N) :-
	connect,
	odbc_prepare(mydb, 'select * from (select A.id as family_id, A.surname as surname, A.male_name as male_name, A.female_name as female_name, count(B.id) as children_cnt from families as A left join children as B on (A.id = B.family_id) group by A.id) as M where M.children_cnt >= ?;', [integer], Statement),
	odbc_execute(Statement, [N], X),
	% arg(1, X, X1), % family id
	arg(2, X, X2), % surname
	arg(3, X, X3), % male_name
	arg(4, X, X4), % female_name
	write('('), write(X3), write(', '), write(X4), write(') '), writeln(X2).

fam_children_by_surname(N) :-
	connect,
	odbc_prepare(mydb, 'select A.id as family_id, A.surname as surname, A.male_name as male_name, A.female_name as female_name, B.child_name as child_name from families as A right join children as B on (A.id = B.family_id) where A.surname = ?;', [varchar], Statement),
	odbc_execute(Statement, [N], X),
	% arg(1, X, X1), % family id
	arg(2, X, X2), % surname
	arg(3, X, X3), % male_name
	arg(4, X, X4), % female_name
	arg(5, X, X5), % child_name
	write('('), write(X2), write(' '), write(X3), write(' та '), write(X4), write(') '), writeln(X5).

fam_by_children_name(N) :-
	connect,
	odbc_prepare(mydb, 'select A.id as family_id, A.surname as surname, A.male_name as male_name, A.female_name as female_name, B.child_name as child_name from families as A right join children as B on (A.id = B.family_id) where B.child_name = ?;', [varchar], Statement),
	odbc_execute(Statement, [N], X),
	% arg(1, X, X1), % family id
	arg(2, X, X2), % surname
	arg(3, X, X3), % male_name
	arg(4, X, X4), % female_name
	arg(5, X, X5), % child_name
	write(X2), write(' '), write(X3), write(' та '), writeln(X4).

families :-
	connect,
	odbc_query(mydb, 'select * from families as A left join children as B on (A.id = B.family_id);', Z),
	arg(1, Z, X1),
	arg(2, Z, X2),
	arg(3, Z, X3),
	arg(4, Z, X4),
	arg(7, Z, X5),
	write(X1), write('; '),
	write(X2), write('; '),
	write(X3), write('; '),
	write(X4), write('; '),
	write(X5), nl.

