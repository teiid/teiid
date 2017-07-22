WITH CTE1 as (
        select a1 from (
                with CTE11 as (select a1 from (
                                                                WITH
                                                                        alias as (SELECT 1 as a),
                                                                        alias1 as (select alias.a as alias, AAA.a as alias1, AAA.b as a from alias join test.test_a AAA on alias.a!=AAA.a or alias.a!=AAA.a),
                                                                        alias2 as (with alias as (select alias.a as alias, AAA.a as alias1, AAA.b as a from alias join test.test_a AAA on alias.a!=AAA.a or alias.a!=AAA.a) select * from alias),
                                                                        alias3 as (with alias as (select alias.a as alias, AAA.a as alias1, AAA.a from alias join alias2 AAA on alias.a!=AAA.a or alias.a!=AAA.a) select * from alias)
                                                                SELECT AAA.alias1 as a1 FROM alias join alias1 as AAA on alias.a=AAA.a or alias.a!=AAA.a join alias2 on AAA.a=alias2.a or AAA.a!=alias.a left join alias3 on alias3.a=alias2.a or alias3.a!=alias2.a
                                                                ) tv2)
                        select a1 from CTE11
        ) as  SUBQ1),
CTE2 as (
        select a1 from (
                with CTE21 as (select a1 from (
                                                                WITH
                                                                        alias as (SELECT 1 as a),
                                                                        alias1 as (select alias.a as alias, AAA.a as alias1, AAA.b as a from alias join test.test_a AAA on alias.a!=AAA.a or alias.a!=AAA.a),
                                                                        alias2 as (with alias as (select alias.a as alias, AAA.a as alias1, AAA.b as a from alias join test.test_a AAA on alias.a!=AAA.a or alias.a!=AAA.a) select * from alias),
                                                                        alias3 as (with alias as (select alias.a as alias, AAA.a as alias1, AAA.a from alias join alias2 AAA on alias.a!=AAA.a or alias.a!=AAA.a) select * from alias)
                                                                SELECT AAA.alias1 as a1 FROM alias join alias1 as AAA on alias.a=AAA.a or alias.a!=AAA.a join alias2 on AAA.a=alias2.a or AAA.a!=alias.a left join alias3 on alias3.a=alias2.a or alias3.a!=alias2.a
                                                                ) tv2)
                        select a1 from CTE21
        ) as  SUBQ2)
select * from CTE1 as T1 join CTE2 as T2 on T1.a1=T2.a1