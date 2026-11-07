-- case: group_by_pk_dependency_django
-- title: Django grouped primary key dependency through a one-to-many left join
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_group_by_pk_dependency_django_t (id int PRIMARY KEY, name text, age int);
CREATE TABLE r0c_group_by_pk_dependency_django_u (id int PRIMARY KEY, t_id int);
INSERT INTO r0c_group_by_pk_dependency_django_t VALUES (1,'Ada',30),(2,'Bob',40),(3,'Cy',20);
INSERT INTO r0c_group_by_pk_dependency_django_u VALUES (11,1),(12,1),(13,2);
SELECT "t"."id", "t"."name", "t"."age", COUNT("u"."id") AS "u__count" FROM r0c_group_by_pk_dependency_django_t "t" LEFT OUTER JOIN r0c_group_by_pk_dependency_django_u "u" ON ("t"."id" = "u"."t_id") GROUP BY "t"."id" ORDER BY "t"."id";
