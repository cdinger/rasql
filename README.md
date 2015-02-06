# RASQL

A Clojure library for converting relational algebra expressions to SQL. What could be more exciting?!

Note that this library itself is not concerned with talking to databases. Its sole responsiblity is to produce SQL statements.

This library is mostly a personal experiment to flesh out some ideas that have
been bouncing around for a while. Please don't use this for production purposes.

## Basic usage

Relations are created and named with the `defrelation` macro. Pass in a name and a keyword that maps to a table
in your RDMS.

```clojure
(defrelation person :ps_person)

(to-sql person)
;; "(SELECT * FROM \"ps_person\")"
```

### Project

The `project` function takes a relation and a vector of columns you want returned from the relation.

```clojure
(to-sql (project person [:emplid :date_of_birth]))
;; (SELECT * FROM "ps_person" WHERE (emplid = '1234567'))
```

You can scope a projected column by explicitly defining the source relation. This is useful when joining.

```clojure
(to-sql (project person [(:emplid  person) (:date_of_birth person)]))
;; "(SELECT \"person\".emplid, \"person\".date_of_birth FROM ps_person \"person\")"
```

### Select

The `select` function limits the rows returned according to the provided predicate. Predicates for `select`s and `join`s are expressed with vectors:

```clojure
(to-sql (select person [:= :emplid "1234567"]))
;; "(SELECT * FROM ps_person \"person\" WHERE (emplid = '1234567'))"

(to-sql (select person [:or [:= :emplid "1234567"]
                            [:= :emplid "1234568"]
                            [:= :emplid "1234569"]]))
;; "(SELECT * FROM ps_person \"person\" WHERE ((emplid = '1234567') OR (emplid = '1234568') OR (emplid = '1234569')))"
```

### Join

A relation can be joined to another relation with `join`. A predicate is supplied (just like with `select`) to define the join criteria:

```clojure
(defrelation person :ps_person)
(defrelation name :ps_names)

(to-sql (join person name [:= (:emplid person) (:emplid name)]))
;; "(SELECT * FROM ps_person \"person\" JOIN (SELECT * FROM ps_names \"name\") \"name\" ON (\"person\".emplid = \"name\".emplid))"
```

### Naming relations

It's a good practice to name all relations—especially when you have a lot of them working together. Name a relation by passing it into `defrelation`, just like you would with a table name. From the previous example, it'd be better to name the resulting relation something like `person-with-name`:

```clojure
(defrelation person :ps_person)
(defrelation name :ps_names)
(defrelation person-with-name (join person name [:= (:emplid person) (:emplid name)]))

(to-sql person-with-name)
;; "(SELECT * FROM (SELECT * FROM ps_person \"person\" JOIN (SELECT * FROM ps_names \"name\") \"name\" ON (\"person\".emplid = \"name\".emplid)) \"person-with-name\")"
```

## Everything is a relation

The result of every projection, selection, join (and others) is just a relation. This lets you compose
smaller relations to get around any wackyness your database schema is imposing on you.

```clojure
(defrelation person :ps_person)

(type person)
;; rasql.core.Relation

(type (project person [:emplid :dob]))
;; rasql.core.Relation

(type (select person [:= :emplid "1234567"]))
;; rasql.core.Relation
```

## Correlated subqueries

You can reference columns that are defined outside the current lexical scope by creating a var of a relation and using it like a value:

```clojure
(defrelation dept :ps_dept_tbl)
(defrelation inner-dept :ps_dept_tbl)

(def max-effdt
  (-> inner-dept
      (project [(maximum (:effdt inner-dept) :effdt)])
      (select [:and [:= (:setid inner-dept) (:setid dept)]
                    [:= (:deptid inner-dept) (:setid dept)]
                    [:<= (:effdt inner-dept) "01-JAN-14"]])))

(to-sql (select dept [:= (:effdt dept) max-effdt]))
;; "(SELECT * FROM ps_dept_tbl \"dept\" WHERE (\"dept\".effdt = (SELECT max(\"inner-dept\".effdt) AS effdt FROM ps_dept_tbl \"inner-dept\" WHERE ((\"inner-dept\".setid = \"dept\".setid) AND (\"inner-dept\".deptid = \"dept\".setid) AND (\"inner-dept\".effdt <= '01-JAN-14')))))"
```

## Tackling complex queries with composition

Working with a schema like PeopleSoft results in beastly, mind-bending SQL queries. RASQL lets you easily break down nastiness into more comprehensible components, yielding simple to use and simply named relations like `effective-acad-prog`:

```clojure
(defrelation acad-prog :ps_acad_prog)

(defrelation effdt-of-acad-prog
  (-> acad-prog
      (project [(:emplid acad-prog)
                (:acad_career acad-prog)
                (:stdnt_car_nbr acad-prog)
                (maximum (:effdt acad-prog) :effdt)])
      (select [:<= (:effdt acad-prog) Date.])))

(defrelation eff-keys-acad-prog
  (-> acad-prog
      (project [(:emplid acad-prog)
                (:acad_career acad-prog)
                (:stdnt_car_nbr acad-prog)
                (:effdt acad-prog)
                (maximum (:effseq acad-prog) :effseq)])
      (join effdt-of-acad-prog [:and [:= (:emplid acad-prog) (:emplid effdt-of-acad-prog)]
                                     [:= (:acad_career acad-prog) (:acad_career effdt-of-acad-prog)]
                                     [:= (:stdnt_car_nbr acad-prog) (:stdnt_car_nbr effdt-of-acad-prog)]
                                     [:= (:effdt acad-prog) (:effdt effdt-of-acad-prog)]])))

(defrelation effective-acad-prog
  (-> acad-prog
      (project [(:* acad-prog)])
      (join eff-keys-acad-prog [:and [:= (:emplid acad-prog) (:emplid eff-keys-acad-prog)]
                                     [:= (:acad_career acad-prog) (:acad_career eff-keys-acad-prog)]
                                     [:= (:stdnt_car_nbr acad-prog) (:stdnt_car_nbr eff-keys-acad-prog)]
                                     [:= (:effdt acad-prog) (:effdt eff-keys-acad-prog)]
                                     [:= (:effseq acad-prog) (:effseq eff-keys-acad-prog)]])))

(to-sql (select effective-acad-prog [:= (:emplid effective-acad-prog) "1234567"]))
;; "(SELECT * FROM (SELECT \"acad_prog\".* FROM ps_acad_prog \"acad_prog\" JOIN (SELECT * FROM (SELECT \"acad_prog\".emplid, \"acad_prog\".acad_career, \"acad_prog\".stdnt_car_nbr, \"acad_prog\".effdt, max(\"acad_prog\".effseq) AS effseq FROM ps_acad_prog \"acad_prog\" JOIN (SELECT * FROM (SELECT \"acad_prog\".emplid, \"acad_prog\".acad_career, \"acad_prog\".stdnt_car_nbr, max(\"acad_prog\".effdt) AS effdt FROM ps_acad_prog \"acad_prog\" WHERE (\"acad_prog\".effdt <= 'SYSDATE') GROUP BY \"acad_prog\".emplid, \"acad_prog\".acad_career, \"acad_prog\".stdnt_car_nbr) \"effdt_of_acad_prog\") \"effdt_of_acad_prog\" ON ((\"acad_prog\".emplid = \"effdt_of_acad_prog\".emplid) AND (\"acad_prog\".acad_career = \"effdt_of_acad_prog\".acad_career) AND (\"acad_prog\".stdnt_car_nbr = \"effdt_of_acad_prog\".stdnt_car_nbr) AND (\"acad_prog\".effdt = \"effdt_of_acad_prog\".effdt)) GROUP BY \"acad_prog\".emplid, \"acad_prog\".acad_career, \"acad_prog\".stdnt_car_nbr, \"acad_prog\".effdt) \"eff_keys_acad_prog\") \"eff_keys_acad_prog\" ON ((\"acad_prog\".emplid = \"eff_keys_acad_prog\".emplid) AND (\"acad_prog\".acad_career = \"eff_keys_acad_prog\".acad_career) AND (\"acad_prog\".stdnt_car_nbr = \"eff_keys_acad_prog\".stdnt_car_nbr) AND (\"acad_prog\".effdt = \"eff_keys_acad_prog\".effdt) AND (\"acad_prog\".effseq = \"eff_keys_acad_prog\".effseq))) \"eff_acad_prog\" WHERE (\"eff_acad_prog\".emplid = '1234567'))"
```

## SQL generation

> What's with all the subqueries? I don't write queries that way!

Yep—a human is not writing these queries. The point of this library is to liberate you from SQL. The generated SQL is not intended to look as though a human wrote it—it's compiled. If fact, thinking in relations will sometimes [produce more performant queries than humans would typically write]().

## Development

RASQL is a [Leiningen 2](http://leiningen.org/) project. Clone this repo and `lein test` to run the test suite.

## License

Copyright © 2014 Chris Dinger

Distributed under the Eclipse Public License either version 1.0.
