(ns rasql.core-test
  (:require [clojure.test :refer :all]
            [rasql.core :refer :all])
  (:import (java.util GregorianCalendar)))

;; Relation

(deftest relation-alias-test
  (let [r (new-relation "table" "x")]
    (is (= (alt r) "x"))))

(deftest basic-relation-test
  (let [blah (new-relation "blah")]
    (is (= (to-sql blah)
           "(SELECT * FROM blah)"))))

(deftest project-one-test
  (let [blah (project (new-relation "blah") [:a])]
    (is (= (to-sql blah)
           "(SELECT a FROM blah)"))))

;; to-sql projection

(deftest project-mult-test
  (let [blah (project (new-relation "blah") [:a :b :c])]
    (is (= (to-sql blah)
           "(SELECT a, b, c FROM blah)"))))

(deftest project-relation-col-test
  (let [some-table (new-relation "sometable")
        blah (project (new-relation "blah") [(:somecol some-table)])]
    (is (= (to-sql blah)
           "(SELECT somecol FROM blah)"))))

(deftest project-aliased-relation-col-test
  (let [t (new-relation "table" "t1")]
    (is (= (to-sql (project t [(:id t)]))
           "(SELECT t1.id FROM table t1)"))))

;; to-sql selection

(deftest select-single-test
  (let [t (new-relation "table")]
    (is (= (to-sql (select t [:= :id "123"]))
           "(SELECT * FROM table WHERE (id = '123'))"))))

(deftest select-single-table-col-test
  (let [t (new-relation "table")]
    (is (= (to-sql (select t [:= (:id t) "123"]))
           "(SELECT * FROM table WHERE (id = '123'))"))))

(deftest select-multiple-predicates-test
  (let [t (new-relation "blah")
        t' (select t [:= (:a t) "123"])
        t'' (select t' [:= (:b t) 456])]
    (is (= (to-sql t'')
           "(SELECT * FROM blah WHERE ((a = '123') AND (b = 456)))"))))

(deftest date-predicate-test
  (let [d (.getTime (GregorianCalendar. 2014 0 14))]
    (is (= (to-sql [:= :asdf d])
           "(asdf = '2014-01-14')"))))

(deftest predicate-value-first-test
    (is (= (to-sql [:= 123 :id])
        "(123 = id)")))

;; join

(deftest join-test
  (let [p (new-relation "person" "p")
        n (new-relation "names" "n")
        p-n (join p n [:= (:emplid p) (:emplid n)])]
    (is (= (to-sql p-n)
           "(SELECT * FROM person p JOIN names n ON (p.emplid = n.emplid))"))))

;; (deftest multi-join-test
;;   (let [p (new-relation "person" "p")
;;         n (new-relation "names" "n")
;;         e (new-relation "email_addresses" "e")
;;         p-n (join p n [:= (:emplid p) (:emplid n)])
;;         p-n-e (join p-n e [:= (:emplid p-n) (:emplid e)])]
;;     (is (= (to-sql p-n-e)
;;            "(SELECT * FROM (SELECT * FROM person p JOIN names n ON (p.emplid = n.emplid)) p JOIN email_addresses e ON (p.emplid = e.emplid))"))))

;; intersect

(deftest intersect-test
  (let [a (new-relation "a")
        b (new-relation "b")]
    (is (= (to-sql (intersect a b))
           "((SELECT * FROM (SELECT * FROM a)) INTERSECT (SELECT * FROM b))"))))

;; union

(deftest union-test
  (let [a (new-relation "a")
        b (new-relation "b")]
    (is (= (to-sql (union a b))
           "((SELECT * FROM (SELECT * FROM a)) UNION (SELECT * FROM b))"))))
