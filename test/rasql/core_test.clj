(ns rasql.core-test
  (:require [clojure.test :refer :all]
            [rasql.core :refer :all])
  (:refer-clojure :exclude [comment]))

(defrelation post :posts_tbl)
(defrelation comment :comments_tbl)

;; Projection

(deftest projection-test
  (let [p (->Projection [:a :b])
       actual (to-sql p)
       expected "SELECT a, b"]
    (is (= expected actual))))

(deftest empty-projection-test
  (let [p (->Projection [])
       actual (to-sql p)
       expected "SELECT *"]
    (is (= expected actual))))

(deftest relation-projection-test
  (let [p (->Projection [(:a post)])
       actual (to-sql p)
       expected "SELECT \"post\".a"]
    (is (= expected actual))))

;; Predicates

(deftest predicate-test
  (let [p [:= (:id post) (:post_id comment)]
        actual (to-sql p)
        expected "(\"post\".id = \"comment\".post_id)"]
    (is (= expected actual))))


(deftest relation-predicate-test
  (let [p [:= (:a post) (:b post)]
        actual (to-sql p)
        expected "(\"post\".a = \"post\".b)"]
    (is (= expected actual))))

;; Joins

(deftest join-test
  (let [j (->Join comment [:= (:id post) (:post_id comment)])
        actual (to-sql j)
        expected " JOIN (SELECT * FROM comments_tbl \"comment\") \"comment\" ON (\"post\".id = \"comment\".post_id)"]
    (is (= expected actual))))

;; Relation

(deftest relation-test
  (let [actual (to-sql post)
        expected "(SELECT * FROM posts_tbl \"post\")"]
    (is (= expected actual))))

(deftest empty-project-test
  (let [r (project post [])
        actual (to-sql r)
        expected "(SELECT * FROM posts_tbl \"post\")"]
    (is (= expected actual))))

(deftest all-project-test
  (let [r (project post [:*])
        actual (to-sql r)
        expected "(SELECT * FROM posts_tbl \"post\")"]
    (is (= expected actual))))

(deftest relation-all-project-test
  (let [r (project post [(:* post)])
        actual (to-sql r)
        expected "(SELECT \"post\".* FROM posts_tbl \"post\")"]
    (is (= expected actual))))

(deftest multiple-project-test
  (let [r (project post [:a :b :c])
        actual (to-sql r)
        expected "(SELECT a, b, c FROM posts_tbl \"post\")"]
    (is (= expected actual))))

(deftest multiple-relation-column-project-test
  (let [r (project post [(:a post) (:b post) :c])
        actual (to-sql r)
        expected "(SELECT \"post\".a, \"post\".b, c FROM posts_tbl \"post\")"]
    (is (= expected actual))))

(deftest relation-select-test
  (let [r (select post [:= :title "An awesome post"])
        actual (to-sql r)
        expected "(SELECT * FROM posts_tbl \"post\" WHERE (title = 'An awesome post'))"]
    (is (= expected actual))))

(deftest relation-qualified-select-test
  (let [r (select post [:= (:author_id post) 123])
        actual (to-sql r)
        expected "(SELECT * FROM posts_tbl \"post\" WHERE (\"post\".author_id = 123))"]
    (is (= expected actual))))
