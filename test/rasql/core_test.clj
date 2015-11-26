(ns rasql.core-test
  (:require [clojure.test :refer :all]
            [rasql.core :refer :all]))

(defrelation posts :posts_tbl)
(defrelation comments :comments_tbl)

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
  (let [p (->Projection [(:a posts)])
       actual (to-sql p)
       expected "SELECT \"posts\".a"]
    (is (= expected actual))))

;; Predicates

(deftest predicate-test
  (let [p [:= (:id posts) (:posts_id comments)]
        actual (to-sql p)
        expected "(\"posts\".id = \"comments\".posts_id)"]
    (is (= expected actual))))


(deftest relation-predicate-test
  (let [p [:= (:a posts) (:b posts)]
        actual (to-sql p)
        expected "(\"posts\".a = \"posts\".b)"]
    (is (= expected actual))))

;; Joins

(deftest join-test
  (let [j (->Join comments [:= (:id posts) (:posts_id comments)])
        actual (to-sql j)
        expected " JOIN (SELECT * FROM comments_tbl \"comments\") \"comments\" ON (\"posts\".id = \"comments\".posts_id)"]
    (is (= expected actual))))

;; Relation

(deftest relation-test
  (let [actual (to-sql posts)
        expected "(SELECT * FROM posts_tbl \"posts\")"]
    (is (= expected actual))))

(deftest empty-project-test
  (let [r (project posts [])
        actual (to-sql r)
        expected "(SELECT * FROM posts_tbl \"posts\")"]
    (is (= expected actual))))

(deftest all-project-test
  (let [r (project posts [:*])
        actual (to-sql r)
        expected "(SELECT * FROM posts_tbl \"posts\")"]
    (is (= expected actual))))

(deftest relation-all-project-test
  (let [r (project posts [(:* posts)])
        actual (to-sql r)
        expected "(SELECT \"posts\".* FROM posts_tbl \"posts\")"]
    (is (= expected actual))))

(deftest multiple-project-test
  (let [r (project posts [:a :b :c])
        actual (to-sql r)
        expected "(SELECT a, b, c FROM posts_tbl \"posts\")"]
    (is (= expected actual))))

(deftest multiple-relation-column-project-test
  (let [r (project posts [(:a posts) (:b posts) :c])
        actual (to-sql r)
        expected "(SELECT \"posts\".a, \"posts\".b, c FROM posts_tbl \"posts\")"]
    (is (= expected actual))))

(deftest relation-select-test
  (let [r (select posts [:= :title "An awesome posts"])
        actual (to-sql r)
        expected "(SELECT * FROM posts_tbl \"posts\" WHERE (title = 'An awesome posts'))"]
    (is (= expected actual))))

(deftest relation-qualified-select-test
  (let [r (select posts [:= (:author_id posts) 123])
        actual (to-sql r)
        expected "(SELECT * FROM posts_tbl \"posts\" WHERE (\"posts\".author_id = 123))"]
    (is (= expected actual))))

;; Grouping

(deftest group-by-test
  (let [r (project posts [(maximum :id :max_id) (:blah posts)])
        actual (to-sql r)
        expected "(SELECT max(id) AS max_id, \"posts\".blah FROM posts_tbl \"posts\" GROUP BY \"posts\".blah)"]
    (is (= expected actual))))

(deftest exclude-group-by-when-single-aggregate-test
  (let [r (project posts [(maximum :id :max_id)])
        actual (to-sql r)
        expected "(SELECT max(id) AS max_id FROM posts_tbl \"posts\")"]
    (is (= expected actual))))
