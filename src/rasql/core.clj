(ns rasql.core
  (:require [clojure.string :as str]))

(defn unnamed-relation-alias [relation]
  (str "unnamed-relation-" (System/identityHashCode relation)))

(defprotocol IRelation
  (base [this])
  (base-alias [this])
  (relation-alias [this])
  (selection [this])
  (projection [this])
  (joins [this]))

(defrecord Column [relation column])
(defrecord Aggregate [function column rename])
(defrecord Join [relation predicate])
(defrecord Projection [columns])

(deftype Relation [base base-alias relation-alias selection projection joins]
  IRelation
  clojure.lang.ILookup
  (base [_] base)
  (base-alias [this] base-alias) ;; (or base-alias (when (= (type base) String) base (unnamed-relation-alias base))))
  (relation-alias [_] relation-alias)
  (selection [_] selection)
  (projection [_] projection)
  (joins [_] joins)
  (valAt [this k]
    (->Column this (name k))))

(defn underscore [s]
  (str/replace s #"-" "_"))

(defmacro defrelation [relation-name base]
  `(def ~relation-name (Relation. ~base (if (string? ~base) ~base (unnamed-relation-alias ~base)) (name '~relation-name) nil (->Projection []) nil)))

;;;;;; convenience methods

(defn project [relation columns]
  (let [base (base relation)
        base-alias (base-alias relation)
        relation-alias (relation-alias relation)
        selection (selection relation)
        projection (->Projection columns)
        joins (joins relation)]
    (Relation. base base-alias relation-alias selection projection joins)))

(defn select [relation predicate]
  (let [base (base relation)
        base-alias (base-alias relation)
        relation-alias (relation-alias relation)
        selection predicate
        projection (projection relation)
        joins (joins relation)]
    (Relation. base base-alias relation-alias selection projection joins)))

(defn join [relation joined-relation predicate]
  (let [base (base relation)
        base-alias (base-alias relation)
        relation-alias (relation-alias relation)
        selection (selection relation)
        projection (projection relation)
        joins (->Join joined-relation predicate)]
    (Relation. base base-alias relation-alias selection projection joins)))

(defn maximum [column rename]
  (->Aggregate :max column rename))

;;;;;; SQL

(declare to-sql)

(defn wrap-parens [s]
  (str "(" s ")"))

(defn wrap-quotes [s]
  (str "\"" s "\""))

(defn group-by-sql [p]
  (let [cols (:columns p)
        aggs (filter #(= (type %) Aggregate) cols)
        non-aggs (filter #(= (type %) Column) cols)
        non-aggs-sql (map #(to-sql %) non-aggs)
        sql (when (and (not-empty aggs) (not-empty non-aggs)) (str " GROUP BY " (str/join ", " non-aggs-sql)))]
    sql))

(defmulti to-sql type)

(defmethod to-sql nil [_])

(defmethod to-sql clojure.lang.Keyword [k]
  (name k))

(defmethod to-sql java.lang.String [s]
  (str "'" s "'"))

(defmethod to-sql java.lang.Long [n]
  n)

(defmethod to-sql Column [c]
  (let [column (:column c)
        relation-alias (relation-alias (:relation c))]
    (str (wrap-quotes relation-alias) "." column)))

(defmethod to-sql Aggregate [a]
  (let [function (name (:function a))
        column (to-sql (:column a))
        rename (to-sql (:rename a))]
  (str function (wrap-parens column) " AS " rename)))

(defmethod to-sql Projection [p]
  (let [raw-columns (:columns p)
        columns (str/join ", " (map #(to-sql %) raw-columns))
        sql-columns (if (empty? raw-columns) "*" columns)
        sql (str "SELECT " sql-columns)]
    sql))

(defmethod to-sql clojure.lang.PersistentVector [p]
  (let [operator (first p)
        operands (rest p)
        predicate-sql (if (contains? #{:and :or} operator)
                        (wrap-parens (str/join (str " " (str/upper-case (name operator)) " ") (map #(to-sql %) operands)))
                        (wrap-parens (str/join " " [(to-sql (first operands))
                                                    (name operator)
                                                    (to-sql (last operands))])))]
    predicate-sql))

(defmethod to-sql Join [j]
  (let [relation (:relation j)
        relation-alias (relation-alias relation)
        predicate (:predicate j)]
    (str " JOIN " (to-sql relation) " " (wrap-quotes relation-alias) " ON " (to-sql predicate))))

(defmethod to-sql Relation [relation]
  (let [base (base relation)
        projection (projection relation)
        selection (selection relation)
        columns (or (to-sql projection) "*")
        joins (joins relation)
        alias (wrap-quotes (relation-alias relation)) ;; (when (= (type base) Relation) (str " " (relation-alias base)))
        select-clause (to-sql projection)
        from-clause (str " FROM " (to-sql base) " " alias)
        join-clause (to-sql joins)
        where-clause (when-not (empty? selection) (str " WHERE " (to-sql selection)))
        group-by-clause (group-by-sql projection)]
    (wrap-parens (str select-clause
                      from-clause
                      join-clause
                      where-clause
                      group-by-clause))))
