(ns rasql.core
  (:refer-clojure :exclude [max])
  (:require [clojure.string :as str])
  (:import (java.util Date)
           (java.text SimpleDateFormat)))

(defn wrap-parens [s]
  (str "(" s ")"))

(defrecord Table [table-name])
(defrecord Column [column-name relation-alias])

(defprotocol IRelation
  (base [this])
  (alt [this])
  (sel [this])
  (proj [this])
  (joins [this])
  (intersection [this])
  (union* [this]))

(deftype Relation [base-relation relation-alias selection projection joins intersect-with union-with]
  IRelation
  clojure.lang.ILookup
  (base [_] base-relation)
  (alt [_] relation-alias)
  (sel [_] selection)
  (proj [_] projection)
  (joins [_] joins)
  (intersection [_] intersect-with)
  (union* [_] union-with)
  (valAt [_ k]
    (->Column (name k) relation-alias)))

(defn new-relation [table-name & [alt sel proj joins intersection union-]]
  (Relation. (->Table table-name) alt sel proj (or joins {}) intersection union-))

(defn project [relation columns]
  ;; TODO: join existing projected columns?
  (Relation. (base relation) (alt relation) (sel relation) columns (joins relation) (intersection relation) (union* relation)))

(defn select [relation predicate]
  (Relation. (base relation) (alt relation) (if (sel relation) [:and (sel relation) predicate] predicate) (proj relation) (joins relation) (intersection relation) (union* relation)))

(defn join [relation join-relation join-predicate]
  ;; TODO: what checks are required on sels and projs?
  (Relation. (base relation) (alt relation) (sel relation) (proj relation) {:relation join-relation
                                                                            :predicate join-predicate} (intersection relation) (union* relation)))

(defn intersect [r1 r2]
  (Relation. r1 nil nil nil {} r2 nil))

(defn union [r1 r2]
  (Relation. r1 nil nil nil {} nil r2))

(defn empty-relation? [r]
  (and (empty? (sel r)) (empty? (proj r)) (empty? (joins r))))

(defmulti to-sql type)
(defmethod to-sql String [s] (str "'" s "'"))
(defmethod to-sql Long [n] (str n))
(defmethod to-sql java.util.Date [d] (str "'" (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") d) "'"))
(defmethod to-sql clojure.lang.Keyword [word] (name word))
(defmethod to-sql Table [table] (:table-name table))
(defmethod to-sql Column [column] (str (when-not (nil? (:relation-alias column)) (str (:relation-alias column)  ".")) (:column-name column)))
(defmethod to-sql Relation [relation]
  (let [raw-cols (proj relation)
        cols     (if (empty? raw-cols) ["*"] (map #(to-sql %) raw-cols))
        al       (alt relation)
        where    (sel relation)
        joins    (joins relation)
        intersection (intersection relation)
        union-   (union* relation)]
          (str "(SELECT " (str/join ", " cols)
               " FROM " (to-sql (base relation))
               (when-not (empty? al)
                 (str " " al))
               (when-not (empty? joins)
                 (str " JOIN " (if (empty-relation? (:relation joins))
                                   (to-sql (base (:relation joins)))
                                   (to-sql (:relation joins)))
                      " " (alt (:relation joins)) " ON " (to-sql (:predicate joins))))
               (when-not (empty? where) (str " WHERE " (to-sql where))) ")"
               (when-not (nil? intersection) (str " INTERSECT " (to-sql intersection)))
               (when-not (nil? union-) (str " UNION " (to-sql union-))))))
(defmethod to-sql clojure.lang.PersistentVector [predicate]
  (let [operator (first predicate)
        operands (vec (rest predicate))]
    (if (contains? #{:and :or} operator)
      (wrap-parens (str/join (str " " (str/upper-case (name operator)) " ") (map #(to-sql %) operands)))
      (wrap-parens (str/join " " [(to-sql (first operands))
                                  (name operator)
                                  (to-sql (last operands))])))))

(defn max [col]
  (str "max(" (name col) ")"))
