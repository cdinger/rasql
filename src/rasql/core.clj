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
  (joins [this]))

(deftype Relation [base-relation relation-alias selection projection joins]
  IRelation
  clojure.lang.ILookup
  (base [_] base-relation)
  (alt [_] relation-alias)
  (sel [_] selection)
  (proj [_] projection)
  (joins [_] joins)
  (valAt [_ k]
    ; (str (when-not (empty? relation-alias) (str relation-alias  ".")) (name k))))
    (->Column (name k) relation-alias)))

(defn new-relation [table-name & [alt sel proj joins]]
  (Relation. (->Table table-name) alt sel proj (or joins {})))

(defn project [relation columns]
  ;; TODO: join existing projected columns?
  (Relation. (base relation) (alt relation) (sel relation) columns (joins relation)))

(defn select [relation predicate]
  (Relation. (base relation) (alt relation) (if (sel relation) [:and (sel relation) predicate] predicate) (proj relation) (joins relation)))

(defn join [relation join-relation join-predicate]
  ;; TODO: what checks are required on sels and projs?
  (Relation. (base relation) (alt relation) (sel relation) (proj relation) {:relation join-relation
                                                                            :predicate join-predicate}))

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
        alt      (alt relation)
        where    (sel relation)
        joins    (joins relation)]
          (wrap-parens (str "SELECT " (str/join ", " cols)
                            " FROM " (to-sql (base relation))
                            (when-not (empty? joins)
                              (str " JOIN " (to-sql (:relation joins)) " ON " (:predicate joins)))
                            (when-not (empty? alt)
                              (str " " alt))
                            (when-not (empty? where) (str " WHERE " (to-sql where)))))))
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
