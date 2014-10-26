(ns rasql.core
  (:require [clojure.string :as str])
  (:import (java.util Date)
           (java.text SimpleDateFormat)))

(declare to-sql)

(defn wrap-parens [s]
  (str "(" s ")"))

(defn uniq-alias [table]
  ;; TODO: it'd be better use
  ;; (str (:table-name table) "_" (System/identityHashCode table)))
  (str (:table-name table)))

(defrecord Table [table-name alt])
(defrecord Column [column-name relation-alias])
(defrecord Aggregate [function column rename])

(defprotocol IRelation
  (base [this])
  (alt [this])
  (sel [this])
  (proj [this])
  (joins [this])
  (intersection [this])
  (union* [this]))

(deftype Relation [table-or-relation relation-alias selection projection joins intersect-with union-with]
  IRelation
  clojure.lang.ILookup
  (base [_] table-or-relation)
  (alt [_] relation-alias)
  (sel [_] selection)
  (proj [_] projection)
  (joins [_] joins)
  (intersection [_] intersect-with)
  (union* [_] union-with)
  (valAt [_ k]
    (->Column (name k) relation-alias)))

(defn new-relation [table-name & [alt sel proj joins intersection union-]]
  (let [table (->Table table-name alt)]
    ;; (Relation. table (or alt (uniq-alias table)) sel proj (or joins {}) intersection union-)))
    (Relation. table alt sel proj (or joins {}) intersection union-)))

(defn project [relation columns]
  ;; (let [cols (map #(when-not (= (type %) Column) (->Column (name %) (alt relation))) columns)]
    (Relation. (base relation) (alt relation) (sel relation) columns (joins relation) (intersection relation) (union* relation)))


(defn select [relation predicate]
  (Relation. (base relation) (alt relation) (if (sel relation) [:and (sel relation) predicate] predicate) (proj relation) (joins relation) (intersection relation) (union* relation)))

(defn join [relation join-relation join-predicate]
  ;; TODO: what checks are required on sels and projs?
  (Relation. (base relation) (alt relation) (sel relation) (proj relation) {:relation join-relation
                                                                            :predicate join-predicate} (intersection relation) (union* relation)))
(defn rename [relation new-name]
  (Relation. (base relation) new-name (sel relation) (proj relation) (joins relation) (intersection relation) (union* relation)))

(defn intersect [r1 r2]
  (Relation. r1 nil nil nil {} r2 nil))

(defn union [r1 r2]
  (Relation. r1 nil nil nil {} nil r2))

(defn empty-relation? [r]
  (and (empty? (sel r)) (empty? (proj r)) (empty? (joins r))))

(defn contains-other-relations? [r]
  false)


(defn group-by-sql [relation]
  (let [cols     (proj relation)
        aggs     (filter #(= (type %) Aggregate) cols)
        non-aggs (filter #(= (type %) Column) cols)
        sql      (when-not (empty? aggs) (str " GROUP BY " (str/join ", " (map #(to-sql %) non-aggs))))]
  sql))

(defmulti to-sql type)
(defmethod to-sql String [s] (str "'" s "'"))
(defmethod to-sql Long [n] (str n))
(defmethod to-sql java.util.Date [d] (str "'" (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") d) "'"))
(defmethod to-sql clojure.lang.Keyword [word] (name word))
(defmethod to-sql Table [table]
  (let [table-name (:table-name table)
        alt (:alt table)]
  (str table-name (when-not (empty? alt) (str " " alt " ")))))
(defmethod to-sql Column [column]
  (let [relation-name (:relation-alias column)
        column-name   (:column-name column)]
    (str (when-not (empty? relation-name) (str relation-name ".")) column-name)))
(defmethod to-sql Aggregate [agg]
  (let [func   (name (:function agg))
        col    (to-sql (:column agg))
        rename (:rename agg)]
    (str func (wrap-parens col) " as " rename)))
(defmethod to-sql Relation [relation]
  (let [raw-cols (proj relation)
        al       (alt relation)
        cols     (if (empty? raw-cols) [(str (when-not (empty? al) (str al ".")) "*")] (map #(to-sql %) raw-cols))
        where    (sel relation)
        joins    (joins relation)
        intersection (intersection relation)
        union-   (union* relation)
        base               (str "(SELECT " (str/join ", " cols)
                                " FROM " (to-sql (base relation))
                                (when-not (empty? joins)
                                  (str " JOIN " (if (empty-relation? (:relation joins))
                                                    (to-sql (base (:relation joins)))
                                                    (to-sql (:relation joins)))
                                       " ON " (to-sql (:predicate joins))))
                                (when-not (empty? where) (str " WHERE " (to-sql where)))
                                (group-by-sql relation)
                                ")")
        intersected (when-not (nil? intersection) (wrap-parens (str base " INTERSECT " (to-sql intersection))))
        unioned     (when-not (nil? union-) (wrap-parens(str base " UNION " (to-sql union-))))]
    (str (or unioned intersected base) (when-not (empty? al) (str " " al)))))
(defmethod to-sql clojure.lang.PersistentVector [predicate]
  (let [operator (first predicate)
        operands (vec (rest predicate))]
    (if (contains? #{:and :or} operator)
      (wrap-parens (str/join (str " " (str/upper-case (name operator)) " ") (map #(to-sql %) operands)))
      (wrap-parens (str/join " " [(to-sql (first operands))
                                  (name operator)
                                  (to-sql (last operands))])))))

(defn max [col & rename]
  (->Aggregate :max col rename))

(def acad-prog-tbl (new-relation "ps_acad_prog_tbl"))
(def eff-keys-acad-prog-tbl (-> acad-prog-tbl
                                (project [(:institution acad-prog-tbl), (:acad_prog acad-prog-tbl), (max (:effdt acad-prog-tbl) "effdt")])
                                (select [:<= :effdt (Date.)])
                                (rename "poo")))

(def eff-acad-prog-tbl (-> acad-prog-tbl
                           (join eff-keys-acad-prog-tbl [:and [:= (:institution acad-prog-tbl) (:institution eff-keys-acad-prog-tbl)]
                                                              [:= (:acad_prog acad-prog-tbl) (:acad_prog eff-keys-acad-prog-tbl)]
                                                              [:= (:effdt acad-prog-tbl) (:effdt eff-keys-acad-prog-tbl)]])))

(to-sql eff-acad-prog-tbl)
