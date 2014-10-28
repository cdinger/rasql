;; # Core principles
;;
;; 1. every relation must have a name
;;   - if you don't provide one, a nasty but unique one will be created
;; 2. RA-focused; this is not a DSL for SQL.
;;
(ns rasql.core
  (:require [clojure.string :as str]))

(defn unnamed-relation-alias [relation]
  (str "unnamed-relation-" (System/identityHashCode relation)))

(defprotocol IRelation
  (base [this])
  (base-alias [this])
  (relation-alias [this])
  (sel [this])
  (projection [this])
  (joins [this])
  (intersection [this])
  (union* [this]))

(defrecord Column [relation column])
(defrecord Join [relation])
(defrecord Projection [columns])
;; (defrecord Predicate [conditions])

(deftype Relation [base base-alias relation-alias selection projection joins intersect-with union-with]
  IRelation
  clojure.lang.ILookup
  (base [_] base)
  (base-alias [this] (or base-alias (when (= (type base) String) base (unnamed-relation-alias base))))
  (relation-alias [_] relation-alias)
  (sel [_] selection)
  (projection [_] projection)
  (joins [_] joins)
  (intersection [_] intersect-with)
  (union* [_] union-with)
  (valAt [this k]
    (->Column this (name k))))

(defmacro defrelation [relation-name base]
  `(def ~relation-name (Relation. ~base (if (= (type ~base) String) ~base (unnamed-relation-alias ~base)) (name '~relation-name) nil (->Projection []) nil nil nil)))

;;;;;; convenience methods

(defn project [relation columns]
  (map #(:relation %) columns)
  (->Projection columns))

;; (defn select [relation raw-predicate]
;;   (->Predicate (map #(->Predicate %) raw-predicate)))

;;;;;; SQL

(defn wrap-parens [s]
  (str "(" s ")"))

(defmulti to-sql type)

(defmethod to-sql nil [_])

(defmethod to-sql clojure.lang.Keyword [k]
  (name k))

(defmethod to-sql String [s]
  (str "'" s "'"))

(defmethod to-sql Column [c]
  (let [column (:column c)
        base-alias (base-alias (:relation c))]
    (str base-alias "." column)))

(defmethod to-sql Projection [p]
  (let [raw-columns (:columns p)
        columns (str/join ", " (map #(to-sql %) raw-columns))
        sql-columns (if (empty? raw-columns) "*" columns)
        sql (str "SELECT " sql-columns)]
    sql))

(defmethod to-sql clojure.lang.PersistentVector [p]
  (let [operator (first p)
        operands (rest p)]
    (if (contains? #{:and :or} operator)
      (wrap-parens (str/join (str " " (str/upper-case (name operator)) " ") (map #(to-sql %) operands)))
      (wrap-parens (str/join " " [(to-sql (first operands))
                                  (name operator)
                                  (to-sql (last operands))])))))

;; (defmethod to-sql Predicate [p]
;;   (let [conditions (:conditions p)
;;         operator (first conditions)
;;         operands (rest conditions)]
;;     (if (contains? #{:and :or} operator)
;;       (wrap-parens (str/join (str " " (str/upper-case (name operator)) " ") (map #(to-sql %) operands)))
;;       (wrap-parens (str/join " " [(to-sql (first operands))
;;                                   (name operator)
;;                                   (to-sql (last operands))])))))

(defmethod to-sql Join [j]
  (let [relation (:relation j)
        relation-alias (relation-alias relation)]
    (str " JOIN " (to-sql relation) " " relation-alias)))

(defmethod to-sql String [s]
  s)

(defmethod to-sql Relation [relation]
  (let [base (base relation)
        projection (projection relation)
        columns (or (to-sql projection) "*")
        joins (joins relation)
        alias (when (= (type base) Relation) (str " " (relation-alias base)))]
    (str "(" (to-sql projection)
         " FROM " (to-sql base) alias
         (to-sql joins) ")")))

;;;;;;;;;;;;;;;;;;;;

(defrelation eff-names "ps_names")
(defrelation eff-blah eff-names)

(to-sql eff-names)
(to-sql eff-blah)

(to-sql (Projection. [(:id eff-blah)]))
(to-sql (Projection. [(:effdt eff-blah)]))
(to-sql (Projection. [(:id eff-names)]))
(to-sql (Projection. [(:* eff-names)]))

(to-sql (Join. eff-names))
(to-sql (Join. eff-blah))

(select eff-blah [:= (:id eff-blah) (:id eff-names)])

(to-sql [:= (:id eff-blah) (:id eff-names)])
(to-sql [:or [:= (:id eff-blah) "12345"]
             [:= (:id eff-blah) "52372"]
             [:= (:id eff-blah) "239746"]])
