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

(defmacro defrelation [relation-name base]
  `(def ~relation-name (Relation. ~base (if (= (type ~base) String) ~base (unnamed-relation-alias ~base)) (name '~relation-name) nil (->Projection []) nil)))

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

(defn maximum [column rename]
  (->Aggregate :max column rename))

;;;;;; SQL

(defn wrap-parens [s]
  (str "(" s ")"))

(defn group-by-sql [p]
  (let [cols (:columns p)
        aggs (filter #(= (type %) Aggregate) cols)
        non-aggs (filter #(= (type %) Column) cols)
        non-aggs-sql (map #(to-sql %) non-aggs)
        sql (when-not (empty? aggs) (str " GROUP BY " (str/join ", " non-aggs-sql)))]
    sql))

(defmulti to-sql type)

(defmethod to-sql nil [_])

(defmethod to-sql clojure.lang.Keyword [k]
  (name k))

(defmethod to-sql String [s]
  (str "'" s "'"))

(defmethod to-sql Column [c]
  (let [column (:column c)
        relation-alias (relation-alias (:relation c))]
    (str relation-alias "." column)))

(defmethod to-sql Aggregate [a]
  (let [function (name (:function a))
        column (to-sql (:column a))
        rename (to-sql (:rename a))]
  (str function (wrap-parens column) " AS \"" rename "\"")))

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
    (str " JOIN " (to-sql relation) " " relation-alias " ON " (to-sql predicate))))

(defmethod to-sql String [s]
  s)

(defmethod to-sql Relation [relation]
  (let [base (base relation)
        projection (projection relation)
        selection (selection relation)
        columns (or (to-sql projection) "*")
        joins (joins relation)
        alias (relation-alias relation) ;; (when (= (type base) Relation) (str " " (relation-alias base)))
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

;;;;;;;;;;;;;;;;;;;;

(defrelation acad-prog "ps_acad_prog")

(to-sql (project (project acad-prog [(:emplid acad-prog) (:acad_career acad-prog)]) [(:* acad-prog)]))


(defrelation eff-names "ps_names")
(defrelation eff-blah eff-names)
(defrelation eff-whoa eff-blah)


(to-sql eff-names)
(to-sql (project eff-blah [(:id eff-blah)]))
(to-sql eff-blah)
(to-sql eff-whoa)

(to-sql (select eff-blah [:or [:= (:id eff-blah) "123"]
                              [:= (:id eff-blah) "456"]]))

(to-sql (Projection. [(:id eff-blah)]))
(to-sql (Projection. [(:effdt eff-blah)]))
(to-sql (Projection. [(:id eff-names)]))
(to-sql (Projection. [(:* eff-names)]))

(to-sql (Join. eff-names [:= (:id eff-names) (:id eff-blah)]))
(relation-alias eff-blah)
(base-alias eff-blah)

;; (select eff-blah [:= (:id eff-blah) (:id eff-names)])

(to-sql [:= (:id eff-blah) (:id eff-names)])
(to-sql [:or [:= (:id eff-blah) "12345"]
             [:= (:id eff-blah) "52372"]
             [:= (:id eff-blah) "239746"]])

(defrelation ps-acad-prog  "ps_acad_prog")
(defrelation max-effdt-of-each-acad-prog
  (-> ps-acad-prog
      (project [(:emplid ps-acad-prog)
                (:acad_career ps-acad-prog)
                (:stndt_car_nbr ps-acad-prog)
                (maximum (:effdt ps-acad-prog) "effdt")])
      (select [:<= (:effdt ps-acad-prog) "SYSDATE"])))
(to-sql max-effdt-of-each-acad-prog)
(defrelation max-effdt-and-effseq-of-each-acad-prog
  (project max-effdt-of-each-acad-prog [(:emplid max-effdt-of-each-acad-prog)
                                        (:acad_career max-effdt-of-each-acad-prog)
                                        (:stndt_car_nbr max-effdt-of-each-acad-prog)
                                        (:effdt max-effdt-of-each-acad-prog)
                                        (maximum (:effseq max-effdt-of-each-acad-prog) "effseq")]))
(to-sql max-effdt-and-effseq-of-each-acad-prog)


(defrelation emplid-and-max-effdt (project acad-prog [(:emplid acad-prog) (maximum (:effdt acad-prog) "max_effdt")]))
(to-sql emplid-and-max-effdt)
