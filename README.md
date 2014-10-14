# rasql

A clojure library for converting relational algebra expressions to SQL.

## Usage

``` clojure
(def person (new-relation "ps_person"))
(to-sql (project person [:emplid :dob]))
;; "(SELECT emplid, dob FROM ps_person)"
```

## Compose queries with multiple relations

```clojure
(def acad-prog (new-relation "ps_acad_prog" "ap"))
(def inner-acad-prog (new-relation "ps_acad_prog"))

(def max-effdt-of-an-acad-prog
  (project
    (select inner-acad-prog
      [:and [:= (:acad_prog acad-prog) :acad_prog]
            [:= (:emplid acad-prog) :emplid]
            [:= (:stdnt_car_nbr acad-prog) :stdnt_car_nbr]
            [:<= (:effdt acad-prog) (java.util.Date.)]])
    [(max :effdt)]))

(def max-effseq-of-max-effdt-of-ps-acad_prog
  (project
    (select inner-acad-prog
      [:and [:= (:acad_prog acad-prog) :acad_prog]
            [:= (:emplid acad-prog) :emplid]
            [:= (:stdnt_car_nbr acad-prog) :stdnt_car_nbr]
            [:= (:effdt acad-prog) :effdt]])
    [(max :effseq)]))

(def effective-acad-progs
  (select acad-prog
          [:and [:= (:effdt acad-prog) max-effdt-of-an-acad-prog]
                [:= (:effseq acad-prog) max-effseq-of-max-effdt-of-ps-acad_prog]]))

(to-sql effective-acad-progs)
;; (SELECT * FROM ps_acad_prog ap WHERE ((ap.effdt = (SELECT max(effdt) FROM ps_acad_prog WHERE ((ap.acad_prog = acad_prog) AND (ap.emplid = emplid) AND (ap.stdnt_car_nbr = stdnt_car_nbr) AND (ap.effdt <= '2014-10-14')))) AND (ap.effseq = (SELECT max(effseq) FROM ps_acad_prog WHERE ((ap.acad_prog = acad_prog) AND (ap.emplid = emplid) AND (ap.stdnt_car_nbr = stdnt_car_nbr) AND (ap.effdt = effdt))))))
```

## License

Copyright © 2014 Chris Dinger

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
