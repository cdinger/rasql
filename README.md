# rasql

A clojure library for converting relational algebra expressions to SQL.

## Usage

``` clojure
(def person (Relation. "ps_person"))
(to-sql (project person [:emplid :dob]))
;; "(SELECT emplid, dob FROM ps_person)"
```

## Compose queries with multiple relations

```clojure
(def outer-acad-prog {:base-relation "ps_acad_prog" :alias "ap"})
(def acad-prog {:base-relation "ps_acad_prog"})

(def max-effdt-of-outer-acad-prog
  (project
    (select acad-prog (and (= (:emplid acad-prog) (:emplid outer-acad-prog))
                           (= (:acad_career acad-prog) (:acad_career outer-acad-prog))
                           (= (:stdnt_car_nbr acad-prog) (:stndt_car_nbr outer-acad-prog))
                           (<= (:effdt acad-prog) Date.)))
    [(max :effdt)]))
(def max-effseq-of-max-effdt-of-ps-acad_prog
  (project
    (select acad-prog (and (= (:emplid acad-prog) (:emplid outer-acad-prog))
                           (= (:acad_career acad-prog) (:acad_career outer-acad-prog))
                           (= (:stdnt_car_nbr acad-prog) (:stndt_car_nbr outer-acad-prog))
                           (= (:effdt acad-prog) (:effdt outer-acad-prog))))
    [(max :effseq)]))
(def effectve-acad-progs
  (select outer-acad-prog (and (= :effdt max-effdt-of-outer-acad-prog)
                               (= :effseq max-effseq-of-max-effdt-of-ps-acad_prog))))

(select effective-acad-progs (= (:emplid outer-acad-prog) "1234567"))
```

## License

Copyright © 2014 Chris Dinger

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
