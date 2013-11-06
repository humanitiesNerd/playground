(ns playground.macros
  (:use
        [clojure.tools.namespace.repl :only (refresh)]

  )

)


(def from-strings-to-numbers
  {:workclass
   {"?" 2 "Private" 2 "Self-emp-not-inc" 3 "Self-emp-inc" 4
    "Federal-gov" 5 "Local-gov" 6 "State-gov" 7 "Without-pay" 8 "Never-worked" 9 }
   :education {"?" 2 "Bachelors" 3 "Some-college" 4 "11th" 5
               "HS-grad" 6 "Prof-school" 7 "Assoc-acdm" 8 "Assoc-voc" 9
               "9th" 10 "7th-8th" 11 "12th" 12 "Masters" 14 "1st-4th" 15 "10th" 16
               "Doctorate" 17 "5th-6th" 18 "Preschool" 19}
   :marital-status {"?" 2 "Married-civ-spouse" 3 "Divorced" 4 "Never-married" 5 "Separated" 6
                    "Widowed" 7 "Married-spouse-absent" 8 "Married-AF-spouse" 9}
   :occupation {"?" 2 "Tech-support" 3 "Craft-repair" 4 "Other-service" 5 "Sales" 6 "Exec-managerial" 7
                "Prof-specialty" 8 "Handlers-cleaners" 9 "Machine-op-inspct" 10 "Adm-clerical" 11
                "Farming-fishing" 12 "Transport-moving" 13 "Priv-house-serv" 14 "Protective-serv" 15 "Armed-Forces" 16}
   :relationship {"?" 2 "Wife" 3 "Own-child" 4 "Husband" 5 "Not-in-family" 6 "Other-relative" 7 "Unmarried" 8}
   :race {"?" 2 "White" 3 "Asian-Pac-Islander" 4 "Amer-Indian-Eskimo" 5 "Other" 6 "Black" 7}
   :sex {"?" 2 "Female" 3 "Male" 4}
   :native-country {"?" 2 "United-States" 3 "Cambodia" 4 "England" 5 "Puerto-Rico" 6
                    "Canada" 7
                    "Germany" 8
                    "Outlying-US(Guam-USVI-etc)" 9
                    "India" 10
                    "Japan" 11 "Greece" 12
                    "South" 13 "China" 14
                    "Cuba" 15 "Iran" 16
                    "Honduras" 17 "Philippines" 18 "Italy" 19
                    "Poland" 20 "Jamaica" 21 "Vietnam" 22
                    "Mexico" 23 "Portugal" 24
                    "Ireland" 25 "France" 26
                    "Dominican-Republic" 27 "Laos" 28 "Ecuador" 29 "Taiwan" 30
                    "Haiti" 31 "Columbia" 32
                    "Hungary" 33 "Guatemala" 34
                    "Nicaragua" 35 "Scotland" 36 "Thailand" 37 "Yugoslavia" 38
                    "El-Salvador" 39
                    "Trinadad&Tobago" 40 "Peru" 41 "Hong" 42
                    "Holand-Netherlands" 43}
   }
  )


(defn convert-to-numbers [& lookup-keys]
  (get-in from-strings-to-numbers (into [] lookup-keys))
  )

(defn extract-y [income-treshold]
  (if (= income-treshold "<= 50k")
    0
    1)
  )

(defmacro lookup [lookup-key]
  `(lookup-proxy ~lookup-key
                 ~(symbol (str "?" (subs (str lookup-key) 1)))
                 ~(symbol ":>")
                 ~(symbol   (str  (subs  (str lookup-key) 1) "-out"))
                 )
  )
