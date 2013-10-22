(ns playground.operations
  (:use
        [playground.mockdata]
        [cascalog.checkpoint]
        [clojure.tools.namespace.repl :only (refresh)]
        [cascalog.more-taps :only (lfs-delimited)]

  )

  (:require
            [incanter.core :as i]

            )
)

;; let´s bootstrap playground
(bootstrap)

(defn transpose
  "can I transpose a matrix with cascalog using incanter.core.trans ?"

  []
  (let [transposed (i/trans mymatrix)]
   transposed)
)

(defn coremult [vector]
  (i/mmult vector (i/trans vector))
  )

(defn coresum [matrix1 matrix2]
  (if (and (i/matrix? matrix1) (i/matrix? matrix2))
    (i/plus matrix1 matrix2)
    )
  ;;(i/matrix [[1 1 1] [1 1 1] [1 1 1]] )
  (i/plus (i/matrix matrix1) (i/matrix matrix2))
  )

(defparallelagg matrix-sum :init-var #'identity :combine-var #'coresum)

(defmapcatop vector-mult [a b c]
  [[   (coremult [a b c])  ]]
  )

(defmapcatop vector-mult-tupla-unica [a b c]
  [  [[  [a] [3]   ]]  ] ;;una tupla che contiene un vettore che contiene due vettori
  )

(defmapcatop vector-mult-seq-di-tuple [a b c]
  [ [a] [3] ] ;; seq di tuple (ogni tupla deve essere un vettore)
  )

;;(def prima-query (<- [?person] (age ?person 25)))
;;(def seconda-query (<- [?person] (person ?person)))
;;(def terza-query (<- [?persona] (person ?persona)))
;;(def quarta-query (<- [?col1 ?col2 ?col3] (mymatrix ?col1 ?col2 ?col3)))
;;((def quinta-query (<- [?col1 ?col2 ?col3] (mymatrix ?col1 ?col2 ?col3)))


;;39, State-gov, 77516, Bachelors, 13, Never-married, Adm-clerical, Not-in-family, White, Male, 2174, 0, 40, United-States, <=50K

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

(def my_source
       (lfs-delimited "adult/adult.data"
                                       :delimiter ", "
                                       :classes [Integer String Integer String Integer
                                                 String String String String String Integer
                                                 Integer Integer String String]
                                       :outfields ["?age" "?workclass" "?fnlwgt" "?education" "?education-num" "?marital-status"
                                                   "?occupation" "?relationship" "?race" "?sex" "?capital-gain" "?capital-loss"
                                                   "?hours-per-week" "?native-country" "?income-treshold"]
       )
)

;;  (?- (stdout) my_source)

(def query (<- [?tuple] (mymatrix :> ?a ?b ?c)
               (vector-mult ?a ?b ?c :> ?intermediate-matrix)
               (matrix-sum ?intermediate-matrix :> ?tuple)
               ) )

(def my_query (<- [?age ?workclass ?fnlwgt ?education ?education-num ?marital-status ?occupation ?relationship
                   ?race ?sex ?capital-gain ?capital-loss
                   ?hours-per-week ?native-country ?income-treshold ] (my_source :> ?age ?workclass ?fnlwgt ?education ?education-num
                                                                                 ?marital-status ?occupation ?relationship ?race
                                                                                 ?sex ?capital-gain ?capital-loss
                                                                                 ?hours-per-week ?native-country ?income-treshold)))




(defn my-workflow []
  (workflow ["tmp"]
            only-step ([]
                          ;;(query mymatrix output-path)
                          (?- (stdout ) my_query )
                          )
            )
  )

;; (my-workflow "" "./outputDiCascalog")



;; (?- (stdout) query)  riuscita !!



;;(def prova-output (lfs-tap :sink-template :TextDelimited"tmp/provaoutput/file"))



;; ora il primo workflow restituisce "true" ma dove minchia lo scrive il file ?
