(ns playground.operations
  (:use
        [playground.mockdata]
        [cascalog.checkpoint]
        [clojure.tools.namespace.repl :only (refresh)]
        [cascalog.more-taps :only (lfs-delimited)]
        [playground.macros]


  )

  (:require [incanter.core :as i])

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
  (let [without-y (subvec vector 0 (- (count vector) 1))]
    (i/mmult without-y (i/trans without-y)))
  )

(defn coremult2 [vector]
  (let [y (peek vector)
        without-y (subvec vector 0 (- (count vector) 1))]
    (i/mult y (i/matrix  without-y))))

(defn coresum [matrix1 matrix2]
  (if (and (i/matrix? matrix1) (i/matrix? matrix2))
    (i/plus matrix1 matrix2)
    )
  ;;(i/matrix [[1 1 1] [1 1 1] [1 1 1]] )
  (i/plus (i/matrix matrix1) (i/matrix matrix2))
  )

(defparallelagg matrix-sum :init-var #'identity :combine-var #'coresum)

(defbufferop dosum [tuples] [(reduce + (map second tuples))])

;;39, State-gov, 77516, Bachelors, 13, Never-married, Adm-clerical, Not-in-family, White, Male, 2174, 0, 40, United-States, <=50K

(defn my_source [path-to-the-data-file]
       (lfs-delimited path-to-the-data-file
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
(defn extract-y [income-treshold]
  (if (= income-treshold "<= 50k")
    0
    1))


(defn produce-X [data-source-tap]
  (<- [
       ?age
       ?workclass-out
       ?fnlwgt
       ?education-out
       ?education-num
       ?marital-status-out
       ?occupation-out
       ?relationship-out
       ?race-out
       ?sex-out
       ?capital-gain
       ?capital-loss
       ?hours-per-week
       ?native-country-out
       ?income-treshold-out
       ]
      (data-source-tap ?age ?workclass ?fnlwgt ?education ?education-num
                       ?marital-status ?occupation ?relationship ?race
                       ?sex ?capital-gain ?capital-loss
                       ?hours-per-week ?native-country ?income-treshold)
      (convert-to-numbers  :workclass ?workclass :> ?workclass-out)
      (convert-to-numbers  :education ?education :> ?education-out)
      (convert-to-numbers  :marital-status ?marital-status :> ?marital-status-out)
      (convert-to-numbers  :occupation ?occupation :> ?occupation-out)
      (convert-to-numbers  :relationship ?relationship :> ?relationship-out)
      (convert-to-numbers  :race ?race :> ?race-out)
      (convert-to-numbers  :sex ?sex :> ?sex-out)
      (convert-to-numbers  :native-country ?native-country :> ?native-country-out)
      (extract-y ?income-treshold :> ?income-treshold-out)
   )
  )


(defn to-int-vector [line]
  (vec (map #(Integer/parseInt %) (clojure.string/split line #", ")))
)

(defmapcatop vectormult [line]
  [[(coremult (to-int-vector line))]]
)

(defmapcatop vectormult2 [line]
  [[(coremult2 (to-int-vector line))]]
)


(defn produce-A [tap]
  (<- [?final-matrix]
      (tap ?line)
      (vectormult ?line :> ?intermediate-matrix)
      (matrix-sum ?intermediate-matrix :> ?final-matrix)
      )
  )

(defn produce-b [tap]
   (<- [?final-vector]
       (tap ?line)
       (vectormult2 ?line :> ?intermediate-vector)
       (matrix-sum ?intermediate-vector :> ?final-vector)
       ))


(defn my-workflow [path-to-the-data-file]
  (workflow ["temporary-folder"]
            X  ([:tmp-dirs [staging-X]]
                 (?- (lfs-delimited staging-X :delimiter ", " :sinkmode :replace)  (produce-X (my_source path-to-the-data-file)))
                 )
            A  ([:deps X :tmp-dirs [staging-A]]
                  (?- (lfs-delimited staging-A :sinkmode :replace) (produce-A (lfs-textline staging-X))))
            b ([:deps X :tmp-dirs [staging-b]]
                (?- (lfs-delimited staging-b :sinkmode :replace) (produce-b (lfs-textline staging-X))) )))

;; (my-workflow "" "./outputDiCascalog")
