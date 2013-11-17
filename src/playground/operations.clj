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

(def a-columns 231)

(defn transpose
  "can I transpose a matrix with cascalog using incanter.core.trans ?"

  []
  (let [transposed (i/trans mymatrix)]
   transposed)
)

(defn coremult [vector]
  (i/mmult vector (i/trans vector))
  )

(defn coremult2 [vector]
  (let [y (peek vector)]
    (i/mult y vector))
  )

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
   )
  )

(defn extract-y [income-treshold]
  (if (= income-treshold "<= 50k")
    0
    1))

(defn produce-y [data-source-tap]
  (<- [?y]
      ((select-fields data-source-tap ["?income-treshold"]) ?income-treshold)
      (extract-y ?income-treshold :> ?y)
      ))

(defn to-int-vector [line]
  (map #(Integer/parseInt %) (clojure.string/split line #", "))
)

(defmapcatop vectormult [line]
  [[(coremult (to-int-vector line))]]
)

(defmapcatop vectormult2 [line number]
  [[(coremult2 (to-int-vector line) number)]]
)


(defn produce-A [tap]
  (<- [?final-matrix]
      (tap ?line)
      (vectormult ?line :> ?intermediate-matrix)
      (matrix-sum ?intermediate-matrix :> ?final-matrix)
      )
  )

(defn produce-b [tap-x tap-y]
   (<- [?final-vector]
       (tap-x ?line)
       ;(tap-y ?y)
       (vectormult2 ?line :> ?intermediate-vector)
       (matrix-sum ?intermediate-vector :> ?final-vector)
       ))


(defn my-workflow [path-to-the-data-file]
  (workflow ["temporary-folder"]
            X  ([:tmp-dirs [staging-X]]
                 (?- (lfs-delimited staging-X :delimiter ", " :sinkmode :replace)  (produce-X (my_source path-to-the-data-file)))
                 )
            y  ([:tmp-dirs [staging-y]]
                 (?- (lfs-delimited staging-y :sinkmode :replace) (produce-y (my_source path-to-the-data-file)) ))

            A  ([:deps X :tmp-dirs [staging-A]]
               (?- (lfs-delimited staging-A :sinkmode :replace) (produce-A (lfs-textline staging-X)))
               )
            b  ([:deps [A y] :tmp-dirs [staging-b]]
                (?- (lfs-delimited staging-b :sinkmode :replace) (produce-b (lfs-textline staging-A) (lfs-textline staging-y))))
            )
  )

;; (my-workflow "" "./outputDiCascalog")
