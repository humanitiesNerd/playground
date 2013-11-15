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

(defn coremult2 [vector y]
  (i/mmult  y vector)
  )

(defn coresum [matrix1 matrix2]
  (if (and (i/matrix? matrix1) (i/matrix? matrix2))
    (i/plus matrix1 matrix2)
    )
  ;;(i/matrix [[1 1 1] [1 1 1] [1 1 1]] )
  (i/plus (i/matrix matrix1) (i/matrix matrix2))
  )

(defn tiles [tuples]
  (reduce + (map second tuples))
  )

(defn tiles2 [tiles]
  (let [
        value   (reduce + (map second
                               tiles))
        ]
    value
    )
  )


(def mockdata [
               [[1 1] "a" 1 1 1]
               [[2 1] "a" 2 1 3]
               [[3 1] "b" 1 1 2]
               ])

(defparallelagg matrix-sum :init-var #'identity :combine-var #'coresum)

(defbufferop collect-tiles [tuples]
  [(tiles tuples)]
  )

;(defmapcatop vector-mult [a b c d e f g h i l m n o p]
;  [[   (coremult [a b c d e f g h i l m n o p])  ]]
;)

(defmapcatop vector-mult-tupla-unica [a b c]
  [  [[  [a] [b] [c]  ]]  ] ;;una tupla che contiene un vettore che contiene due vettori
  )

(defmapcatop vector-mult-seq-di-tuple [a b c d]
  [ [a] [b] [c] [d]] ;; seq di tuple (ogni tupla deve essere un vettore)
  )

(defmapcatop split [linenumber a b c d]
  [

   [[1 1] "a" 1 linenumber a]
   [[2 1] "a" 2 linenumber b]
   [[3 1] "a" 3 linenumber c]
   [[1 1] "b" linenumber 1 d]
   [[2 1] "b" linenumber 1 d]
   [[3 1] "b" linenumber 1 d]

   ]

  )

(def query4 (<- [?index ?cell]
                (mymatrix :> ?linenumber ?a ?b ?c)
                (mycolumnvector :> ?linenumber ?d)
                (split ?linenumber ?a ?b ?c ?d  :> ?index ?from-matrix ?row ?column ?value)
                (collect-tiles ?from-matrix ?value :>  ?cell )
                )
  )

 (def test-tap [["a" "b" 1]
                 ["b" "c" 2]
                 ["a" "d" 3]])

(defbufferop dosum [tuples] [(reduce + (map second tuples))])

(def query5 (<- [?a ?sum] (test-tap ?a ?b ?c) (dosum ?b ?c :> ?sum))
  )

;;(def prima-query (<- [?person] (age ?person 25)))
;;(def seconda-query (<- [?person] (person ?person)))

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

; not really needed!

;(defn source-A [path-to-the-data-file]
;      (lfs-delimited path-to-the-data-file
;                                       :delimiter ", "
;                                       :classes [Integer Integer Integer Integer Integer Integer Integer
;                                                 Integer Integer Integer Integer Integer Integer Integer
;                                                 Integer]
;                                       :outfields ["?linenumber" "?age" "?workclass" "?fnlwgt" "?education" "?education-num" "?marital-status"
;                                                   "?occupation" "?relationship" "?race" "?sex" "?capital-gain" "?capital-loss"
;                                                   "?hours-per-week" "?native-country" ]
;       )
;)


;;  (?- (stdout) my_source)

;(def query (<- [?tuple] (mymatrix :> ?a ?b ?c)
;               (vector-mult ?a ?b ?c :> ?intermediate-matrix)
;               (matrix-sum ?intermediate-matrix :> ?tuple)
;               )
;  )

(def query2 (<- [?tupla] (mymatrix :> ?a ?b ?c)
                (vector-mult-tupla-unica ?a ?b ?c :> ?tupla)))

(def query3 (<- [?tupla] (mymatrix :> ?a ?b ?c)
                (vector-mult-seq-di-tuple ?a ?b ?c :> ?tupla)))

;(defn workflow-demergenza []
;  (workflow ["tmp"]
;            only-step ([]
;                         (?- (stdout) query))))

;;(def mockquery
;;  (<- [!input !id]
;;     (input !input)
;;     ;;(get-in lookup-table [!input] :> !id)
;;     ;;(get lookup-table !input :> id)
;;     (lookup-proxy lookup-table !input :> !id)
;;  )
;;)

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
      ;;(lookup :workclass)
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

(defn produce-A [tap]
  (<- [?final-matrix]
      (tap ?line)
      (vectormult ?line :> ?intermediate-matrix)
      (matrix-sum ?intermediate-matrix :> ?final-matrix)
      )
  )

(defn produce-b [tap-a tap-y]
   (<- [?b]
       (tap-a ?age ?workclass ?fnlwgt ?education ?education-num
              ?marital-status ?occupation ?relationship ?race
              ?sex ?capital-gain ?capital-loss
              ?hours-per-week ?native-country)
       ;(tap-y ?)
       ))


(defn my-workflow [path-to-the-data-file]
  (workflow ["temporary-folder"]
            X ([:tmp-dirs [staging-X]]
                 (?- (lfs-delimited staging-X :delimiter ", " :sinkmode :replace)  (produce-X (my_source path-to-the-data-file)))
                 )
            y ([:tmp-dirs [staging-y]]
                 (?- (lfs-delimited staging-y :sinkmode :replace) (produce-y (my_source path-to-the-data-file)) ))

           A  ([:deps X :tmp-dirs [staging-A]]
               (?- (lfs-delimited staging-A :sinkmode :replace) (produce-A (lfs-textline staging-X)))
               )

            )
  )

;; (my-workflow "" "./outputDiCascalog")



;; (?- (stdout) query)  riuscita !!



;;(def prova-output (lfs-tap :sink-template :TextDelimited"tmp/provaoutput/file"))



;; ora il primo workflow restituisce "true" ma dove minchia lo scrive il file ?
