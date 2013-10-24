(ns playground.operations
  (:use
        [playground.mockdata]
        [cascalog.checkpoint]
        [clojure.tools.namespace.repl :only (refresh)]
        [cascalog.more-taps :only (lfs-delimited)]
        [playground.macros]

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


(def lookup-table
  {"US" 1
   "UK" 2
   "France" 3})

(def input [["US"] ["US"] ["UK"] ["UK"] ["France"]])

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


;;(def mockquery
;;  (<- [!input !id]
;;     (input !input)
;;     ;;(get-in lookup-table [!input] :> !id)
;;     ;;(get lookup-table !input :> id)
;;     (lookup-proxy lookup-table !input :> !id)
;;  )
;;)

(defn convert-to-numbers [data-source-tap]
  (<- [?age
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
      (data-source-tap  ?age ?workclass ?fnlwgt ?education ?education-num
                       ?marital-status ?occupation ?relationship ?race
                       ?sex ?capital-gain ?capital-loss
                       ?hours-per-week ?native-country ?income-treshold)
      (lookup-proxy from-strings-to-numbers :workclass ?workclass :> ?workclass-out)
      ;;(lookup :workclass)
      (lookup-proxy from-strings-to-numbers :education ?education :> ?education-out)
      (lookup-proxy from-strings-to-numbers :marital-status ?marital-status :> ?marital-status-out)
      (lookup-proxy from-strings-to-numbers :occupation ?occupation :> ?occupation-out)
      (lookup-proxy from-strings-to-numbers :relationship ?relationship :> ?relationship-out)
      (lookup-proxy from-strings-to-numbers :race ?race :> ?race-out)
      (lookup-proxy from-strings-to-numbers :sex ?sex :> ?sex-out)
      (lookup-proxy from-strings-to-numbers :native-country ?native-country :> ?native-country-out)
   )
)

(defn my-workflow []
  (workflow ["tmp"]
            only-step ([]
                         (?- (stdout ) (convert-to-numbers my_source) )
                        ;; (?- (stdout) mockquery)
                          )
            )
  )

;; (my-workflow "" "./outputDiCascalog")



;; (?- (stdout) query)  riuscita !!



;;(def prova-output (lfs-tap :sink-template :TextDelimited"tmp/provaoutput/file"))



;; ora il primo workflow restituisce "true" ma dove minchia lo scrive il file ?
