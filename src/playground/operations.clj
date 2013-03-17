(ns playground.operations
  (:use [playground.mockdata]
        [cascalog.checkpoint]
        [cascalog.more-taps :only (lfs-delimited)])
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

(def prima-query (<- [?person] (age ?person 25)))
(def seconda-query (<- [?person] (person ?person)))
(def terza-query (<- [?persona] (person ?persona)))
(def quarta-query (<- [?col1 ?col2 ?col3] (mymatrix ?col1 ?col2 ?col3)))
(def quinta-query (<- [?col1 ?col2 ?col3] (mymatrix ?col1 ?col2 ?col3)))

(def query (<- [?tuple] (mymatrix :> ?a ?b ?c)
               (vector-mult ?a ?b ?c :> ?intermediate-matrix)
               (matrix-sum ?intermediate-matrix :> ?tuple)
               ) )

(defn my-workflow [input-path output-path]
  (workflow ["tmp"]
            only-step ([]
                          ;;(query mymatrix output-path)
                          (?- (lfs-delimited "madonna" :delimiter "k" :sinkmode :replace ) query )
                          )
            )
  )

;; (my-workflow "" "/home/catonano/Berlino/outputDiCascalog")



;; (?- (stdout) query)  riuscita !!



;;(def prova-output (lfs-tap :sink-template :TextDelimited"tmp/provaoutput/file"))



;; ora il primo workflow restituisce "true" ma dove minchia lo scrive il file ?
