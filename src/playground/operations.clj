(ns playground.operations
  (:use [playground.mockdata]
        [cascalog.checkpoint])
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
  (let [x vector]
   (i/mmult x (i/trans x))
   )
)


(defmapcatop vector-mult [a b c]
  [[ [ (coremult [a b c]) ] ]]
)


(def prima-query (<- [?person] (age ?person 25)))
(def seconda-query (<- [?person] (person ?person)))
(def terza-query (<- [?persona] (person ?persona)))
(def quarta-query (<- [?col1 ?col2 ?col3] (mymatrix ?col1 ?col2 ?col3)))
(def quinta-query (<- [?col1 ?col2 ?col3] (mymatrix ?col1 ?col2 ?col3)))

(def query (<- [?tuple] (mymatrix :> ?a ?b ?c) (vector-mult ?a ?b ?c :> ?tuple)) )

(defn my-workflow [input-path output-path]
  (workflow ["tmp"]
            only-step ([]
                          (query mymatrix output-path)
                          ;;(?- (lfs-textline "madonna" :sinkmode :replace ) query )
                          )
            )
  )

;; (my-workflow "" "/home/catonano/Berlino/outputDiCascalog")



;; (?- (stdout) query)  riuscita !!



;;(def prova-output (lfs-tap :sink-template :TextDelimited"tmp/provaoutput/file"))



;; ora il primo workflow restituisce "true" ma dove minchia lo scrive il file ?
