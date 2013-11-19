(ns playground.operations
  (:use

   [playground.mockdata]
   [cascalog.checkpoint]
   [clojure.tools.namespace.repl :only (refresh)]
   [cascalog.more-taps :only (lfs-delimited)]
   [playground.adult-dataset]


  )

  (:require [incanter.core :as i])

  )

;; let´s bootstrap playground
(bootstrap)


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

(defn write-out [tap]
  (<- [?line]
      (tap ?line)))

(defn my-workflow [path-to-the-data-file]
  (workflow ["temporary-folder"]
            X  ([:tmp-dirs [staging-X]]
                 (?- (lfs-delimited staging-X :delimiter ", " :sinkmode :replace)  (produce-X (my_source path-to-the-data-file)))
                 )
            A  ([:deps X :tmp-dirs [staging-A]]
                  (?- (lfs-delimited staging-A :sinkmode :replace) (produce-A (lfs-textline staging-X))))
            b ([:deps X :tmp-dirs [staging-b]]
                 (?- (lfs-delimited staging-b :sinkmode :replace) (produce-b (lfs-textline staging-X))) )
            write-out ([:deps [A b]]
                         (?- (lfs-delimited "A-matrix" :sinkmode :replace) (write-out (lfs-textline staging-A)))
                         (?- (lfs-delimited "b-vector" :sinkmode :replace) (write-out (lfs-textline staging-b))))))

;; (my-workflow "" "./outputDiCascalog")
