(ns playground.operations
  (:use [playground.mockdata])
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

(defn coremult []
  (let [x [1 2 3]]
   (i/mmult x (i/trans x))
   )

)
 

(defmapcatop vector-mult [a b c]
  [[  [a b c] [1 2 3]  ]]
)

<<<<<<< HEAD
;;   (?<- (stdout) [?person] (age ?person 25)) 
;;   (?<- (stdout) [?person] (person ?person)) 
;;   (?<- (stdout) [?persona] (person ?persona)) 
;;   (?<- (stdout) [?col1 ?col2 ?col3] (mymatrix ?col1 ?col2 ?col3)) 
;;   (<- [?col1 ?col2 ?col3] (mymatrix ?col1 ?col2 ?col3))
 
(def query (<- [?tuple1 ?tuple2] (mymatrix :> ?a ?b ?c) (vector-mult ?a ?b ?c :> ?tuple1 ?tuple2)) )

=======
;;   (?<- (stdout) [?person] (age ?person 25))
;;   (?<- (stdout) [?person] (person ?person))
;;   (?<- (stdout) [?persona] (person ?persona))
;;   (?<- (stdout) [?col1 ?col2 ?col3] (mymatrix ?col1 ?col2 ?col3))
;;   (?<- (stdout) [?col1 ?col2 ?col3 ?output] (mymatrix ?col1 ?col2 ?col3) (dosum ?col1 :> ?output))
;;   (macroexpand-all '(?<- (stdout) [?col1 ?col2 ?col3 ?output] (mymatrix ?col1 ?col2 ?col3) (dosum ?col1 :> ?output)) )
(def query  (<- [?col1 ?col2 ?col3 ?output] (mymatrix ?col1 ?col2 ?col3) (dosum ?col1 :> ?output)))


;; my tuples are like [1 0 0] so identity receives 2 arguments but it expects one.
;;This is because the parallelagg doesn´t deal with tuples, it passes single values around (to init-var and to combine-var)
;;Here https://github.com/nathanmarz/cascalog/wiki/Guide-to-custom-operations
;;defmultibufferop and defparallelbuf are quoted and I hope those could be useful to me in this case
>>>>>>> b4933b36c0260abdc1459c24b648e8ebcb376d8f
