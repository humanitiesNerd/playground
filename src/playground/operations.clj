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
 

;;   (?<- (stdout) [?person] (age ?person 25)) 
;;   (?<- (stdout) [?person] (person ?person)) 
;;   (?<- (stdout) [?persona] (person ?persona)) 
;;   (?<- (stdout) [?col1 ?col2 ?col3] (mymatrix ?col1 ?col2 ?col3)) 
;;   (<- [?col1 ?col2 ?col3] (mymatrix ?col1 ?col2 ?col3))
 


