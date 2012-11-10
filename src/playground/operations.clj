(ns playground.operations
  (:use [playground.mockdata])
  (:require 
            [incanter.core :as i]
            ))

;; let´s bootstrap playground
(bootstrap)

(defn transpose
  "can I transpose a matrix with cascalog using incanter.core.trans ?"

  []
  (let [transposed (i/trans mymatrix)]
   transposed)
) 
