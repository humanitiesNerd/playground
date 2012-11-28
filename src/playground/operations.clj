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

(defn my-identity [first-argument second-argument]
  [first-argument second-argument]
)

(defparallelagg dosum :init-var #'my-identity :combine-var #'identity)


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
