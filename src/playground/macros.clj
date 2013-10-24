(ns playground.macros
  (:use
        [clojure.tools.namespace.repl :only (refresh)]
       
  )

)


(defmacro lookup [lookup-key]
  `(lookup-proxy ~lookup-key
                 ~(symbol (str "?" (subs (str lookup-key) 1)))
                 ~(symbol ":>")
                 ~(symbol   (str  (subs  (str lookup-key) 1) "-out"))
                 )
  )

