(defproject prova_cascalog "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                [org.clojure/clojure "1.4.0"] 
                [cascalog "1.10.0"]
                [incanter/incanter-core "1.3.0"]
                ]
  :profiles { :dev 
                  {:dependencies [
                                 [org.apache.hadoop/hadoop-core "0.20.2-dev" 
                                 :exclusions [
                                             [org.slf4j/slf4j-api] 
                                             [org.slf4j/slf4j-log4j12] 
                                             [log4j]
                                             [commons-logging]
                                             commons-codec
                                             ]
                                 ]            
                                 ]
                  }

  })


