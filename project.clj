(defproject prova_cascalog "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [
                [org.clojure/clojure "1.5.0"]
                [cascalog "1.10.2"]
                [cascalog/cascalog-more-taps "1.10.2"]
                [incanter/incanter-core "1.3.0"]
                [org.clojure/tools.namespace "0.2.4"]
                [org.apache.hadoop/hadoop-core "1.0.3"]
                ]
  :profiles { :dev
                  {:dependencies [
                                 [org.apache.hadoop/hadoop-core "1.0.3"
                                 :exclusions [
                                            
                                             ]
                                 ]
                                 ]
                  }

  }
)
