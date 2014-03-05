(defproject prova_cascalog "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [
                [org.clojure/clojure "1.5.1"]
                [cascalog/cascalog-core "2.0.0"]
                [cascalog/cascalog-more-taps "2.0.0" ]
                [cascalog/cascalog-checkpoint "2.0.0"]
                [cascalog/midje-cascalog "2.0.0"]
                [com.netflix.pigpen/pigpen "0.2.0"]
                [incanter/incanter-core "1.3.0"]
                [org.clojure/tools.namespace "0.2.4"]
                [matchure "0.10.1"]
                ]
  :profiles { :dev
                  {:dependencies [

                                 [org.apache.hadoop/hadoop-core "1.1.2"]
                                 [org.apache.pig/pig "0.11.1"]
                                 ]
                   }


             }
    :repositories {"conjars" "http://conjars.org/repo"}
)
