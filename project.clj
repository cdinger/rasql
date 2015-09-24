(defproject com.houseofding/rasql "0.1.0"
  :description "A library for converting relational algebra expressions to SQL"
  :url "https://github.umn.edu/ding0057/rasql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/clojurescript "1.7.122" :classifier "aot"]]

  :plugins [[lein-cljsbuild "1.1.0"]]

  :cljsbuild {
    :builds [{:id "test"
              :source-paths ["src" "test"]
              :compiler {
                :main rasql.core-test
                :output-to "out/core-test.js"
                :output-dir "out"
                :optimizations :none
                :target :nodejs
                :cache-analysis true
                :source-map true}}
             ]})

  ; :cljsbuild {:builds {:test {:source-paths ["test"]
  ;                           :compiler {:output-to "resources/test/compiled.js"
  ;                                      :optimizations :none
  ;                                      :target :nodejs}}}
  ;           :test-commands {"test" ["jrunscript" "-f" "resources/test/compiled.js"]}})
