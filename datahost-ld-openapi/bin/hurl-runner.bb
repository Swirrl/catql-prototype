(ns hurl-runner
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.pprint :as pp]
            [babashka.fs :as fs]
            [babashka.cli :as cli]
            [tpximpact.datahost.ldapi.test-util.hurl :refer [run-directory success?]])
  (:import [java.nio.file Path Files]))

(def cli-options {:dir {:coerce :string}
                  :variable {:coerce []}
                  :report-junit {:coerce :string}
                  :report-html {:coerce :string}})

(defn- transform-cli-variables
  [vars]
  (into {} (map #(vec (.split % "="))) vars))

(defn main [cmd-line-args]
  (let [options {:spec cli-options :require [:dir]
                 :error-fn (fn [{:keys [spec type cause msg option] :as data}]
                             (if (= :org.babashka/cli type)
                               (case cause
                                 :require
                                 (println
                                  (format "Missing required argument:\n%s\n"
                                          (cli/format-opts {:spec (select-keys cli-options [option])})))
                                 (println msg))
                               (throw (ex-info msg data)))
                             (System/exit 1))}
        _ (cli/parse-args cmd-line-args options)
        {:keys [variable] :as opts} (cli/parse-opts cmd-line-args
                                                    {:spec cli-options
                                                     :require [:dir]})]
    (run-directory (:dir opts)
                   (-> opts
                       (dissoc :variable)
                       (assoc :variables (transform-cli-variables variable))))))

(binding [pp/*print-right-margin* 200]
  (let [results (main *command-line-args*)]
    (doseq [result results]
      (do
        (println "🚀 running: " (:script result))
        (pp/pprint (dissoc result :err :script))
        (println "--- stderr ---")
        (println (:err result))))
    (when-not (success? results)
      (println "🙅‍♂️ there were errors")
      (System/exit 2))
    (println " 👍 all good")))
