(ns tpximpact.test-helpers
  (:require [clojure.data :as c.data]
            [clojure.test :refer :all]
            [integrant.core :as ig]
            [tpximpact.db-cleaner :as dc]
            [tpximpact.datahost.sys :as sys]
            [tpximpact.datahost.ldapi.test-util.http-client :as http-client]))

(defmethod ig/init-key :tpximpact.datahost.ldapi.test/http-client [_ {:keys [port] :as config}]
  (http-client/make-client config))

(defn start-system [configs]
  (-> configs
      (sys/load-configs)
      (sys/prep-config)
      (ig/init)))

(def ^:dynamic *system* (atom nil))

(defn start-system
  ([]
   (start-system ["ldapi/base-system.edn"
                  ;; TODO - nuke test-system & move contents to env.edn
                  "test-system.edn"
                  ;; env.edn contains environment specific
                  ;; overrides to the base-system.edn and
                  ;; is set on classpath depending on env.
                  "ldapi/env.edn"]))
  ([configs]
   (let [sys (-> configs
                 (sys/load-configs)
                 (sys/prep-config)
                 (ig/init))]
     (reset! *system* sys))))

(defn stop-system
  []
  (let [sys @*system*]
    (reset! *system* nil)
    (ig/halt! sys)))

(defn with-system-fixture [test-fn]
  (start-system)
  (test-fn)
  (stop-system))

(defmacro with-system* [{:keys [on-halt on-init] :as _opts} system-binding & body]
  `(let [sys# (start-system)
         ~system-binding sys#]
     (try
       (doseq [f!# ~on-init]
         (f!# sys#))
       ~@body
       (finally
         (doseq [f!# ~on-halt]
           (f!# sys#))))))

(defmacro with-system [system-bindings system-loader & body]
  `(with-system* {:on-halt [ig/halt!]}
                 ~system-bindings ~system-loader
                 ~@body))


(defmacro with-system-and-clean-up [system-binding & body]
  `(with-system* {:on-init [dc/clean-db]
                  :on-halt [ig/halt!]}
                 ~system-binding
                 ~@body))

(defn truncate-string [s n]
  (subs s 0 (min (count s) n)))

(defn submap?
  "Checks whether map a contains all entries in map b."
  [a b]
  (= b (last (c.data/diff a b))))
