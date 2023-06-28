(ns tpximpact.datahost.ldapi.compact
  (:require [grafter-2.rdf4j.io :as gio])
  (:import [java.net URI]))

(def default-context (atom {}))

(defn add-prefix [prefix uri]
  (swap! default-context assoc (name prefix) uri))

(defn- add-grafter-prefixes []
  (doseq [[prefix uri-str] gio/default-prefixes]
    (add-prefix prefix (URI. uri-str))))

(defn expand [compact-uri]
  (let [prefix-key (-> compact-uri namespace)
        prefixes @default-context]
    (if-let [^URI prefix (get prefixes prefix-key)]
      (.resolve prefix (name compact-uri))
      (throw (ex-info (format "Unknown prefix '%s'" (name prefix-key)) {:prefixes prefixes})))))

(add-prefix :dh (URI. "https://publishmydata.com/def/datahost/"))
(add-grafter-prefixes)
