(ns tpximpact.datahost.system-uris
  "Contains few families of functions for creating resource
  keys (strings) or URIs. Also a generic `resource-uri` fn
  (and associated multimethod `-resource-uri` that accepts can
  construct an URI from a map of path params.
  
  
  - 'dataset-[series|release|revision]-uri' - ctors taking individual
  ids as arguments
  - 'dataset-[series|release|revision]-uri*'- ctors taking a map
  as argument"
  (:require [integrant.core :as ig])
  (:import (java.net URI)))

(defn- dataset-series-key [ld-root series-slug]
  (str (.getPath ld-root) series-slug))

(defn- release-key [ld-root series-slug release-slug]
  (str (dataset-series-key ld-root series-slug) "/release/" release-slug))

(defn- release-schema-key
  ([ld-root {:keys [series-slug release-slug]}]
   (release-schema-key ld-root series-slug release-slug))
  ([ld-root series-slug release-slug]
   (str (release-key ld-root series-slug release-slug) "/schema" )))

(defn- revision-key [ld-root series-slug release-slug revision-id]
  (str (release-key ld-root series-slug release-slug) "/revision/" revision-id))

(defn commit-key [ld-root series-slug release-slug revision-id commit-id]
  (str (revision-key ld-root series-slug release-slug revision-id) "/commit/" commit-id))

(defprotocol AppUri
  "Build LD API application URIs based upon a configurable base URI state"
  (rdf-base-uri [this])

  (dataset-series-uri [this series-slug])

  (dataset-series-uri* [this api-params])

  (release-uri-base [this series-slug])

  (dataset-release-uri [this ^URI series-uri release-slug])

  (dataset-release-uri* [this api-params])

  (release-schema-uri [this {:keys [series-slug release-slug]}])

  (revision-uri [_ series-slug release-slug revision-id])

  (dataset-revision-uri* [this api-params])

  (commit-uri [this series-slug release-slug revision-id commit-id])

  (commit-uri* [this api-params]))

(defn make-system-uris
  "Returns an object that builds URIs based upon the configured RDF base URI"
  [base-uri]

  (reify AppUri
    (rdf-base-uri [_] base-uri)

    (dataset-series-uri [_ series-slug]
      (.resolve base-uri series-slug))

    (dataset-series-uri* [this {:keys [series-slug] :as _api-params}]
      (dataset-series-uri this series-slug))

    (release-uri-base [_ series-slug]
      (str "/data/" series-slug "/release/"))

    (dataset-release-uri [_ series-uri release-slug]
      (URI. (format "%s/release/%s" series-uri release-slug)))

    (dataset-release-uri* [this {:keys [series-slug release-slug] :as _api-params}]
      (URI. (format "%s/release/%s" (dataset-series-uri this series-slug) release-slug)))

    (release-schema-uri [_ {:keys [series-slug release-slug]}]
      (.resolve base-uri (release-schema-key base-uri series-slug release-slug)))

    (revision-uri [_ series-slug release-slug revision-id]
      (.resolve base-uri (revision-key base-uri series-slug release-slug revision-id)))

    (dataset-revision-uri* [this {:keys [series-slug release-slug revision-id]}]
      (URI. (format "%s/release/%s/revision/%s" (dataset-series-uri this series-slug) release-slug revision-id)))

    (commit-uri [_ series-slug release-slug revision-id commit-id]
      (assert commit-id)
      (.resolve base-uri (commit-key base-uri series-slug release-slug revision-id commit-id)))

    (commit-uri* [_this {:keys [series-slug release-slug revision-id commit-id]}]
      (assert commit-id)
      (.resolve base-uri (commit-key base-uri series-slug release-slug revision-id commit-id)))))

(defmethod ig/init-key ::uris [_ opts]
  (make-system-uris (:rdf-base-uri opts)))

(defmulti -resource-uri
          "Returns URI for the given resource."
          (fn [resource _system-uris _] resource))

(defmethod -resource-uri :dh/DatasetSeries [_ system-uris params]
  (dataset-series-uri* system-uris params))

(defmethod -resource-uri :dh/Release [_ system-uris params]
  (dataset-release-uri* system-uris params))

(defmethod -resource-uri :dh/Revision [_ system-uris params]
  (dataset-revision-uri* system-uris params))

(defmethod -resource-uri :dh/Change [_ system-uris {:keys [series-slug release-slug revision-id change-id]}]
  (commit-uri system-uris series-slug release-slug revision-id change-id))

(defn resource-uri
  "Returns an uri for resource, where resource in
  #{:dh/DatasetsSeries :dh/Release :dh/Revision} and params should
  match the typical path params."
  [resource system-uris params]
  (-resource-uri resource system-uris params))
