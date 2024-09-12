(ns tpximpact.datahost.ldapi.json-ld
  (:require [jsonista.core :as jsonista]
            [tpximpact.datahost.system-uris :as su])
  (:import (com.apicatalog.jsonld JsonLd)
           (com.apicatalog.jsonld.document JsonDocument)
           (java.io StringReader)))

(defn context 
  ([system-uris] 
  ;; Serve the context file with an application/json header from jsdelvir (free CDN)
  ;; See here for instructions: https://www.jsdelivr.com/?docs=gh
  ;;
  ;; The context file cannot be served directly from raw.githubusercontent.com because
  ;; Github serves all files as text/plain, and the com.apicatalog.jsonld requires that
  ;; the file has the correct header.
  ;;
  ;; NOTE: This should be updated to track the dluhc-integration branch
   ["https://cdn.jsdelivr.net/gh/Swirrl/datahost-prototypes@1282114/datahost-ld-openapi/resources/jsonld-context.json"
    {"@base" (su/rdf-base-uri system-uris)}]
   )
  ([system-uris series-slug]
   ["https://cdn.jsdelivr.net/gh/Swirrl/datahost-prototypes@1282114/datahost-ld-openapi/resources/jsonld-context.json"
    {"@base" (su/rdf-base-uri system-uris)
     "@dh:hasRelease" {"@context" {"@base" (su/release-uri-base system-uris series-slug)}}}])
  )

(defn ->json-document
  ^JsonDocument [edn]
  (-> edn
      (jsonista/write-value-as-string)
      (StringReader.)
      (JsonDocument/of)))

(defn compact
  "Takes an EDN JSON-LD data structure and converts them to a compacted
  jakarta.json.JsonObject, which can be further manipulated or written
  to a string via .toString"
  [json-ld context]
  (-> (->json-document json-ld)
      (JsonLd/compact (->json-document context))
      (.compactArrays true)
      (.compactToRelative true)
      (.get)))
