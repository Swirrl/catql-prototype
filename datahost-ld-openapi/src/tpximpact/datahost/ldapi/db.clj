(ns tpximpact.datahost.ldapi.db
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [com.yetanalytics.flint :as f]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.repository :as repo]
            [metrics.timers :refer [time!]]
            [tpximpact.datahost.ldapi.compact :as compact]
            [tpximpact.datahost.ldapi.metrics :as metrics]
            [tpximpact.datahost.ldapi.native-datastore :as datastore]
            [tpximpact.datahost.ldapi.resource :as resource]
            [tpximpact.datahost.system-uris :as su]
            [tpximpact.datahost.time :as time])
  (:import (java.net URI)
           (org.eclipse.rdf4j.common.transaction IsolationLevels)
           (org.eclipse.rdf4j.repository RepositoryConnection)))

(def default-prefixes {:dcat (URI. "http://www.w3.org/ns/dcat#")
                       :dcterms (URI. "http://purl.org/dc/terms/")
                       :owl (URI. "http://www.w3.org/2002/07/owl#")
                       :qb (URI. "http://purl.org/linked-data/cube#")
                       :rdf (URI. "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
                       :rdfs (URI. "http://www.w3.org/2000/01/rdf-schema#")
                       :skos (URI. "http://www.w3.org/2004/02/skos/core#")
                       :void (URI. "http://rdfs.org/ns/void#")
                       :xsd (URI. "http://www.w3.org/2001/XMLSchema#")
                       :foaf (URI. "http://xmlns.com/foaf/0.1/")
                       :dh (URI. "https://publishmydata.com/def/datahost/")})

(defn resource-exists? [triplestore uri]
  (datastore/eager-query triplestore
                         (f/format-query {:ask []
                                          :where [[uri '?p '?o]]} :pretty? true)))

(defn- map-properties [prop-mapping m]
  (reduce-kv (fn [props k v]
               (if-let [prop-uri (get prop-mapping k)]
                 (resource/add-properties-property props prop-uri v)
                 props))
             (resource/empty-properties)
             m))
(defn- params->title-description-properties [query-params]
  (let [prop-mapping {:title (compact/expand :dcterms/title)
                      :description (compact/expand :dcterms/description)}]
    (map-properties prop-mapping query-params)))

(defn get-release-by-uri
  "Loads a Release in triple form"
  [triplestore release-uri]
  (let [q {:prefixes (compact/as-flint-prefixes)
           :construct [[release-uri 'a :dh/Release]
                       [release-uri :dcterms/title '?title]
                       [release-uri :dcterms/description '?description]
                       [release-uri :dcat/inSeries '?series]
                       [release-uri :dh/hasRevision '?revision]
                       [release-uri :dh/hasSchema '?schema]
                       [release-uri :dcterms/modified '?modified]
                       [release-uri :dcterms/issued '?issued]
                       [release-uri :dcterms/license '?license]
                       [release-uri :dh/coverage '?coverage]
                       [release-uri :dh/geographyDefinition '?geoDefinition]
                       [release-uri :dh/reasonForChange '?reasonForChange]]
           :where [[release-uri 'a :dh/Release]
                   [release-uri :dcterms/title '?title]
                   [release-uri :dcat/inSeries '?series]
                   [:optional [[release-uri :dh/hasRevision '?revision]]]
                   [:optional [[release-uri :dh/hasSchema '?schema]]]
                   [:optional [[release-uri :dcterms/description '?description]]]
                   [:optional [[release-uri :dcterms/license '?license]]]
                   [:optional [[release-uri :dh/coverage '?coverage]]]
                   [:optional [[release-uri :dh/geographyDefinition '?geoDefinition]]]
                   [:optional [[release-uri :dh/reasonForChange '?reasonForChange]]]
                   [release-uri :dcterms/modified '?modified]
                   [release-uri :dcterms/issued '?issued]]}]
    (time! metrics/get-release-by-uri
           (datastore/eager-query triplestore
                                  (f/format-query q :pretty? true)))))

(defn get-dataset-series [triplestore series-uri]
  (let [bgps [[series-uri 'a :dh/DatasetSeries]
              [series-uri :dcterms/title '?title]
              [series-uri :dh/baseEntity '?baseentity]
              [series-uri :dcterms/modified '?modified]
              [series-uri :dcterms/issued '?issued]]
        series-query {:prefixes default-prefixes
                      :construct (conj bgps
                                       [series-uri :dcterms/description '?description]
                                       [series-uri :rdfs/comment '?comment]
                                       [series-uri :dcterms/publisher '?publisher]
                                       [series-uri :dcat/theme '?theme]
                                       [series-uri :dcterms/license '?license]
                                       [series-uri :dcat/keywords '?keywords]
                                       [series-uri :dh/nextUpdate '?nextUpdate]
                                       [series-uri :dh/relatedLinks '?links]
                                       [series-uri :dh/contactName '?contactName]
                                       [series-uri :dh/contactEmail '?contactEmail]
                                       [series-uri :dh/contactPhone '?contactPhone]
                                       [series-uri :dh/hasRelease '?release]
                                       ['?release 'a :dh/Release]
                                       ['?release :dcterms/title '?releaseTitle])
                      :where (conj bgps
                                   [:optional [[series-uri :dcterms/description '?description]]]
                                   [:optional [[series-uri :rdfs/comment '?comment]]]
                                   [:optional [[series-uri :dcterms/publisher '?publisher]]]
                                   [:optional [[series-uri :dcat/theme '?theme]]]
                                   [:optional [[series-uri :dcterms/license '?license]]]
                                   [:optional [[series-uri :dcat/keywords '?keywords]]]
                                   [:optional [[series-uri :dh/nextUpdate '?nextUpdate]]]
                                   [:optional [[series-uri :dh/relatedLinks '?links]]]
                                   [:optional [[series-uri :dh/contactName '?contactName]]]
                                   [:optional [[series-uri :dh/contactEmail '?contactEmail]]]
                                   [:optional [[series-uri :dh/contactPhone '?contactPhone]]]
                                   [:optional [['?release :dcat/inSeries series-uri]
                                               ['?release 'a :dh/Release]
                                               ['?release :dcterms/title '?releaseTitle]]])}]

    (time! metrics/get-dataset-series
           (datastore/eager-query triplestore
                                  (f/format-query series-query :pretty? true)))))

(defn get-release-schema-statements
  [triplestore release-uri]
  (let [q {:prefixes default-prefixes
           :construct [['?schema '?p '?o]
                       ['?schema :dh/columns '?col]
                       ['?col '?col_p '?col_o]]
           :where [[release-uri :dh/hasSchema '?schema]
                   ['?schema '?p '?o]
                   ['?schema :dh/columns '?col]
                   ['?col '?col_p '?col_o]]}
        q2 (f/format-query q :pretty? true)]
    (datastore/eager-query triplestore q2)))

(defn ^:deprecated get-release-schema
  [triplestore release-uri]
  (let [schema-statements (get-release-schema-statements triplestore release-uri)]
    (when-let [schema (when (seq schema-statements)
                        (resource/from-statements schema-statements))]
      (assert (resource/id schema))
      schema)))

(defn get-revision
  [triplestore revision-uri]
  (let [q (let [bgps [[revision-uri 'a :dh/Revision]
                      [revision-uri :dcterms/title '?title]
                      [revision-uri :dh/appliesToRelease '?release]]]
            {:prefixes default-prefixes
             :construct (conj bgps
                              [revision-uri :dh/hasChange '?change]
                              [revision-uri :dcterms/description '?description]
                              [revision-uri :dh/revisionSnapshotCSV '?snapshot]
                              [revision-uri :dh/publicationDate '?publicationDate]
                              [revision-uri :dcterms/license '?license]
                              [revision-uri :dh/reasonForChange '?reasonForChange])
             :where (conj bgps
                          [:optional [[revision-uri :dh/hasChange '?change] ['?change :dh/revisionSnapshotCSV '?snapshot]]]
                          [:optional [[revision-uri :dcterms/description '?description]]]
                          [:optional [[revision-uri :dh/publicationDate '?publicationDate]]]
                          [:optional [[revision-uri :dcterms/license '?license]]]
                          [:optional [[revision-uri :dh/reasonForChange '?reasonForChange]]])})]
    (datastore/eager-query triplestore
                           (f/format-query q :pretty? true))))

(defn get-all-series
  "Returns all Series in triple from"
  [triplestore]
  (let [q (let [bgps [['?series 'a :dh/DatasetSeries]
                      ['?series :dcterms/title '?title]
                      ['?series :dh/baseEntity '?baseentity]
                      ['?series :dcterms/modified '?modified]
                      ['?series :dcterms/issued '?issued]]]
            {:prefixes default-prefixes
             :construct (conj bgps
                              ['?series :dcterms/description '?description]
                              ['?series :dh/hasRelease '?release]
                              ['?release 'a :dh/Release]
                              ['?release :dcterms/title '?releaseTitle])
             :where (conj bgps
                          [:optional [['?series :dcterms/description '?description]]]
                          [:optional [['?release :dcat/inSeries '?series]
                                      ['?release 'a :dh/Release]
                                      ['?release :dcterms/title '?releaseTitle]]])
             :order-by '[(asc ?issued)]})]
    (time! metrics/get-all-series
     (datastore/eager-query triplestore
                            (f/format-query q :pretty? true)))))

(defn get-revisions
  "Returns all Revisions for a Release in triple form"
  [triplestore release-uri]
  (let [q (let [bgps [['?revision_uri 'a :dh/Revision]
                      ['?revision_uri :dcterms/title '?title]
                      ['?revision_uri :dh/appliesToRelease release-uri]]]
            {:prefixes default-prefixes
             :construct (conj bgps
                              ['?revision_uri :dh/hasChange '?change]
                              ['?revision_uri :dcterms/description '?description])
             :where (conj bgps
                          [:optional [['?revision_uri :dh/hasChange '?change]]]
                          [:optional [['?revision_uri :dcterms/description '?description]]])
             :order-by '[(asc ?revision_uri)]})]
    (datastore/eager-query triplestore
                           (f/format-query q :pretty? true))))

(defn get-releases
  "Returns all Releases for a Series in triple form"
  [triplestore series-uri]
  (let [q (let [bgps [['?release 'a :dh/Release]
                      ['?release :dcterms/title '?title]
                      ['?release :dcat/inSeries series-uri]
                      ['?release :dcterms/modified '?modified]
                      ['?release :dcterms/issued '?issued]]]
            {:prefixes default-prefixes
             :construct (conj bgps
                              ['?release :dh/hasRevision '?revision]
                              ['?release :dh/hasSchema '?schema]
                              ['?release :dcterms/description '?description]
                              ['?release :dh/reasonForChange '?reason])
             :where (conj bgps
                          [:optional [['?release :dh/hasRevision '?revision]]]
                          [:optional [['?release :dh/hasSchema '?schema]]]
                          [:optional [['?release :dcterms/description '?description]]]
                          [:optional [['?release :dh/reasonForChange '?reason]]]
                          )
             :order-by '[(asc ?release)]})]
    (datastore/eager-query triplestore
                           (f/format-query q :pretty? true))))

(defn get-change
  "Returns a single Revision Change in triple form"
  [triplestore change-uri]
  (->> (f/format-query (let [bgps [[change-uri 'a :dh/Change]
                                   [change-uri :dcterms/description '?description]
                                   [change-uri :dcterms/format '?format]
                                   [change-uri :dh/updates '?updates]
                                   [change-uri :dh/appliesToRevision '?revision]
                                   [change-uri :dh/changeKind '?changeKind]]
                             snapshot-csv-tri [change-uri :dh/revisionSnapshotCSV '?snapshot]]
                         {:prefixes default-prefixes
                          :construct (conj bgps snapshot-csv-tri)
                          :where (conj bgps [:optional [snapshot-csv-tri]])})
                       :pretty? true)
       (datastore/eager-query triplestore)))

(defn- get-changes-info-query
  [release-uri max-rev {:keys [order limit] :or {order 'asc}}]
  (cond-> {:prefixes (into {} (map (fn [k] [k (str "<" (get default-prefixes k) ">")]) [:xsd :dh :dcterms]))
           :select ['?rev '?updates '?rev_number '?change_number '?kind '?format '?snapshotKey]
           :where (cond-> [['?rev :dh/appliesToRelease release-uri]
                           ['?rev :dh/hasChange '?change]
                           ['?change :dh/updates '?updates]
                           ['?change :dh/changeKind '?kind]
                           ['?change :dcterms/format '?format]
                           ['?change :dh/revisionSnapshotCSV '?snapshotKey]
                           [:bind ['(:xsd/integer (replace (str ?rev) "^.*/([^/]*)$" "$1")) '?rev_number]]
                           [:bind ['(:xsd/integer (replace (str ?change) "^.*/([^/]*)$" "$1")) '?change_number]]]
                    (some? max-rev) (conj [:filter (list '<= '?rev_number max-rev)]))
           :order-by [(list order '?rev_number)
                      (list order '?change_number)]}
    limit (assoc :limit limit)))

(defn get-changes-info
  "Returns records for all changes, optionally up to `?max-rev` (int)
  revision (inclusive).

  The returned seq will contain maps of shape
       {:rev_num Number :rev URI :updates FILE-KEY :kind change-kind}"
  ([triplestore release-uri] (get-changes-info triplestore release-uri nil))
  ([triplestore release-uri ?max-rev]
   {:pre [(some? release-uri) (or (nil? ?max-rev) (pos? ?max-rev))]}
   (time!
    metrics/get-changes-info
    (datastore/eager-query triplestore (f/format-query (get-changes-info-query release-uri ?max-rev {}))))))

(defn get-latest-change-info
  "Returns record for the latest change, or nil.

  The returned map is of shape:
       {:rev_num Number :rev URI :updates FILE-KEY :kind change-kind}"
  ([triplestore release-uri]
   {:pre [(some? release-uri)]}
   (-> triplestore
       (datastore/eager-query (f/format-query (get-changes-info-query release-uri Integer/MAX_VALUE
                                                                      {:order 'desc :limit 1})))
       first)))

(defn- input-context [ld-root]
  (assoc (update-vals @compact/default-context str)
    "@base" (str ld-root)))

(defn- output-context [prefixes ld-root]
  (assoc (update-vals (compact/sub-context prefixes) str)
    "@base" (str ld-root)))

(defn- annotate-json-resource [json-doc resource-uri resource-type ld-root]
  (assoc json-doc
    "@id" (str resource-uri)
    "@type" (str resource-type)
    "@context" (input-context ld-root)))

(defn- request->series [system-uris api-params json-doc]
  (let [series-uri (su/dataset-series-uri system-uris (:series-slug api-params))
        ld-root (su/rdf-base-uri system-uris)
        series-doc (annotate-json-resource json-doc series-uri (compact/expand :dh/DatasetSeries) ld-root)
        doc-resource (resource/from-json-ld-doc series-doc)
        param-properties (params->title-description-properties api-params)]
    (resource/set-properties doc-resource param-properties)))

(defn- request->release [system-uris api-params json-doc]
  (let [series-uri (su/dataset-series-uri* system-uris api-params)
        release-uri (su/dataset-release-uri* system-uris api-params)
        release-doc (annotate-json-resource json-doc release-uri (compact/expand :dh/Release) (su/rdf-base-uri system-uris))
        doc-resource (resource/from-json-ld-doc release-doc)
        param-properties (params->title-description-properties api-params)
        base-release (resource/set-properties doc-resource param-properties)]
    (resource/set-property1 base-release (compact/expand :dcat/inSeries) series-uri)))

(defn- update-resource-title-description-modified-query [resource]
  (let [resource-uri (resource/id resource)
        resource-type (resource/get-property1 resource (compact/expand :rdf/type))
        title (resource/get-property1 resource (compact/expand :dcterms/title))
        description (resource/get-property1 resource (compact/expand :dcterms/description))
        modified-at (resource/get-property1 resource (compact/expand :dcterms/modified))]
    {:prefixes default-prefixes
     :delete [[resource-uri :dcterms/title '?title]
              [resource-uri :dcterms/description '?description]
              [resource-uri :dcterms/modified '?modified]]
     :insert [[resource-uri :dcterms/title title]
              [resource-uri :dcterms/description description]
              [resource-uri :dcterms/modified modified-at]]
     :where [[resource-uri 'a resource-type]
             [resource-uri :dcterms/title '?title]
             [resource-uri :dcterms/description '?description]
             [resource-uri :dcterms/modified '?modified]]}))

(defn- submit-updates [triplestore update-queries]
  (let [qs (f/format-updates update-queries :pretty? true)]
    (with-open [conn (repo/->connection triplestore)]
      (pr/update! conn qs))))

(defn- update-resource-title-description-modified [triplestore resource]
  (let [q (update-resource-title-description-modified-query resource)
        qs (f/format-update q :pretty? true)]
    (with-open [conn (repo/->connection triplestore)]
      (pr/update! conn qs))))

(defn- update-series [triplestore series]
  (update-resource-title-description-modified triplestore series))

(defn- update-release [triplestore release]
  (update-resource-title-description-modified triplestore release))

(defn- modified-if-properties-changed [clock existing-resource request-resource diff-properties]
  (let [request-properties (resource/get-properties request-resource diff-properties)
        ;; NOTE: may only be a partial update so only fetch properties defined on the request
        existing-properties (resource/get-properties existing-resource (keys request-properties))]
    (if (= existing-properties request-properties)
      [false existing-resource]
      (let [updated (-> existing-resource
                        (resource/set-properties request-properties)
                        (resource/set-property1 (compact/expand :dcterms/modified) (time/now clock)))]
        [true updated]))))

(defn- modified-if-title-description-changed [clock existing-resource request-resource]
  (let [diff-properties [(compact/expand :dcterms/title)
                         (compact/expand :dcterms/description)]]
    (modified-if-properties-changed clock existing-resource request-resource diff-properties)))

(defn- merge-series-updates [clock old-series new-properties]
  (modified-if-title-description-changed clock old-series new-properties))

(defn- merge-release-updates [clock existing-release request-release]
  (modified-if-title-description-changed clock existing-release request-release))

(defn- set-timestamps [clock resource]
  (let [now (time/now clock)]
    (-> resource
        (resource/set-property1 (compact/expand :dcterms/issued) now)
        (resource/set-property1 (compact/expand :dcterms/modified) now))))

(defn- set-base-entity [series]
  (resource/set-property1 series (compact/expand :dh/baseEntity) (resource/id series)))

(defn- insert-resource [triplestore resource]
  (with-open [conn (repo/->connection triplestore)]
    (pr/add conn (resource/->statements resource))))

(defn- insert-series [clock triplestore series]
  ;; TODO: move setting default properties outside?
  (let [series (->> series (set-timestamps clock) set-base-entity)]
    (time! metrics/insert-series (insert-resource triplestore series))
    series))

(defn- insert-release [clock triplestore release]
  (let [release (set-timestamps clock release)]
    (insert-resource triplestore release)
    release))

;; TODO: move this!
(defn series->response-body [series ld-root]
  (resource/->json-ld series (output-context ["dh" "dcterms" "rdf" "rdfs" "dcat" "csvw" "appropriate-csvw"] ld-root)))

(defn release->response-body [release ld-root]
  (resource/->json-ld release (output-context ["dh" "dcterms" "rdf" "rdfs" "dcat" "csvw" "appropriate-csvw"] ld-root)))

(defn revision->response-body [revision ld-root]
  (resource/->json-ld revision (output-context ["dh" "dcterms" "rdf"] ld-root)))

(defn- map-by [f items]
  (into {} (map (fn [v] [(f v) v]) items)))

(defn- ->coll [x]
  (if (coll? x) x [x]))

(defn- column-number [col]
  (Integer/parseInt (get col "csvw:number")))

(defn schema->response-body [schema ld-root]
  ;; NOTE: Schema documents are modified from the standard json-ld serialisation
  ;; The top-level @graph node is removed and the column nodes are inlined within
  ;; the schema node
  (let [json-ld-str (resource/->json-ld schema (output-context ["dh" "dcterms" "csvw" "appropriate-csvw"] ld-root))
        json-ld-doc (json/read-str json-ld-str)
        nodes (get json-ld-doc "@graph")
        is-schema-node? (fn [n] (= "dh:TableSchema" (get n "@type")))
        schema-node (first (filter is-schema-node? nodes))
        column-nodes (remove is-schema-node? nodes)
        base-uri-str (get-in json-ld-doc ["@context" "@base"])
        node-uri->node (map-by (fn [col] (str base-uri-str (get col "@id"))) column-nodes)
        inlined-schema (update schema-node "dh:columns" (fn [col-uri-or-uris]
                                                          (->> (->coll col-uri-or-uris)
                                                               (map node-uri->node)
                                                               (sort-by column-number)
                                                               (vec))))
        inlined-doc (assoc inlined-schema "@context" (get json-ld-doc "@context"))]
    (json/write-str inlined-doc)))

;; TODO: return series directly instead of formatting json-ld doc here
(defn upsert-series!
  "Returns a map {:op ... :jsonld-doc ...}, where :op conforms to
  `tpximpact.datahost.ldapi.schemas.api/UpsertOp`"
  [clock triplestore system-uris api-params incoming-jsonld-doc]
  (let [series-uri (su/dataset-series-uri system-uris (:series-slug api-params))
        ld-root (su/rdf-base-uri system-uris)
        request-series (request->series system-uris api-params incoming-jsonld-doc)]
    (if-let [existing-series (some->> (get-dataset-series triplestore series-uri)
                                      (resource/from-statements))]
      (let [[changed? new-series] (merge-series-updates clock existing-series request-series)]
        (when changed?
          (update-series triplestore new-series))
        {:op (if changed? :update :noop)
         :jsonld-doc (series->response-body new-series ld-root)})
      (let [created-series (insert-series clock triplestore request-series)]
        {:op :create :jsonld-doc (series->response-body created-series ld-root)}))))

(defn delete-series-changes-query [series-uri]
  {:prefixes (compact/as-flint-prefixes)
   :delete [['?change '?p '?o]
            ['?revision :dh/hasChange '?change]]
   :where [['?release :dcat/inSeries series-uri]
           ['?revision :dh/appliesToRelease '?release]
           ['?revision :dh/hasChange '?change]
           ['?change '?p '?o]]})

(defn delete-series-revisions-query [series-uri]
  {:prefixes (compact/as-flint-prefixes)
   :delete [['?revision '?p '?o]
            ['?release :dh/hasRevision '?revision]]
   :where [['?release :dcat/inSeries series-uri]
           ['?revision :dh/appliesToRelease '?release]
           ['?revision '?p '?o]]})

(defn delete-series-releases-schemas-query [series-uri]
  {:prefixes (compact/as-flint-prefixes)
   :delete [['?col '?pColumnData '?oColumnData]
            ['?schema '?p '?o]
            ['?release :dh/hasSchema '?schema]
            ['?schema :dh/columns '?col]]
   :where [['?release :dcat/inSeries series-uri]
           ['?release :dh/hasSchema '?schema]
           ['?schema :dh/columns '?col]
           ['?col '?pColumnData '?oColumnData]
           ['?schema '?p '?o]]})

(defn delete-series-releases-query [series-uri]
  {:prefixes (compact/as-flint-prefixes)
   :delete [['?release '?p '?o]]
   :where [['?release :dcat/inSeries series-uri]
           ['?release '?p '?o]]})

(defn delete-series-query [series-uri]
  {:delete [[series-uri '?p '?o]]
   :where [[series-uri '?p '?o]]})

(defn- find-orphaned-changes-query
  "Query to find all change keys which are only referenced by series-uri"
  [series-uri]
  {:prefixes (compact/as-flint-prefixes)
   :select ['?key]
   :where [[:where {:select ['?key]
                    :where [['?release :dcat/inSeries series-uri]
                            ['?revision :dh/appliesToRelease '?release]
                            ['?revision :dh/hasChange '?change]
                            ['?change :dh/updates '?key]]}]
           [:minus [[:where {:select ['?key]
                             :where [['?release :dcat/inSeries '?series]
                                     [:filter (list 'not= '?series series-uri)]
                                     ['?revision :dh/appliesToRelease '?release]
                                     ['?revision :dh/hasChange '?change]
                                     ['?change :dh/updates '?key]]}]]]]})

(defn- find-orphaned-changes [triplestore series-uri]
  (let [q (find-orphaned-changes-query series-uri)
        qs (f/format-query q :pretty? true)]
    (with-open [conn (repo/->connection triplestore)]
      (let [results (repo/query conn qs)]
        (set (map :key results))))))

(defn delete-series!
  "Deletes a series and all associated child resources. Returns a collection of change
  keys which have been orphaned by the deletion within the change store."
  [triplestore system-uris series-slug]
  (let [series-uri (su/dataset-series-uri system-uris series-slug)
        orphaned-changes (find-orphaned-changes triplestore series-uri)
        queries [(delete-series-changes-query series-uri)
                 (delete-series-releases-schemas-query series-uri)
                 (delete-series-revisions-query series-uri)
                 (delete-series-releases-query series-uri)
                 (delete-series-query series-uri)]]
    (time! metrics/delete-series!
           (submit-updates triplestore queries))
    orphaned-changes))

(defn upsert-release!
  "Returns a map {:op ... :jsonld-doc ...} where :op conforms to
  `tpximpact.datahost.ldapi.schemas.api/UpsertOp`"
  [clock triplestore system-uris api-params incoming-jsonld-doc]
  (let [ld-root (su/rdf-base-uri system-uris)
        request-release (request->release system-uris api-params incoming-jsonld-doc)]
    (if-let [existing-release (some->> (get-release-by-uri triplestore (resource/id request-release))
                                       (resource/from-statements))]
      (let [[changed? new-release] (merge-release-updates clock existing-release request-release)]
        (when changed?
          (update-release triplestore new-release))
        {:op (if changed? :update :noop)
         :jsonld-doc (release->response-body new-release ld-root)})
      (let [created-release (insert-release clock triplestore request-release)]
        {:op :create :jsonld-doc (release->response-body created-release ld-root)}))))

(defn- select-auto-increment-query [parent-uri child-pred]
  {:prefixes (select-keys default-prefixes [:dh :xsd])
   :select ['?next]
   :where [[:where {:select '[[(max (:xsd/integer (replace (str ?child) "^.*/([^/]*)$" "$1"))) ?highest]]
                    :where [[parent-uri child-pred '?child]]}]
           [:bind ['(coalesce (+ ?highest 1) 1) '?next]]]})

(defn- select-max-n-query [parent-uri child-pred]
  (when (nil? parent-uri)
    (throw (java.lang.IllegalArgumentException. "'parent-uri' must be non nil.")))
  {:prefixes (select-keys default-prefixes [:dh :xsd])
   :select ['?n]
   :where [[:where {:select '[[(max (:xsd/integer (replace (str ?child) "^.*/([^/]*)$" "$1"))) ?highest]]
                    :where [[parent-uri child-pred '?child]]}]
           [:bind ['(coalesce ?highest 0) '?n]]]})

(defn- select-max-n
  "Returns a number or nil."
  [connection parent-uri child-pred]
  (-> connection
      (repo/query (f/format-query (select-max-n-query parent-uri child-pred)))
      (doall)
      first
      :n))

(defn get-release-snapshot-info
  "Given a release (as path-params) attempts to fetch the latest
  revision-id and the key of the dataset snapshot (as
  in :dh/revisionSnapshotCSV).

  Returns a map of {:data-key ... :revision-id ...} or nil"
  [triplestore system-uris path-params]
  (let [release-uri (su/dataset-release-uri* system-uris  path-params)
        rev-id (with-open [conn (repo/->connection triplestore)]
                 (select-max-n conn release-uri (compact/expand :dh/hasRevision)))
        revision (when (pos? rev-id)
                   (get-revision triplestore
                                 (su/dataset-revision-uri* system-uris
                                                           (assoc path-params :revision-id rev-id))))
        pred (compact/expand :dh/revisionSnapshotCSV)
        pred-fn (fn [q] (= pred (:p q)))
        data-key (:o (first (filter pred-fn revision)))]
    (when revision
      {:revision-id rev-id :data-key data-key})))

(defn fetch-next-child-resource-number [triplestore parent-uri child-pred]
  (let [q (select-auto-increment-query parent-uri child-pred)
        qs (f/format-query q :pretty? true)
        bindings (with-open [conn (repo/->connection triplestore)]
                   (doall (repo/query conn qs)))]
    (if-let [bs (first bindings)]
      (:next bs)
      (throw (ex-info "Couldn't fetch new child number for parent resource" {:resource-uri parent-uri})))))

(defn- request->revision [release-uri revision-uri api-params json-doc ld-root]
  (let [revision-doc (annotate-json-resource json-doc revision-uri (compact/expand :dh/Revision) ld-root)
        doc-resource (resource/from-json-ld-doc revision-doc)
        param-properties (params->title-description-properties api-params)]
    (-> doc-resource
        (resource/set-properties param-properties)
        (resource/set-property1 (compact/expand :dh/appliesToRelease) release-uri))))

(defn- request->change [change-kind api-params ld-root revision-uri change-uri]
  (let [change-doc (annotate-json-resource {} change-uri (compact/expand :dh/Change) ld-root)
        doc-resource (resource/from-json-ld-doc change-doc)
        param-properties (params->title-description-properties api-params)]
    (-> doc-resource
        (resource/set-properties param-properties)
        (resource/set-property1 (compact/expand :dh/changeKind) change-kind)
        (resource/set-property1 (compact/expand :dh/appliesToRevision) revision-uri)
        (resource/set-property1 (compact/expand :dcterms/format) (get api-params :format)))))

(defn- request->schema [json-doc ld-root schema-uri release-uri]
  (let [schema-doc (annotate-json-resource json-doc schema-uri (compact/expand :dh/TableSchema) ld-root)
        schema-doc (update schema-doc "dh:columns" (fn [cols]
                                                     (map-indexed (fn mapper [index col]
                                                                    (assoc col "@id" (str schema-uri "/columns/" (inc index))
                                                                           "csvw:number" (inc index)))
                                                                  cols)))
        schema-resource (resource/from-json-ld-doc schema-doc)]
    (-> schema-resource
        (resource/set-property1 (compact/expand :dh/appliesToRelease) release-uri)
        (resource/set-property1 (compact/expand :appropriate-csvw/modeling-of-dialect) "UTF-8,RFC4180"))))

(defn- release-revision-statements
  "Returns a collection of triples connecting a release to a revision"
  [revision]
  [(pr/->Triple (resource/get-property1 revision (compact/expand :dh/appliesToRelease))
                (compact/expand :dh/hasRevision)
                (resource/id revision))])

(defn- revision-change-statements
  "Returns a collection of triples connecting a revision to a change"
  [change]
  [(pr/->Triple (resource/get-property1 change (compact/expand :dh/appliesToRevision))
                (compact/expand :dh/hasChange)
                (resource/id change))])

(defn- dataset-snapshot-statements
  [uri snapshot-key]
  [(pr/->Triple uri (compact/expand :dh/revisionSnapshotCSV) snapshot-key)])

(defn tag-with-snapshot
  "Updates the entity under uri with dataset snapshot information.

  Assumes m contains keys:

  - :new-snapshot-key
  - :previous-snapshot-key (when available) - to remove the snapshot
    key from the revision."
  [triplestore ^URI uri {key :new-snapshot-key prev-key :previous-snapshot-key}]
  (when (nil? key)
    (throw (IllegalArgumentException. "'key' must be non nil.")))
  (let [insert-statements (dataset-snapshot-statements uri key)]
    (log/debug "tag-revision-with-snapshot:" (.getPath uri) (format "key='%s'" key))
    (with-open [conn (repo/->connection triplestore)]
      (when prev-key
        (log/debug "tag-with-snapshot: delete" (.getPath uri) (format "prev-key='%s'" prev-key))
        (pr/delete conn (dataset-snapshot-statements uri prev-key)))
      (pr/add conn insert-statements))))

(defn insert-revision! [triplestore api-params incoming-jsonld-doc ld-root release-uri revision-uri revision-number]
  (let [revision (request->revision release-uri revision-uri api-params incoming-jsonld-doc ld-root)]
    (with-open [conn (repo/->connection triplestore)]
      (pr/add conn (concat (resource/->statements revision)
                           (release-revision-statements revision))))
    {:resource-id revision-number
     :jsonld-doc (revision->response-body revision ld-root)}))

(defn- insert-change-statement*
  "Statement for: insert only when the revision has no changes already."
  [revision-uri change-uri statements]
  (let [type-uri (URI. "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")]
    {:prefixes (select-keys default-prefixes [:dh :xsd])
     :insert (mapv #(vector (:s %) (:p %) (:o %))  statements)
     :where
     [[:where {:select [['(max (:xsd/integer (replace (str ?e) "^.*/([^/]*)$" "$1"))) '?last]]
               :where [['?e type-uri (compact/expand :dh/Change)]]}]
      [:filter (list 'not-exists
                     [[change-uri type-uri (compact/expand :dh/Change)]])]]}))

(defn- last-change-num
  "Returns an integer zero or positive integer."
  [conn revision-uri]
  (select-max-n conn revision-uri (compact/expand :dh/hasChange)))


(defn- maybe-insert-change
  "Returns a map {:before number :after number}, where
  the before and after numbers are the last change-id
  before and after the insert.

  The [[pr/update]] function's return value doesn't say whether the
  insert actually happened."
  [^RepositoryConnection conn rev-uri change-uri change]
  (let [before (last-change-num conn rev-uri)
        statements (concat (resource/->statements change)
                           (revision-change-statements change))]
    (->> (insert-change-statement* rev-uri change-uri statements)
         (f/format-update)
         (pr/update! conn))
    {:before before :after (last-change-num conn rev-uri)}))

(defn insert-change!
  "Returns {:change-id NUMBER :inserted-jsonld-doc DOC :change-uri URI} on success,
  {:message STRING} when an insert could not be performed."
  [triplestore
   system-uris
   {:keys [api-params ld-root store-key datahost.change/kind]
    {rev-uri :dh/Revision} :datahost.request/uris}]
  {:pre [(some? kind) (some? store-key) (some? rev-uri)]}
  (time!
   metrics/insert-change!
   (with-open [conn ^RepositoryConnection (repo/->connection triplestore)]
     (let [prev-change-id (last-change-num conn rev-uri)
           change-id (inc prev-change-id)
           change-uri (su/commit-uri* system-uris (assoc api-params :commit-id change-id))
           _ (log/debug (format "will insert-change for '%s', new change id = %s"
                                (.getPath ^URI rev-uri) change-id))
           change (request->change kind api-params ld-root rev-uri change-uri)
           change (resource/set-property1 change (compact/expand :dh/updates) store-key)
           {:keys [before after]} (do
                                    (.setIsolationLevel conn IsolationLevels/SERIALIZABLE)
                                    (maybe-insert-change conn rev-uri change-uri change))]
       (log/debug "insert-change: " {:change-id change-id :before before :after after})
       (if (and (= prev-change-id before) (= change-id after))
         {:change-id change-id
          :change-uri change-uri
          :inserted-jsonld-doc (resource/->json-ld change (output-context ["dh" "dcterms" "rdf"] ld-root))}
         {:message "Change already exists."})))))

(defn- previous-change-coords
  "Given revision and change id, tries to find the preceding change's
  revision and change ids. If not possible, indicates an extra DB
  lookup is needed.

  Returns a [:new :new] | [rev-id change-id] | [rev-id :find]."
  [revision-id change-id]
  {:pre [(pos? revision-id) (pos? change-id)]}
  (cond
    (and (= revision-id 1) (= 1 change-id)) [:new :new]

    (< 1 change-id) [revision-id (dec change-id)]

    ;; we need to find the last change-id in previous revision
    :else [(dec revision-id) :find]))

(defn get-previous-change
  "Returns the previous change (as Quads), nil when no previous change exists."
  [triplestore system-uris  {:keys [revision-id change-id] :as params}]
  (let [revision-uri ^URI (su/dataset-revision-uri* system-uris params)
        [prev-rev-id c] (previous-change-coords revision-id change-id)]
    (log/debug (format "get-previous-change: '%s'" (.getPath revision-uri)) [prev-rev-id c])
    (cond
      (= :new c)                        ; the given revision+change is the first
      nil

      (number? c)                          ; we got a definitive answer
      (get-change triplestore
                  (su/commit-uri* system-uris (assoc params :revision-id prev-rev-id :commit-id c)))

      (= :find c)                       ; we need to find change-id
      (let [prev-rev-uri (su/dataset-revision-uri* system-uris (assoc params :revision-id prev-rev-id))
            prev-change-id (with-open [conn (repo/->connection triplestore)]
                             (select-max-n conn prev-rev-uri (compact/expand :dh/hasChange)))]
        (log/debug "get-previous-change: find prev change id: " prev-change-id)
        (when (= 0N prev-change-id)
          (throw (ex-info (format "No previous change-id could be found for revision %s"
                                  (.getPath revision-uri))
                          {:params params :revision-uri revision-uri})))
        (get-change triplestore
                    (su/commit-uri* system-uris
                                    (assoc params
                                      :revision-id prev-rev-id
                                      :commit-id prev-change-id))))

      :else (throw (ex-info "Error: illegal state" {:params params})))))

(defn- release-schema-statements [schema]
  [(pr/->Triple (resource/get-property1 schema (compact/expand :dh/appliesToRelease))
                (compact/expand :dh/hasSchema)
                (resource/id schema))])

(defn- insert-schema [clock triplestore schema]
  (let [new-schema (set-timestamps clock schema)]
    (with-open [conn (repo/->connection triplestore)]
      (pr/add conn (concat (resource/->statements new-schema)
                           (release-schema-statements new-schema))))
    new-schema))

(defn upsert-release-schema!
  [clock triplestore system-uris incoming-jsonld-doc slugs]
  (let [schema-uri (su/release-schema-uri system-uris slugs)
        release-uri (su/dataset-release-uri* system-uris slugs)
        ld-root (su/rdf-base-uri system-uris)
        request-schema (request->schema incoming-jsonld-doc ld-root schema-uri release-uri)]
    (if-let [existing-schema (get-release-schema triplestore release-uri)]
      {:op :noop
       :jsonld-doc (schema->response-body existing-schema ld-root)}
      (let [new-schema (insert-schema clock triplestore request-schema)]
        {:op :create :jsonld-doc (schema->response-body new-schema ld-root)}))))
