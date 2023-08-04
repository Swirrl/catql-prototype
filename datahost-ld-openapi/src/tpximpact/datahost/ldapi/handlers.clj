(ns tpximpact.datahost.ldapi.handlers
  (:require
   [clojure.tools.logging :as log]
   [grafter.vocabularies.rdf :as vocab.rdf]
   [grafter.matcha.alpha :as matcha]
   [ring.util.io :as ring-io]
   [tablecloth.api :as tc]
   [tpximpact.datahost.ldapi.compact :as cmp]
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.store :as store]
   [tpximpact.datahost.ldapi.json-ld :as json-ld]
   [tpximpact.datahost.ldapi.resource :as resource]
   [tpximpact.datahost.ldapi.schemas.api :as s.api]
   [tpximpact.datahost.ldapi.models.shared :as models.shared]
   [tpximpact.datahost.ldapi.util.data-validation :as data-validation])
  (:import (java.net URI)))

(def not-found-response
  {:status 404
   :body "Not found"})

(defn- as-json-ld [response]
  (assoc-in response [:headers "content-type"] "application/json+ld"))

(defn get-api-params [{:keys [path-params query-params]}]
  (-> query-params (update-keys keyword) (merge path-params)))

(defn write-dataset-to-outputstream [tc-dataset]
  (ring-io/piped-input-stream
    (fn [out-stream]
      (tc/write! tc-dataset out-stream {:file-type :csv}))))

(defn get-dataset-series [triplestore {{:keys [series-slug]} :path-params}]
  (if-let [series (db/get-series-by-slug triplestore series-slug)]
    (as-json-ld {:status 200
                 :body (db/series->response-body series)})
    not-found-response))

(defn triples->ld-resource
  "Given triples returned from a DB query, transform them into a single resource
   map (e.g. Release or Revision) that's ready for JSON serialization"
  [matcha-db]
  (-> (matcha/build-1 [(keyword "@id") ?s]
                      {?p ?o
                       (keyword "@type") ?t}
                      [[?s ?p ?o]
                       (matcha/optional [[?s vocab.rdf/rdf:a ?t]])]
                      matcha-db)
      (dissoc vocab.rdf/rdf:a)))

(defn triples->ld-resource-collection
  "Given triples returned from a DB query, transform them into a collection of
  resource maps (e.g. seq of Revisions) that are ready for JSON serialization"
  [matcha-db]
  (->> (matcha/build [(keyword "@id") ?s]
                     {?p ?o
                      (keyword "@type") ?t}
                     [[?s ?p ?o]
                      (matcha/optional [[?s vocab.rdf/rdf:a ?t]])]
                     matcha-db)
       (map #(dissoc % vocab.rdf/rdf:a))))

(defn input-stream->dataset [is]
  (tc/dataset is {:file-type :csv}))

(defn csv-file-locations->dataset [change-store append-keys]
  (if (sequential? append-keys)
    (some->> (remove nil? append-keys)
             (map #(input-stream->dataset (store/get-append change-store %)))
             (apply tc/concat))
    (some->> (store/get-append change-store append-keys)
             (input-stream->dataset))))

(defn release->csv-stream [triplestore change-store release]
  (let [appends-file-keys (->> (db/get-appends triplestore (resource/id release) nil)
                               (map :appends))]
    (when-let [merged-datasets (csv-file-locations->dataset change-store appends-file-keys)]
      (write-dataset-to-outputstream merged-datasets))))

(defn op->response-code
  "Takes [s.api/UpsertOp] and returns a HTTP status code (number)."
  [op]
  {:pre [(s.api/upsert-op-valid? op)]}
  (case op
    :create 201
    :update 200
    :noop   200))

(defn put-dataset-series [clock triplestore {:keys [body-params] :as request}]
  (let [api-params (get-api-params request)
        incoming-jsonld-doc body-params
        {:keys [op jsonld-doc]} (db/upsert-series! clock triplestore api-params incoming-jsonld-doc)]
    (as-json-ld {:status (op->response-code op)
                 :body jsonld-doc})))

(defn get-release [triplestore change-store {{:keys [series-slug release-slug]} :path-params
                                             {:strs [accept]} :headers}]
  (if-let [release (->> (db/get-release triplestore series-slug release-slug)
                        (matcha/index-triples)
                        (triples->ld-resource))]
    (if (= accept "text/csv")
      {:status 200
       :headers {"content-type" "text/csv"
                 "content-disposition" "attachment ; filename=release.csv"}
       :body (or (release->csv-stream triplestore change-store release) "")}
      (as-json-ld {:status 200
                   :body (-> (json-ld/compact release json-ld/simple-context)
                             (.toString))}))
    not-found-response))

(defn put-release [clock triplestore {{:keys [series-slug]} :path-params
                       body-params :body-params :as request}]
  (if-let [series (db/get-series-by-slug triplestore series-slug)]
    (let [api-params (get-api-params request)
          incoming-jsonld-doc body-params
          {:keys [op jsonld-doc]} (db/upsert-release! clock triplestore series api-params incoming-jsonld-doc)]
      (as-json-ld {:status (op->response-code op)
                   :body jsonld-doc}))
    {:status 422
     :body "Series for this release does not exist"}))

(defn put-release-schema
  [clock triplestore {{:keys [series-slug]} :path-params
                      incoming-jsonld-doc :body-params
                      :as request}]
   (if (not (db/get-series-by-slug triplestore series-slug))
    not-found-response

    (let [{:keys [op jsonld-doc]} (db/upsert-release-schema! clock triplestore
                                                             (get-api-params request)
                                                             incoming-jsonld-doc)]
      (as-json-ld {:status (op->response-code op)
                   :body jsonld-doc}))))

(defn get-release-schema
    [triplestore {{:keys [series-slug release-slug]} :path-params}]
    (let [release-uri (models.shared/release-uri-from-slugs series-slug release-slug)]
      (if-let [schema (db/get-release-schema triplestore release-uri)]
        (as-json-ld {:status 200
                     :body (db/schema->response-body schema)})
        {:status 404
         :body {:status "error"
                :message "Not found"}})))

(defn- revision-number
  "Returns a number or throws."
  [rev-id]
  (let [path (.getPath ^URI rev-id)]
    (try
      (Long/parseLong (-> (re-find #"^.*/([^/]*)$" path) next first))
      (catch NumberFormatException ex
        (throw (ex-info (format "Could not extract revision number from given id: %s" rev-id)
                        {:revision-id rev-id} ex))))))

(defn revision->csv-stream [triplestore change-store revision]
  (let [rev-id (resource/id revision)
        release-id (get revision (cmp/expand :dh/appliesToRelease))
        appends (db/get-appends triplestore release-id (revision-number rev-id))]
    (when-let [merged-datasets (csv-file-locations->dataset change-store (map :appends appends))]
      (write-dataset-to-outputstream merged-datasets))))

(defn get-revision
  [triplestore change-store {{:keys [series-slug release-slug revision-id]} :path-params
                             {:strs [accept]} :headers :as _request}]
  (if-let [revision-ld (->> (db/get-revision triplestore series-slug release-slug revision-id)
                            matcha/index-triples
                            triples->ld-resource)]
    (if (= accept "text/csv")
      {:status 200
       :headers {"content-type" "text/csv"
                 "content-disposition" "attachment ; filename=revision.csv"}
       :body (or (revision->csv-stream triplestore change-store revision-ld) "")}

      (as-json-ld {:status 200
                   :body (-> (json-ld/compact revision-ld json-ld/simple-context)
                             (.toString))}))
    not-found-response))

(defn- wrap-ld-collection-contents [coll]
  {"https://publishmydata.com/def/datahost/collection-contents" coll})

(defn get-series-list [triplestore _request]
  (let [issued-uri (tpximpact.datahost.ldapi.compact/expand :dcterms/issued)
        series (->> (db/get-all-series triplestore)
                    (matcha/index-triples)
                    (triples->ld-resource-collection)
                    (sort-by #(get % issued-uri))
                    (reverse))
        response-body (-> (wrap-ld-collection-contents series)
                          (json-ld/compact json-ld/simple-collection-context)
                          (.toString))]
    (as-json-ld {:status 200
                 :body response-body})))

(defn get-revision-list [triplestore {{:keys [series-slug release-slug]} :path-params}]
  (let [revisions (->> (db/get-revisions triplestore series-slug release-slug)
                       (matcha/index-triples)
                       (triples->ld-resource-collection)
                       (sort-by (comp str (keyword "@id")))
                       (reverse))
        response-body (-> (wrap-ld-collection-contents revisions)
                          (json-ld/compact json-ld/simple-collection-context)
                          (.toString))]
    (as-json-ld {:status 200
                 :body response-body})))

(defn get-release-list [triplestore {{:keys [series-slug]} :path-params}]
  (let [issued-uri (tpximpact.datahost.ldapi.compact/expand :dcterms/issued)
        releases (->> (db/get-releases triplestore series-slug)
                      (matcha/index-triples)
                      (triples->ld-resource-collection)
                      (sort-by #(get % issued-uri))
                      (reverse))
        response-body (-> (wrap-ld-collection-contents releases)
                          (json-ld/compact json-ld/simple-collection-context)
                          (.toString))]
    (as-json-ld {:status 200
                 :body response-body})))

(defn post-revision [triplestore {{:keys [series-slug release-slug]} :path-params
                                  body-params :body-params :as request}]
  (let [release-uri (-> (models.shared/dataset-series-uri series-slug)
                        (models.shared/dataset-release-uri release-slug))]
    (if (db/resource-exists? triplestore release-uri)
      (let [{:keys [jsonld-doc resource-id]} (db/insert-revision! triplestore (get-api-params request) body-params)]
        (as-json-ld {:status 201
                     :headers {"Location" (format "/data/%s/releases/%s/revisions/%s"
                                                  series-slug release-slug resource-id)}
                     :body jsonld-doc}))

      {:status 422
       :body "Release for this revision does not exist"})))

(defn- dataset-validation-error!
  "Returns nil on success, error response on validation failure."
  [release-schema appends]
  (try
    (let [dataset (data-validation/as-dataset (:tempfile appends) {})
          schema (data-validation/make-row-schema release-schema)
          {:keys [explanation]} (data-validation/validate-dataset dataset schema
                                                                  {:fail-fast? true})]
      (when (some? explanation)
        {:status 400
         :body explanation}))))

(defn post-change [triplestore
                   change-store
                   {{:keys [series-slug release-slug revision-id]} :path-params
                    {{:keys [appends]} :multipart}                 :parameters
                    body-params                                    :body-params :as request}]
  (cond
    (db/resource-exists? triplestore
                         (models.shared/change-uri series-slug release-slug revision-id 1))
    {:status 422
     :body "A change is already associated with the revision."}
    
    (db/resource-exists? triplestore
                         (models.shared/revision-uri series-slug release-slug revision-id))
    (let [api-params (get-api-params request)
          incoming-jsonld-doc body-params
          release-schema (db/get-release-schema triplestore (models.shared/release-uri-from-slugs series-slug release-slug))
          validation-err (when (some? release-schema)
                           (dataset-validation-error! release-schema appends))
          {:keys [jsonld-doc resource-id message]} (when-not validation-err
                                                     (db/insert-change! triplestore change-store api-params
                                                                        incoming-jsonld-doc appends))]
      (log/info (format "post-change: validation: found-schema? = %s change-valid? = "
                        (some? release-schema) (nil? validation-err)))
      (cond
        (some? validation-err) validation-err

        (some? message) {:status 422 :body message}

        :else-success
        (as-json-ld {:status 201
                     :headers {"Location" (format "/data/%s/releases/%s/revisions/%s/changes/%s"
                                                  series-slug release-slug revision-id resource-id)}
                     :body jsonld-doc})))

    :else
    {:status 422
     :body "Revision for this change does not exist"}))

(defn change->csv-stream [change-store change]
  (let [appends (get change (cmp/expand :dh/appends))]
    (when-let [dataset (csv-file-locations->dataset change-store [appends])]
      (write-dataset-to-outputstream dataset))))

(defn get-change [triplestore change-store {{:keys [series-slug release-slug revision-id change-id]} :path-params
                        {:strs [accept]} :headers :as _request}]
  (if-let [change (->> (db/get-change triplestore series-slug release-slug revision-id change-id)
                       matcha/index-triples
                       triples->ld-resource)]
    (if (= accept "text/csv")
      {:status 200
       :headers {"content-type" "text/csv"
                 "content-disposition" "attachment ; filename=change.csv"}
       :body (or (change->csv-stream change-store change) "")}

      {:status 406
       :headers {}
       :body "Only text/csv format is available at this time."})
    not-found-response))
