(ns tpximpact.datahost.ldapi.handlers
  (:require
   [clojure.string :as string]
   [clojure.tools.logging :as log]
   [jsonista.core :as json]
   [grafter.matcha.alpha :as matcha]
   [ring.util.io :as ring-io]
   [tablecloth.api :as tc]
   [reitit.core :as rc]
   [ring.util.request :as util.request]
   [ring.util.response :as util.response]
   [tpximpact.datahost.system-uris :as su]
   [tpximpact.datahost.ldapi.compact :as cmp]
   [tpximpact.datahost.ldapi.db :as db]
   [tpximpact.datahost.ldapi.errors :as errors]
   [tpximpact.datahost.ldapi.store :as store]
   [tpximpact.datahost.ldapi.json-ld :as json-ld]
   [tpximpact.datahost.ldapi.handlers.internal :as internal]
   [tpximpact.datahost.ldapi.resource :as resource]
   [tpximpact.datahost.ldapi.routes.shared :as shared]
   [tpximpact.datahost.ldapi.schemas.api :as s.api]
   [tpximpact.datahost.ldapi.util.data.validation :as data.validation]
   [tpximpact.datahost.ldapi.util.triples
    :refer [triples->ld-resource triples->ld-resource-collection]]
   [clojure.java.io :as io])
  (:import
   (java.net URI)
   (java.io ByteArrayInputStream ByteArrayOutputStream InputStream OutputStream)))

(defn- box [x]
  (if (coll? x) x [x]))

(defn- as-json-ld [response]
  (assoc-in response [:headers "content-type"] "application/ld+json"))

(defn get-api-params [{:keys [path-params query-params]}]
  (-> query-params (update-keys keyword) (merge path-params)))

(defn write-dataset-to-outputstream [tc-dataset]
  (ring-io/piped-input-stream
    (fn [out-stream]
      (tc/write! tc-dataset out-stream {:file-type :csv}))))

(defn- change-store-to-ring-io-writer
  "Use as argument to `ring-io/piped-input-stream`."
  [change-store data-key ^OutputStream out-stream]
  (with-open [in ^InputStream (store/get-data change-store data-key)]
    (.transferTo in out-stream)))

(defn csv-file-location->dataset [change-store key]
  (with-open [in (store/get-data change-store key)]
    (data.validation/as-dataset in {:file-type :csv})))

(defn op->response-code
  "Takes [s.api/UpsertOp] and returns a HTTP status code (number)."
  [op]
  {:pre [(s.api/upsert-op-valid? op)]}
  (case op
    :create 201
    :update 200
    :noop   200))

(defn uri->id [uri]
  (last (string/split (str uri) #"/")))

(defn uri->context [uri]
  (str (string/join "/" (butlast (string/split (str uri) #"/"))) "/"))

(defn nested-ld-resource-collection [nest-on ld-resources]
  (let [nestk (cmp/expand nest-on)
        nestp #(contains? % nestk)
        roots (filter nestp ld-resources)
        nests (->> (remove nestp ld-resources)
                   (group-by (keyword "@id")))]
    (map (fn [root]
           (let [id (get root nestk)]
             (assoc root
                    nestk (map #(let [at-id (get % (keyword "@id"))]
                                  (assoc % (keyword "@id") (uri->id at-id)))
                               (get nests id)))))
         roots)))

(defn add-base-url [base-url x]
  (assoc x "@context" {"@base" base-url}))

(defn update-releases [series]
  (let [base-url (str "./" (series "@id") "/releases/")]
    (update series
            "dh:hasRelease"
            (partial map (partial add-base-url base-url)))))

(defn get-dataset-series [triplestore system-uris {{:keys [series-slug]} :path-params :as request}]
  (if-let [series (->> (db/get-dataset-series triplestore (su/dataset-series-uri system-uris series-slug))
                       (matcha/index-triples)
                       (triples->ld-resource-collection))]
    (let [[series'] (nested-ld-resource-collection :dh/hasRelease series)
          json-str (-> (json-ld/compact series' (json-ld/context system-uris))
                       (.toString))
          response-body (-> (json/read-value json-str)
                            (update-releases))]
      (as-json-ld {:status 200
                   :body response-body}))
    (errors/not-found-response request)))

(defn put-dataset-series [clock triplestore system-uris {:keys [body-params] :as request}]
  (let [api-params (get-api-params request)
        incoming-jsonld-doc body-params
        {:keys [op jsonld-doc]} (db/upsert-series! clock triplestore system-uris api-params incoming-jsonld-doc)]
    (as-json-ld {:status (op->response-code op)
                 :body jsonld-doc})))

(defn delete-dataset-series [triplestore change-store system-uris {{:keys [series-slug]} :path-params :as request}]
  (if-let [_series (db/get-dataset-series triplestore (su/dataset-series-uri system-uris series-slug))]
    (let [orphaned-change-keys (db/delete-series! triplestore system-uris series-slug)]
      (doseq [change-key orphaned-change-keys]
        (store/-delete change-store change-key))
      {:status 204})
    (errors/not-found-response request)))

(defn get-release
  [triplestore _change-store system-uris
   {{:keys [extension] :as path-params} :path-params
    {release-uri :dh/Release} :datahost.request/uris
    :as request}]
  (if-let [release (->> release-uri
                        (db/get-release-by-uri triplestore)
                        (matcha/index-triples)
                        (triples->ld-resource)
                        (shared/add-dcat:distribution-formats request))]
    (if (= extension "csv")
      (let [change-info (db/get-latest-change-info triplestore release-uri)]
        (cond
          (nil? change-info) (-> (util.response/response "This release has no revisions yet")
                                 (util.response/status 422)
                                 (util.response/header "content-type" "text/plain"))
          :else (let [rev-uri ^URI (:rev change-info)]
                  (-> (util.response/redirect (str (.getPath rev-uri) ".csv"))
                      (shared/set-csvm-header request)))))
      (as-json-ld {:status 200
                   :body (-> (json-ld/compact release (json-ld/context system-uris))
                             (.toString))}))
    (errors/not-found-response request)))


(defn put-release [clock triplestore system-uris {path-params :path-params
                                                  body-params :body-params :as request}]

  (if (db/resource-exists? triplestore (su/dataset-series-uri* system-uris path-params))
    (let [api-params (get-api-params request)
          incoming-jsonld-doc body-params
          {:keys [op jsonld-doc]} (db/upsert-release! clock triplestore system-uris api-params incoming-jsonld-doc)]
      (as-json-ld {:status (op->response-code op)
                   :body jsonld-doc}))
    {:status 422
     :body "Series for this release does not exist"}))

(defn post-release-schema
  [clock triplestore system-uris {path-params :path-params
                                  {schema-file :body} :parameters :as request}]
  {:pre [schema-file]}
  (if (db/resource-exists? triplestore (su/dataset-series-uri* system-uris path-params))
    (let [incoming-jsonld-doc schema-file
          api-params (get-api-params request)]
      (as-> (db/upsert-release-schema! clock triplestore system-uris incoming-jsonld-doc api-params) insert-result
            (as-json-ld {:status (op->response-code (:op insert-result))
                         :body (:jsonld-doc insert-result)})))
    (errors/not-found-response request)))

(defn- get-schema-id [matcha-db]
  ((grafter.matcha.alpha/select-1 [?schema]
                                  [[?schema (cmp/expand :dh/columns) ?col]])
   matcha-db))

(defn get-release-schema
  [triplestore system-uris {path-params :path-params :as request}]
  (let [release-uri (su/dataset-release-uri* system-uris path-params)
        matcha-db (matcha/index-triples (db/get-release-schema-statements triplestore release-uri))]
    (if-let [schema-id (get-schema-id matcha-db)]
      (let [schema-resource (triples->ld-resource matcha-db schema-id)
            csvw-number-uri (cmp/expand :csvw/number)
            columns (->> (box (get schema-resource (cmp/expand :dh/columns)))
                         (map #(triples->ld-resource matcha-db %))
                         (sort-by #(get % csvw-number-uri)))
            schema-ld-with-columns (assoc schema-resource (cmp/expand :dh/columns) columns)]
        (as-json-ld {:status 200
                     :body (-> (json-ld/compact schema-ld-with-columns (json-ld/context system-uris))
                               (.toString))}))
      (errors/not-found-response request))))

(defn- revision-number
  "Returns a number or throws."
  [rev-id]
  (let [path (.getPath ^URI rev-id)]
    (try
      (Long/parseLong (-> (re-find #"^.*/([^/]*)$" path) next first))
      (catch NumberFormatException ex
        (throw (ex-info (format "Could not extract revision number from given id: %s" rev-id)
                        {:revision-id rev-id} ex))))))

(defn get-revision
  [triplestore
   change-store
   system-uris
   {{revision :dh/Revision} :datahost.request/entities
    {release-uri :dh/Release} :datahost.request/uris
    {:strs [accept]} :headers
    :as request}]
  (let [revision-ld (->> (matcha/index-triples revision)
                         (triples->ld-resource)
                         (shared/add-dcat:distribution-formats request))]
    (if (= accept "text/csv")
      (-> {:status 200
           :headers {"content-type" "text/csv"
                     "content-disposition" "attachment ; filename=revision.csv"}
           :body (let [change-infos (db/get-changes-info triplestore release-uri (revision-number (resource/id revision-ld)))]
                  (if (empty? change-infos)
                    "" ;TODO: return table header here, need to get schema first``
                    (let [key (:snapshotKey (last change-infos))]
                      (assert (string? key))
                      (when (nil? key)
                        (throw (ex-info "No snapshot reference for revision" {:revision revision})))
                      (ring-io/piped-input-stream (partial change-store-to-ring-io-writer change-store key)))))}
          (shared/set-csvm-header request))

      (as-json-ld {:status 200
                   :body (-> (json-ld/compact revision-ld (json-ld/context system-uris))
                             (.toString))}))))

(defn- wrap-ld-collection-contents [coll]
  {"https://publishmydata.com/def/datahost/collection-contents" coll})

(defn get-series-list [triplestore system-uris request]
  (let [issued-uri (cmp/expand :dcterms/issued)
        series (->> (db/get-all-series triplestore)
                    (matcha/index-triples)
                    (triples->ld-resource-collection)
                    (nested-ld-resource-collection :dh/hasRelease)
                    (sort-by #(get % issued-uri) #(compare %2 %1)))
        json-str (-> (wrap-ld-collection-contents series)
                     (json-ld/compact (json-ld/context system-uris))
                     (.toString))
        response-body (-> (json/read-value json-str)
                          (update "contents" (partial map update-releases)))]
    (as-json-ld {:status 200
                 :body response-body})))

(defn get-revision-list [triplestore system-uris {path-params :path-params :as request}]
  (let [revisions (->> (su/dataset-release-uri* system-uris path-params)
                       (db/get-revisions triplestore)
                       (matcha/index-triples)
                       (triples->ld-resource-collection)
                       (map (partial shared/add-dcat:distribution-formats request))
                       (sort-by (comp str (keyword "@id")) #(compare %2 %1)))
        response-body (-> (wrap-ld-collection-contents revisions)
                          (json-ld/compact (json-ld/context system-uris))
                          (.toString))]
    (as-json-ld {:status 200
                 :body response-body})))

(defn get-release-list [triplestore system-uris {{:keys [series-slug]} :path-params :as request}]
  (let [issued-uri (cmp/expand :dcterms/issued)
        releases (->> (db/get-releases triplestore (su/dataset-series-uri system-uris series-slug))
                      (matcha/index-triples)
                      (triples->ld-resource-collection)
                      (map (partial shared/add-dcat:distribution-formats request))
                      (sort-by #(get % issued-uri) #(compare %2 %1)))
        response-body (-> (wrap-ld-collection-contents releases)
                          (json-ld/compact (json-ld/context system-uris))
                          (.toString))]
    (as-json-ld {:status 200
                 :body response-body})))

(defn post-revision [triplestore system-uris {{:keys [series-slug release-slug]} :path-params
                                              router :reitit.core/router
                                              body-params :body-params :as request}]
  (let [api-params (get-api-params request)
        release-uri (su/dataset-release-uri system-uris (su/dataset-series-uri system-uris series-slug) release-slug)]
    (if (db/resource-exists? triplestore release-uri)
      (let [next-revision-id (db/fetch-next-child-resource-number triplestore release-uri :dh/hasRevision)
            revision-uri (su/revision-uri system-uris series-slug release-slug next-revision-id)
            {:keys [jsonld-doc resource-id]} (db/insert-revision! triplestore api-params
                                                                  body-params (su/rdf-base-uri system-uris)
                                                                  release-uri revision-uri next-revision-id)]
        (as-json-ld {:status 201
                     :headers {"Location" (-> (rc/match-by-name router
                                                                :tpximpact.datahost.ldapi.router/revision
                                                                {:series-slug series-slug
                                                                 :release-slug release-slug
                                                                 :revision-id resource-id})
                                              (rc/match->path))}
                     :body jsonld-doc}))

      {:status 422
       :body "Release for this revision does not exist"})))

(defn- validate-incoming-change-data
  "Returns a map {:dataset ?DATASET (optional-key :error-response) ..., row-schema MALLI-SCHEMA},
  containing :error-response entry when validation failed."
  [release-schema appends]
  (let [row-schema (data.validation/make-row-schema release-schema)
        {:keys [explanation dataset]}
        (try
          (->(data.validation/as-dataset appends {:enforce-schema row-schema})
             (data.validation/validate-dataset row-schema {:fail-fast? true}))
          (catch clojure.lang.ExceptionInfo ex
            (if (= ::data.validation/dataset-creation (-> ex ex-data :type))
              {:explanation (ex-message ex)}
              (throw ex))))]
    (cond-> {:row-schema row-schema}
      (some? dataset) (assoc :dataset dataset)
      (some? explanation) (assoc :error-response
                                 {:status 400
                                  :body {:message "Invalid data"
                                         :explanation explanation
                                         :column-names (data.validation/row-schema->column-names row-schema)}}))))

(defn ->byte-array-input-stream [input-stream]
  (with-open [intermediate (ByteArrayOutputStream.)]
    (io/copy input-stream intermediate)
    (ByteArrayInputStream. (.toByteArray intermediate))))

(defn post-change
  [triplestore
   change-store
   system-uris
   change-kind
   {router :reitit.core/router
    {:keys [series-slug release-slug revision-id] :as path-params} :path-params
    query-params :query-params
    appends :body
    {release-uri :dh/Release :as request-uris} :datahost.request/uris
    :as request}]
  (let [;; validate incoming data
        release-schema (db/get-release-schema triplestore release-uri)
        ;; If there's no release-schema, then no validation happens, and db/insert
        ;; is a NOOP. This fails further down in the `let` body in
        ;; #'internal/post-change--generate-csv-snapshot
        ;; We don't need to proceed further if there's no release-schema!
        _ (when (nil? release-schema)
            (throw (ex-info (str "No release schema found for: " release-uri)
                            {:release-uri release-uri})))
        appends ^InputStream (->byte-array-input-stream appends)
        insert-req (store/make-insert-request! change-store appends)
        {validation-err :error-response
         row-schema :row-schema
         change-ds :dataset} (some-> release-schema (validate-incoming-change-data appends))
        _ (.reset appends)
        content-type (util.request/content-type request)
        ;; insert relevant triples
        {:keys [inserted-jsonld-doc
                change-id
                change-uri
                message]} (when-not validation-err
                            (db/insert-change! triplestore
                                               system-uris
                                               {:api-params (assoc (get-api-params request)
                                                                   :format content-type)
                                                :ld-root (su/rdf-base-uri system-uris)
                                                :store-key (:key insert-req)
                                                :datahost.change/kind change-kind
                                                :datahost.request/uris request-uris}))]
    (log/info (format "post-change: '%s' validation: found-schema? = %s, change-valid? = %s, insert-ok? = %s"
                      change-uri (some? release-schema) (nil? validation-err) (nil? message)))
    (cond
      (some? validation-err) validation-err

      (some? message) {:status 422 :body message}

      :else
      (do
        ;; store the change
        (store/request-data-insert change-store insert-req)
        (log/debug (format "post-change: '%s' stored-change: '%s'" (.getPath change-uri) (:key insert-req)))

        ;; generate and store the dataset snapshot
        (let [{:keys [new-snapshot-key] :as snapshot-result}
              (internal/post-change--generate-csv-snapshot
               {:triplestore triplestore
                :change-store change-store
                :system-uris system-uris}
               {:path-params path-params
                :change-kind change-kind
                :change-id change-id
                :dataset change-ds
                :row-schema row-schema
                "dcterms:format" content-type})]
          (log/debug (format "post-change: '%s' stored snapshot" (.getPath change-uri))
                     {:new-snapshot-key new-snapshot-key})
          (db/tag-with-snapshot triplestore change-uri snapshot-result))

        (as-json-ld {:status 201
                     :headers {"Location" (-> (rc/match-by-name router
                                                                :tpximpact.datahost.ldapi.router/commit
                                                                {:series-slug series-slug
                                                                 :release-slug release-slug
                                                                 :revision-id revision-id
                                                                 :commit-id change-id})
                                              (rc/match->path))}
                     :body inserted-jsonld-doc})))))

(defn change->csv-stream [change-store change]
  (let [appends (get change (cmp/expand :dh/updates))]
    (when-let [dataset (csv-file-location->dataset change-store appends)]
      (write-dataset-to-outputstream dataset))))

(defn get-change [triplestore change-store system-uris {{:keys [series-slug release-slug revision-id commit-id]} :path-params
                        {:strs [accept]} :headers :as request}]
  (if-let [change (->> (su/commit-uri system-uris series-slug release-slug revision-id commit-id)
                       (db/get-change triplestore)
                       matcha/index-triples
                       triples->ld-resource)]
    (if (= accept "application/json")
       (as-json-ld{:status 200
       :body (-> (json-ld/compact change (json-ld/context system-uris))
                             (.toString))})
      {:status 200
       :headers {"content-type" "text/csv"
                 "content-disposition" "attachment ; filename=change.csv"}
       :body (or (change->csv-stream change-store change) "")}


      )
    (errors/not-found-response request)))
