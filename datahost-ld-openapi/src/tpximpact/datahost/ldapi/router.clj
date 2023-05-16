(ns tpximpact.datahost.ldapi.router
  (:require
   [com.yetanalytics.flint :as fl]
   [integrant.core :as ig]
   [reitit.dev.pretty :as pretty]
   [reitit.interceptor.sieppari :as sieppari]
   [reitit.openapi :as openapi]
   [reitit.ring :as ring]
   [reitit.ring.coercion :as coercion]
   [reitit.ring.middleware.exception :as exception]
   [reitit.ring.middleware.multipart :as multipart]
   [reitit.ring.middleware.muuntaja :as muuntaja]
   [reitit.ring.middleware.parameters :as parameters]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [tpximpact.datahost.ldapi.native-datastore :as datastore]
   [reitit.coercion.malli :as rcm]
   [muuntaja.core :as m]
   [malli.util :as mu]
   [tpximpact.datahost.ldapi.handlers :as handlers]))

(defn query-example [triplestore request]
    ;; temporary code to facilitate end-to-end service wire up
  (let [qry {:prefixes {:dcat "<http://www.w3.org/ns/dcat#>"
                        :rdfs "<http://www.w3.org/2000/01/rdf-schema#>"}
             :select '[?label ?g]
             :where [[:graph datastore/background-data-graph
                      '[[?datasets a :dcat/Catalog]
                        [?datasets :rdfs/label ?label]]]]}

        results (datastore/eager-query triplestore (fl/format-query qry :pretty? true))]
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (-> results first :label)}))

(def json-string-keys-muuntaja-coercer
  (m/create
   (-> m/default-options
       (assoc-in [:formats "application/json" :decoder-opts] {:decode-key-fn str})
       (assoc-in [:formats "application/json" :encoder-opts] {:encode-key-fn str}))))

(defn router [triplestore]
  (ring/router
   [["/triplestore-query" ;; TODO remove this route when we have real ones using the triplestore
     {:get {:nodoc true}
      :handler #(query-example triplestore %)}]
    ["/openapi.json"
     {:get {:no-doc true
            :openapi {:openapi "3.0.0"
                      :info {:title "Prototype OpenData API"
                             :description "blah blah blah. [a link](http://foo.com/)
* bulleted
* list
* here "
                             :version "0.0.1"}
                      ;; used in /secure APIs below
                      :components {:securitySchemes {"auth" {:type :apiKey
                                                             :in :header
                                                             :name "Example-Api-Key"}}}}
            :handler (openapi/create-openapi-handler)}}]

    ["/data" {:tags ["linked data api"]}
     ["/:dataset-series"
      {:muuntaja json-string-keys-muuntaja-coercer
       :get {:summary "Retrieve metadata for an existing dataset-series"
             :description "blah blah blah. [a link](http://foo.com/)
* bulleted
* list
* here"
             :parameters {:path {:dataset-series string?}}
             :responses {200 {:body {:name string?, :size int?}}}
             :handler handlers/get-dataset-series}
       :put {:summary "Create or update metadata on a dataset-series"
             :parameters {:body [:maybe
                                 [:map
                                  ["dcterms:title" string?]
                                  ["@context" [:or :string
                                               [:tuple :string [:map
                                                                ["@base" string?]]]]]]]
                          :path {:dataset-series string?}
                          :query [:map
                                  [:title {:title "X parameter"
                                           :description "Description for X parameter"
                                           :optional true} string?]
                                  [:description {:optional true} string?]]}
             :responses {200 {:body {:name string?, :size int?}}}
             :handler handlers/put-dataset-series}}]]]

   {;;:reitit.middleware/transform dev/print-request-diffs ;; pretty diffs
    ;;:validate spec/validate ;; enable spec validation for route data
    ;;:reitit.spec/wrap spell/closed ;; strict top-level validation
    :exception pretty/exception
    :data {:coercion (rcm/create
                      {;; set of keys to include in error messages
                       :error-keys #{#_:type :coercion :in :schema :value :errors :humanized #_:transformed}
                       ;; schema identity function (default: close all map schemas)
                       :compile mu/closed-schema
                       ;; strip-extra-keys (effects only predefined transformers)
                       :strip-extra-keys true
                       ;; add/set default values
                       :default-values true
                       ;; malli options
                       :options nil})
           :muuntaja m/instance
           :middleware [;; swagger & openapi
                        swagger/swagger-feature
                        openapi/openapi-feature
                        ;; query-params & form-params
                        parameters/parameters-middleware
                        ;; content-negotiation
                        muuntaja/format-negotiate-middleware
                        ;; encoding response body
                        muuntaja/format-response-middleware
                        ;; exception handling
                        exception/exception-middleware
                        ;; decoding request body
                        muuntaja/format-request-middleware
                        ;; coercing response bodys
                        coercion/coerce-response-middleware
                        ;; coercing request parameters
                        coercion/coerce-request-middleware
                        ;; multipart
                        multipart/multipart-middleware]}}))

(defmethod ig/init-key :tpximpact.datahost.ldapi.router/handler
  [_ {:keys [api-route-data triplestore] ::ring/keys [opts default-handlers handlers]}]
  (ring/ring-handler
   (router triplestore)
   (ring/routes
    (swagger-ui/create-swagger-ui-handler
     {:path "/"
      :config {:validatorUrl nil
               :urls [{:name "swagger", :url "swagger.json"}
                      {:name "openapi", :url "openapi.json"}]
               :urls.primaryName "openapi"
               :operationsSorter "alpha"}})
    (ring/create-default-handler))
   {:executor sieppari/executor}))
