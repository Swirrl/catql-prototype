(ns tpximpact.datahost.ldapi.routes.series
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]
   [tpximpact.datahost.ldapi.schemas.series :as schema]))

(defn get-series-route-config [triplestore]
  {:summary "Retrieve metadata for an existing dataset-series"
   :description "A dataset series is a named dataset regardless of schema, methodology or compatibility changes"
   :handler (partial handlers/get-dataset-series triplestore)
   :parameters {:path {:series-slug string?}}
   :responses {200 {:body string?}
               404 {:body [:re "Not found"]}}})

(defn put-series-route-config [clock triplestore]
  {:summary "Create or update metadata on a dataset-series"
   :handler (partial handlers/put-dataset-series clock triplestore)
   :parameters {:body routes-shared/JsonLdSchema
                :path {:series-slug string?}
                :query schema/ApiQueryParams}
   :responses {200 {:description "Series already existed and was successfully updated"
                    :body routes-shared/ResourceSchema}
               201 {:description "Series did not exist previously and was successfully created"
                    :body routes-shared/ResourceSchema}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})

(defn delete-series-route-config [triplestore]
  {:summary "Delete a series and all its child resources"
   :handler (partial handlers/delete-dataset-series triplestore)
   :parameters {:path {:series-slug string?}}
   :responses {204 {:description "Series existed and was successfully deleted"}
               404 {:description "Series does not exist"}}}
  )
