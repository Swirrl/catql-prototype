(ns tpximpact.datahost.ldapi.routes.release
  (:require
   [tpximpact.datahost.ldapi.handlers :as handlers]
   [tpximpact.datahost.ldapi.routes.copy :as copy]
   [tpximpact.datahost.ldapi.routes.shared :as routes-shared]
   [tpximpact.datahost.ldapi.schemas.release :as schema]))

(defn get-release-route-config [triplestore]
  {:summary copy/get-release-summary
   :handler (partial handlers/get-release triplestore)
   :parameters {:path {:series-slug string?
                       :release-slug string?}}
   :responses {200 {:body string?}
               404 {:body [:re "Not found"]}}})

(defn put-release-route-config [clock triplestore]
  {:summary copy/put-release-summary
   :handler (partial handlers/put-release clock triplestore)
   :parameters {:body routes-shared/JsonLdSchema
                :path {:series-slug string?
                       :release-slug string?}
                :query schema/ApiQueryParams}
   :responses {200 {:description copy/put-release-200-desc
                    :body string?}
               201 {:description copy/put-release-201-desc
                    :body string?}
               500 {:description copy/internal-server-error-desc
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})
