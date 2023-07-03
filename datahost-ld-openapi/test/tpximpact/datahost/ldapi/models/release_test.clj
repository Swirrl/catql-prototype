(ns tpximpact.datahost.ldapi.models.release-test
  (:require
    [clj-http.client :as http]
    [clojure.data.json :as json]
    [clojure.test :refer [deftest is testing] :as t]
    [grafter-2.rdf4j.repository :as repo]
    [tpximpact.datahost.ldapi.router :as router]
    [tpximpact.datahost.time :as time]
    [tpximpact.test-helpers :as th]
    [tpximpact.datahost.ldapi.models.release :as sut])
  (:import [clojure.lang ExceptionInfo]))

(defn- put-series [put-fn]
  (let [jsonld {"@context"
                ["https://publishmydata.com/def/datahost/context"
                 {"@base" "https://example.org/data/"}]
                "dcterms:title" "A title"}]
    (put-fn "/data/new-series"
            {:content-type :json
             :body (json/write-str jsonld)})))

(defn- create-series [handler]
  (let [request-json {"dcterms:title" "A title" "dcterms:description" "Description"}
        request {:uri "/data/new-series"
                 :request-method :put
                 :headers {"content-type" "application/json"}
                 :body (json/write-str request-json)}
        response (handler request)
        series-doc (json/read-str (:body response))]
    (get series-doc "@id")))

(defn- temp-repo []
  (repo/sparql-repo "http://localhost:5820/test/query" "http://localhost:5820/test/update"))

(t/deftest put-release-create-test
  (let [repo (repo/sail-repo)
        t (time/parse "2023-07-03T11:16:16Z")
        clock (time/manual-clock t)
        handler (router/handler clock repo (atom {}))
        series-slug (create-series handler)

        request-json {"dcterms:title" "Release title" "dcterms:description" "Description"}
        release-request {:uri (format "/data/%s/release/test-release" series-slug)
                         :request-method :put
                         :headers {"content-type" "application/json"}
                         :body (json/write-str request-json)}
        {:keys [body status] :as response} (handler release-request)
        release-doc (json/read-str body)]
    (t/is (= status 201))

    (t/is (= "Release title" (get release-doc "dcterms:title")))
    (t/is (= "Description" (get release-doc "dcterms:description")))
    (t/is (= (str t) (get release-doc "dcterms:issued")))
    (t/is (= (str t) (get release-doc "dcterms:modified")))
    (t/is (= (format "https://example.org/data/%s" series-slug) (get release-doc "dcat:inSeries")))))

(deftest round-tripping-release-test
  (th/with-system-and-clean-up {{:keys [GET PUT]} :tpximpact.datahost.ldapi.test/http-client
                                :as sys}
    (testing "Fetching a release for a series that does not exist returns 'not found'"
      (try
        
        (GET "/data/does-not-exist/release/release-1")

        (catch Throwable ex
          (let [{:keys [status body]} (ex-data ex)]
            (is (= 404 status))
            (is (= "Not found" body))))))

    (testing "Fetching a release that does not exist returns 'not found'"
      (try
        (GET "/data/new-series/release/release-1")

        (catch Throwable ex
          (let [{:keys [status body]} (ex-data ex)]
            (is (= 404 status))
            (is (= "Not found" body))))))

    (testing "Creating a release for a series that is not found fails gracefully"
      (let [jsonld {"@context"
                    ["https://publishmydata.com/debf/datahost/context"
                     {"@base" "http://example.org/data/"}]
                    "dcterms:title" "Example Release"}]
        (try
          (PUT "/data/new-series/release/release-1"
               {:content-type :json
                :body (json/write-str jsonld)})

          (catch Throwable ex
            (let [{:keys [status body]} (ex-data ex)]
              (is (= 422 status))
              (is (= "Series for this release does not exist" body)))))))

    (put-series PUT)

    (let [request-ednld {"@context"
                         ["https://publishmydata.com/def/datahost/context"
                          {"@base" "https://example.org/data/new-series/"}]
                         "dcterms:title" "Example Release"}
          normalised-ednld {"@context"
                            ["https://publishmydata.com/def/datahost/context"
                             {"@base" "https://example.org/data/new-series/"}],
                            "dcterms:title" "Example Release",
                            "@type" "dh:Release"
                            "@id" "release-1",
                            "dcat:inSeries" "../new-series"}]

      (testing "Creating a release for a series that does exist works"
        (let [response (PUT "/data/new-series/release/release-1"
                            {:content-type :json
                             :body (json/write-str request-ednld)})
              body (json/read-str (:body response))]
          (is (= 201 (:status response)))
          (is (= normalised-ednld (dissoc body "dcterms:issued" "dcterms:modified")))
          (is (= (get body "dcterms:issued")
                 (get body "dcterms:modified")))))

      (testing "Fetching a release that does exist works"
        (let [response (GET "/data/new-series/release/release-1 ")
              body (json/read-str (:body response))]
          (is (= 200 (:status response)))
          (is (= normalised-ednld (dissoc body "dcterms:issued" "dcterms:modified")))))

      (testing "A release can be updated, query params take precedence"
        (let [{body-str-before :body} (GET "/data/new-series/release/release-1")
              {:keys [body] :as response} (PUT (str "/data/new-series"
                                                    "/release/release-1?title=A%20new%20title")
                                               {:content-type :json
                                                :body (json/write-str request-ednld)})
              body-before (json/read-str body-str-before)
              body (json/read-str body)]
          (is (= 200 (:status response)))
          (is (= "A new title" (get body "dcterms:title")))
          (is (= (get body-before "dcterms:issued")
                 (get body "dcterms:issued")))
          (is (not= (get body "dcterms:modified")
                    (get body-before "dcterms:modified")))

          (testing "No update when query params same as in existing doc"
            (let [response (PUT (str "/data/new-series"
                                     "/release/release-1?title=A%20new%20title")
                                {:content-type :json
                                 :body nil})
                  body' (-> response :body json/read-str)]
              (is (= 200 (:status response)))
              (is (= "A new title" (get body' "dcterms:title")))
              (is (= (select-keys body ["dcterms:issued" "dcterms:modified"])
                     (select-keys body' ["dcterms:issued" "dcterms:modified"]))
                  "The document shouldn't be modified"))))))))

(deftest normalise-release-test
  (testing "invalid cases"
    (testing "missing :series-slug"
      (is (thrown? ExceptionInfo
                   (sut/normalise-release "https://example.org/data/series-1"
                                          {}
                                          {})))

      ;; Invalid for PUT as slug will be part of URI
      (is (thrown? ExceptionInfo
                   (sut/normalise-release "https://example.org/data/series-1"
                                          {}
                                          {"@id" "release-1"}))))

    (testing "missing :release-slug"
      (is (thrown? ExceptionInfo
                   (sut/normalise-release "https://example.org/data/series-1"
                                          {:series-slug "my-dataset-series"}
                                          {}))))

    (testing "different base is invalid"
      (is (thrown? ExceptionInfo
                   (sut/normalise-release "https://example.org/data/series-1"
                                          {:series-slug "my-dataset-series"}
                                          {"@context" ["https://publishmydata.com/def/datahost/context"
                                                       {"@base" "https://different.base/is/invalid/"}]}))))

    (testing "@id must be slugised in the doc"
      (is (thrown? clojure.lang.ExceptionInfo
                   (sut/normalise-release "https://example.org/data/series-1"
                                          {:series-slug "my-dataset-series"
                                           :release-slug "2023"}
                                          {"@context" ["https://publishmydata.com/def/datahost/context",
                                                       {"@base" "https://example.org/data/"}],
                                           "@id" "https://example.org/data/my-dataset-series"}))
          "@id usage in the series document is (for now) restricted to be in slugised form only")))

  (testing "valid cases"
    (let [returned-value (sut/normalise-release "https://example.org/data/series-1"
                                                {:series-slug "my-dataset-series"
                                                 :release-slug "release-1"}
                                                {})]
      (testing "canonicalisation works"
        (is (= {"@context"
                ["https://publishmydata.com/def/datahost/context"
                 {"@base" "https://example.org/data/series-1"}]
                "@id" "release-1"
                "@type" "dh:Release"
                "dcat:inSeries" "../my-dataset-series"}
               returned-value)))

      (testing "canonicalisation is idempotent"
        (is (= returned-value (sut/normalise-release "https://example.org/data/series-1"
                                                     {:series-slug "my-dataset-series"
                                                      :release-slug "release-1"}
                                                     returned-value))))

      (testing "with title metadata"
        (is (= {"@context"
                ["https://publishmydata.com/def/datahost/context"
                 {"@base" "https://example.org/data/series-1"}]
                "@id" "release-1"
                "@type" "dh:Release"
                "dcterms:title" "My Release"
                "dcat:inSeries" "../my-dataset-series"}
               (sut/normalise-release "https://example.org/data/series-1"
                                      {:series-slug "my-dataset-series"
                                       :release-slug "release-1"}
                                      {"@context" ["https://publishmydata.com/def/datahost/context"
                                                    {"@base" "https://example.org/data/series-1"}]
                                        "dcterms:title" "My Release"})))))))
