(ns ^:hurl tpximpact.datahost.ldapi.integration-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [tpximpact.test-helpers :as th]
   [tpximpact.datahost.ldapi.test-util.hurl :as hurl :refer [hurl]]))

(deftest ^:hurl minimal-tests
  (testing "verify we plain `hurl` call works"
    (th/with-system-and-clean-up {http-port :tpximpact.datahost.ldapi.jetty/http-port :as sys}
      (let [variables {:host_name (str "localhost:" http-port)
                       :series (str "my-series-" (random-uuid))
                       :auth_token "ignore"}]

        (let [result (hurl {:variables variables :script "hurl-scripts/int-001.hurl"
                            :report-junit "test-results/hurl.xml"})]
          (is (= 0 (:exit result))))))))

(deftest ^:hurl integration-tests
  (testing "all ./hurl-scripts"
    (th/with-system-and-clean-up {http-port :tpximpact.datahost.ldapi.jetty/http-port :as sys}
      (let [variables {:host_name (str "localhost:" http-port)
                       :series :hurl.variable.named/series
                       :release :hurl.variable.named/release
                       :auth_token "ignore"}
            result (hurl/run-directory "hurl-scripts"
                                       {:variables variables
                                        :report-junit "test-results/hurl-regression-tests.xml"
                                        :report-html "test-report"})]
        (is (hurl/success? result))))))
