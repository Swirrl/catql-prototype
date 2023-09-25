(ns tpximpact.datahost.ldapi.delta-handler
  (:require [clojure.string :as str]
            [ring.util.io :as ring-io]
            [tablecloth.api :as tc]
            [tpximpact.datahost.ldapi.util.data-validation :as data-validation])
  (:import (net.openhft.hashing LongHashFunction)))

(def default-schema
  {:columns [{:name "Area"
              :datatype :string
              :coltype :qb/dimension
              :valid-value? (fn [v] (= 9 (count v)))}
             {:name "Period"
              :datatype :period
              :coltype :qb/dimension
              :valid-value? (constantly true)}
             {:name "Sex"
              :datatype :string
              :coltype :qb/dimension
              :valid-value? #{"Male" "Female" "Other"}}
             {:name "Life Expectancy"
              :datatype :double
              :coltype :qb/measure
              :valid-value? double?}]})

(def ^:private long-hash-fn-instance (LongHashFunction/xx128low))

(def ^:private right-column-prefix-pattern #"^right.")

(def id-columns
  (->> default-schema
       :columns
       (filter (comp #{:qb/dimension :qb/attribute} :coltype))
       (map :name)))

(defn hash-chars [^String s]
  (.hashChars long-hash-fn-instance s))

(defn hash-fn [& dims]
  (-> (apply str (interpose "|" dims))
      hash-chars))

(defn add-ids [ds]
  (-> ds
      ;; adds a new column named "ID" by applying `hash-fn` to the
      ;; vals in the selected columns
      (tc/map-columns "ID" id-columns hash-fn)))

(defn get-measure-column-name [schema]
  (->> schema
       :columns
       (some #(when ((comp #{:qb/measure} :coltype) %) %))
       :name))

(defn- rename-column-mappings [pattern column-names]
  (->> (map #(vector % (str/replace % pattern ""))
            column-names)
       (into {})))

(defn write-dataset-to-outputstream [tc-dataset]
  (ring-io/piped-input-stream
   (fn [out-stream]
     (tc/write! tc-dataset out-stream {:file-type :csv}))))

(defn derive-deltas [original-ds new-ds]
  (let [old (add-ids original-ds)
        new (add-ids new-ds)
        measure-column-name (get-measure-column-name default-schema)
        right-measure-column-name (str "right." measure-column-name)
        column-names (cons "ID" (tc/column-names original-ds))
        right-column-names (map #(str "right." %) column-names)
        labelled-ds (tc/map-columns (tc/full-join old new "ID")
                                    :status
                                    [measure-column-name right-measure-column-name]
                                    (fn [old-measure-val new-measure-val]
                                      (cond
                                        (nil? old-measure-val) :added
                                        (nil? new-measure-val) :deleted
                                        (not= old-measure-val new-measure-val) :modified)))

        added (-> (tc/select-rows labelled-ds (comp #(= :added %) :status))
                  (tc/select-columns (cons :status right-column-names))
                  (tc/rename-columns (rename-column-mappings right-column-prefix-pattern right-column-names)))

        deleted (-> (tc/select-rows labelled-ds (comp #(= :deleted %) :status))
            (tc/select-columns (cons :status column-names)))

        modified-added (-> labelled-ds
                           (tc/select-rows (comp #(= :modified %) :status))
                           (tc/select-columns (conj column-names (str "right." measure-column-name)))
                           (tc/map-columns :status [right-measure-column-name] (constantly :added))
                           (tc/drop-columns measure-column-name)
                           (tc/rename-columns (rename-column-mappings right-column-prefix-pattern right-column-names)))
        modified-deleted (-> labelled-ds
                             (tc/select-rows (comp #(= :modified %) :status))
                             (tc/select-columns column-names)
                             (tc/map-columns :status [measure-column-name] (constantly :deleted)))]
    (tc/concat deleted
               modified-deleted
               modified-added
               added)))

(defn post-delta-files
  [{{{:keys [base-csv delta-csv]} :multipart} :parameters :as _request}]
  (let [diff-results (derive-deltas (data-validation/slurpable->dataset (:tempfile base-csv)
                                                                        {:file-type :csv :encoding "UTF-8"})
                                    (data-validation/slurpable->dataset (:tempfile delta-csv)
                                                                        {:file-type :csv :encoding "UTF-8"}))]
    {:status 200
     :body (write-dataset-to-outputstream diff-results)}))

(defn delta-tool-route-config []
  {:handler (partial post-delta-files)
   :parameters {:multipart [:map
                            [:base-csv reitit.ring.malli/temp-file-part]
                            [:delta-csv reitit.ring.malli/temp-file-part]]}
   :openapi {:security [{"basic" []}]}
   :responses {201 {:description "Differences between input files calculated"
                    :content {"application/json"
                              {:body string?}}
                    ;; headers is not currently supported
                    :headers {"Location" string?}}
               500 {:description "Internal server error"
                    :body [:map
                           [:status [:enum "error"]]
                           [:message string?]]}}})

; Curl command used to test the delta route:
;
; curl -X 'POST' 'http://localhost:3000/delta' -H 'accept: application/json' \
;   -H 'Content-Type: multipart/form-data' \
;   -F 'base-csv=@./env/test/resources/test-inputs/delta/orig.csv;type=text/csv' \
;   -F 'delta-csv=@./env/test/resources/test-inputs/delta/new.csv;type=text/csv' \
;   --output ./deltas.csv
;
