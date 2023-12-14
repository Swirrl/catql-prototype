(ns tpximpact.datahost.ldapi.datagen.reports
  ;;Test dataset generation for load/performace experiments and testing. 
  ;; Example usage-  Creating a dataset of 100,000 rows and 15 columns:
  ;; (gen/get-data 15 100000 "test/tpximpact/datahost/ldapi/datagen/hurl_tests/" "FILENAME")
  (:require [tpximpact.datahost.ldapi.datagen.load-test :as load-test]))


(def seed (System/currentTimeMillis))
;; Documentation

;; GENERATE DATA
;; - (load-test/generate x y repeats filename seed)
;; x: int number of desired columns
;; y: int number of desired rows
;; repeats: int (only relevant for AD)
;; filename: string with no spaces
;; seed: int used for regenerating the same dataset again

;; This will generate a schema and corresponding csv in datagen/issue-310
;; It will also generate two hurl scripts:
;; A: Append, tests a single append
;; AD: Append Delete, appends and deletes the csv for as many repeats as specified

;; GENERATE SPLIT DATA
;; - (load-test/generate-split x y splits filename seed repeats)
;; splits: int how many csv's the data should be divided into

;; This will generate a schema and a number corresponding csv's in datagen/issue-310
;; It will also generate a hurl script:
;; CA: Consecutive Appends, posts all the split csvs one after the other

;; NOTES
;; - The command for running the hurl scripts can be found at the top of each one
;; - It takes a few minutes to create data for 10mil, let it run or the csv will be incomplete.

;; report on some basic cases -

;; (load-test/generate 15 10000000 1 "tenmil" seed)
;; Append
;; Error 500 - Error1 - found on issue-310 on Github

;; (load-test/generate-split 15 10000000 10 "tenmil" seed)
;; Consecutive Appends
;; Error 500 on 3rd Append ~ Error 1.

;; (load-test/generate-split 15 10000000 100 "tenmil" seed)
;; Consecutive Appends
;; Error 500 on 37th Append ~ Error 1.

;; (load-test/generate 15 5000000 1 "fivemil" seed)
;; Append 
;; Error 500 ~ Error 1.

;; (load-test/generate 15 4000000 1 "fourmil" seed)
;; Append 
;; Error 500 ~ Error 1.

;; (load-test/generate 15 3000000 1 "threemil" seed)
;; Append
;; Successful

;; Append Delete
;; Error 500 on the 1st delete ~ Error 1.

;; (load-test/generate 15 2000000 10 "twomil" seed)
;; Append
;; Successful

;; Append Delete (cycle)
;; Inconsistent resutls - sometimes 500 on the 1st or 4th delete
;; other times it can do 4 + cycles successfully ~ Error 1.

;; (load-test/generate 15 1000000 10 "onemil" seed)
;; Append Delete (cycle)
;; Successful for 10 cycles (takes a long while)
