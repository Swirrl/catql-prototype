(ns tpximpact.datahost.ldapi.schemas.api
  "API specific schemas"
  (:require
   [malli.core :as m]))

(def UpsertOp 
  "Enum for semantics of the upsert operation.

  Motivation: we may want to to convey to the client what actually
  happened (in a form of meaningful HTTP status code, for example)."
  [:enum :noop :create :update])

(def upsert-op-valid? (m/validator UpsertOp))
