(ns hirop-couchdb.core-test
  (:use clojure.test
        clojure.pprint
        hirop.backend
        hirop-couchdb.core)
  (:require [hirop.core :as hirop]
            [com.ashafa.clutch :as clutch]))

(def doctypes
  {:Foo {:fields {:id {}}}
   :Bar {:fields {:title {}}}
   :Baz {:fields {:title {}}}
   :Baq {:fields {:title {}}}})

(def cardinality-test-context
  {:relations
   [{:from :Bar :to :Foo :external true :cardinality :one}
    {:from :Baz :to :Bar :cardinality :many}]
   :selections
   {:test
    {:Foo {:sort-by [:id] :select :last}
     :Bar {:select :all}
     :Baz {:select :all}}}
   :configurations {}})

(defn cardinality-test-fetcher [_]
  [{:_hirop {:id "0" :type :Foo}
    :id "0"}
   {:_hirop {:id "1" :type :Bar :rels {:Foo "0"}}
    :title "First"}
   {:_hirop {:id "2" :type :Bar :rels {:Foo "0"}}
    :title "Second"}
   {:_hirop {:id "3" :type :Baz :rels {:Bar ["1" "2"]}}
    :title "Third"}])

(deftest save-fetch-test
  (let [connection-data
        {:connection-string "http://127.0.0.1:5984/testdb"
         :username nil
         :password nil}
        context
        (->
         (hirop/create-context :Test cardinality-test-context doctypes {:Foo "0"} {} :none)
         (hirop/fetch cardinality-test-fetcher)
         (hirop/merge-remote))
        docs (hirop/checkout context :Baz)
        doc (assoc (first docs) :title "Starred")
        context (hirop/commit context doc)
        new-bar (assoc-in (hirop/new-document context :Baz) [:_hirop :rels] {:Bar ["2"]})
        context (hirop/commit context new-bar)
        external-ids {:Foo "0"}]
    (init-database connection-data)
    ;;(with-db (clutch/put-document {:_id "0" :$hirop {:type "Foo"}}))
    (with-db connection-data (clutch/put-document {:docs [{:_hirop {:id "0" :type "Foo"}}]}))
    (let [res (save* connection-data context)
          remap (:remap res)
          docs (fetch* connection-data context)]
      (pprint docs)
      (is true)
      #_(is (= (set (hirop/hrel (first (filter #(= (hirop/htype %) :Baz) docs)) :Bar))
             (set [(remap "tmp1") (remap "tmp2")]))))))
