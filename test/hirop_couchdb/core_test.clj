(ns hirop-couchdb.core-test
  (:use clojure.test
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
  {:documents
   [{:_hirop {:id "0" :type :Foo}
     :id "0"}
    {:_hirop {:id "1" :type :Bar :rels {:Foo "0"}}
     :title "First"}
    {:_hirop {:id "2" :type :Bar :rels {:Foo "0"}}
     :title "Second"}
    {:_hirop {:id "3" :type :Baz :rels {:Bar ["1" "2"]}}
     :title "Third"}]})

(deftest save-fetch-test
  (let [connection-data
        {:protocol "http"
         :host "localhost"
         :port 5984
         :path "/testdb"
         :username nil
         :password nil}
        context
        (->
         (hirop/init-context :Test cardinality-test-context doctypes {:Foo "0"} {} :none)
         (hirop/fetch cardinality-test-fetcher)
         (hirop/merge-remote))
        docs (hirop/checkout context :Baz)
        doc (assoc (first docs) :title "Starred")
        context (hirop/commit context doc)
        new-bar (assoc-in (hirop/new-document context :Baz) [:_hirop :rels] {:Bar ["2"]})
        new-bar-id (hirop/hid new-bar)
        context (hirop/commit context new-bar)
        external-ids {:Foo "0"}]
    (with-db connection-data (clutch/delete-database))
    (init-database connection-data)
    ;;(with-db (clutch/put-document {:_id "0" :$hirop {:type "Foo"}}))
    (with-db connection-data (clutch/put-document {:docs [{:_hirop {:id "0" :type "Foo"}}]}))
    (let [res (save* connection-data context)
          {docs :documents} (fetch* connection-data context)]
      (is (= {:Bar ["2"]}
            (hirop/hrels (first (filter #(= (hirop/hid %) new-bar-id) docs))))))))

