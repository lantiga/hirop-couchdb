(ns hirop-couchdb.core
  (:use hirop.backend)
  (:use clojure.pprint)
  (:use [com.ashafa.clutch.http-client :only [couchdb-request]])
  (:require [hirop.core :as hirop]
            [com.ashafa.clutch :as clutch]
            [cheshire.core :as json]
            [cemerick.url :as url]))

(def ^:dynamic *connection-data* nil)

(defn init [connection-data]
  (alter-var-root #'*connection-data* (fn [_] connection-data))
  (try
    (clutch/get-database
     (assoc (cemerick.url/url (:connection-string *connection-data*))
       :username (:username *connection-data*)
       :password (:password *connection-data*)))
    (catch Exception e (prn e))))

(defmacro with-db [& forms]
  `(clutch/with-db
     (assoc (cemerick.url/url (:connection-string *connection-data*))
       :username (:username *connection-data*)
       :password (:password *connection-data*))
     (do ~@forms)))

;; Design:
;; entire contexts are stored in documents
;; external documents (e.g. Tags) are stored separately with their ids
;; and referenced in the contexts.
;; They could be stored in other contexts or as individual documents.
;; It's the same since we're going to get them through the view.

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn save-views
  []
  (with-db
    (clutch/save-view
     "docs"
     (clutch/view-server-fns
      :javascript
      {:context
       {:map
        "function(doc) { if (doc['external-ids'] !== undefined) { emit(doc['external-ids'], doc); }}"}
       :all
       {:map
        "function(doc) { if (doc.docs !== undefined) { for (var i=0; i<doc.docs.length; i++) { emit(doc.docs[i]._hirop.id, doc.docs[i]); }} else { emit(doc._id, doc) }}"}}))))

(defn couch->hirop
  [doc]
  (if (:_hirop doc)
    (clutch/dissoc-meta doc)
    (->
     doc
     (clutch/dissoc-meta)
     (dissoc :$hirop)
     (assoc :_hirop (:$hirop doc))
     (hirop/assoc-hid (:_id doc))
     (hirop/assoc-hrev (:_rev doc)))))

(defn get-docs-view-doc
  [id]
  (->
   (first (map :value (with-db (clutch/get-view "docs" :all {:key id}))))
   couch->hirop))

(defn get-docs-view-docs
  [ids]
  (->>
   (map :value (with-db (clutch/get-view "docs" :all {:keys ids})))
   (map couch->hirop)))

(defn fetch*
  [context]
  (let [external-ids (:external-ids context)
        context-doc-id (json/generate-string external-ids)
        ;; Alternatively we could use the docs view
        context-doc (with-db (clutch/get-document context-doc-id))
        external-docs (map #(get-docs-view-doc %) (vals external-ids))
        external-doctypes (set (hirop/get-external-doctypes context))
        external-docs
        (reduce
         (fn [out rel-map]
           (reduce
            (fn [out [_ rel-ids]]
              (if (coll? rel-ids)
                (concat out (get-docs-view-docs rel-ids))
                (conj out (get-docs-view-doc rel-ids))))
            out
            (filter #(contains? external-doctypes (first %)) rel-map)))
         external-docs
         (vals (:rels context-doc)))
        external-docs (distinct external-docs)
        context-doc (update-in context-doc [:docs] #(concat % external-docs))
        docs
        (map
         (fn [doc]
           (hirop/assoc-hrels doc (get-in context-doc [:rels (keyword (hirop/hid doc))])))
         (:docs context-doc))]
    docs))

(defn- context-document-id-rev
  [doc]
  (if-let [hrev (hirop/hrev doc)]
    (rest (re-find #"(.*)\#(.*)" hrev))
    [nil nil]))

(defn- document-rev
  [context-doc-id context-doc-rev]
  (str context-doc-id "#" context-doc-rev))

(defn save*
  [store context]
  (let [docs (vals (merge (:stored store) (:starred store)))
        external-doctypes (set (hirop/get-external-doctypes context))
        docs (filter #(not (contains? external-doctypes (hirop/htype %))) docs)
        external-ids (:external-ids context)
        tmp-starred (filter hirop/has-temporary-id? (vals (:starred store)))
        ;;uuids (:uuids (with-db (clutch/uuids (count tmp-starred))))
        uuids (repeatedly (count tmp-starred) uuid)
        tmp-map (zipmap (map hirop/hid tmp-starred) uuids)
        context-doc-id (json/generate-string external-ids)
        context-doc
        (if (with-db (clutch/document-exists? context-doc-id))
          (with-db (clutch/get-document context-doc-id))
          (with-db (clutch/put-document {:_id context-doc-id})))
        rels
        (reduce
         (fn [out doc]
           (let [remap (fn [id] (if (contains? tmp-map id) (get tmp-map id) id))
                 hid (remap (hirop/hid doc))
                 htype (hirop/htype doc)
                 hrels (hirop/hrels doc)
                 hrels
                 (into {}
                  (map
                   (fn [[rel-type rel-ids]]
                     [rel-type
                      (if (coll? rel-ids)
                        (map remap rel-ids)
                        (remap rel-ids))])
                   hrels))]
             (if (empty? hrels)
               out
               (assoc out hid hrels))))
         {}
         docs)
        docs
        (map
         (fn [doc]
           (let [doc
                 (if (contains? tmp-map (hirop/hid doc))
                   (hirop/assoc-hid doc (get tmp-map (hirop/hid doc)))
                   doc)]
             (->
              doc
              (hirop/dissoc-hrev)
              (hirop/dissoc-hrels))))
         docs)
        context-name (:context-name store)
        doc-data
        {:context-name context-name
         :external-ids external-ids
         :rels rels
         :docs docs}
        context-doc (merge context-doc doc-data)
        ]
    (with-db
      (clutch/put-document context-doc))
    tmp-map))

(defmethod fetch :couchdb
  [backend context]
  (fetch* context))

(defmethod save :couchdb
  [backend store context]
  (save* store context))

(defmethod history :orientdb
  [backend id]
  #_(with-db
    ))
