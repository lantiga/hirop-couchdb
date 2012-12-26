(ns hirop-couchdb.core
  (:use hirop.backend)
  (:use clojure.pprint)
  (:use [com.ashafa.clutch.http-client :only [couchdb-request]])
  (:require [hirop.core :as hirop]
            [com.ashafa.clutch :as clutch]
            [cheshire.core :as json]
            [cemerick.url :as url]))

(defmacro with-db [& forms]
  ;; TODO: if db not initialized, initialize it 
  `(clutch/with-db
     (assoc (cemerick.url/url (:connection-string *connection-data*))
       :username (:username *connection-data*)
       :password (:password *connection-data*))
     (do ~@forms)))

(defn save-views
  []
  (clutch/save-view
   "hirop"
   (clutch/view-server-fns
    :javascript
    {:context
     {:map
      "function(doc) { if (doc['external-ids'] !== undefined) { emit(doc['external-ids'], doc); }}"}
     :all
     {:map
      "function(doc) { if (doc.docs !== undefined) { for (var i=0; i<doc.docs.length; i++) { emit(doc.docs[i]._hirop.id, doc.docs[i]); }} else { emit(doc._id, doc) }}"}})))

(defn init-database
  [connection-data]
  (try
    (binding [*connection-data* connection-data]
      (with-db
        (clutch/get-database)
        (save-views)))
    (catch Exception e (prn e))))

;; Design:
;; entire contexts are stored in documents
;; external documents are stored separately with their ids and referenced in the contexts.
;; They could be stored in other contexts or as individual documents.
;; It's the same since we're going to get them through the view.

(defn uuid [] (str (java.util.UUID/randomUUID)))

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

(defn get-hirop-view-doc
  [id]
  (->
   (first (map :value (with-db (clutch/get-view "hirop" :all {:key id}))))
   couch->hirop))

(defn get-hirop-view-docs
  [ids]
  (->>
   (map :value (with-db (clutch/get-view "hirop" :all {:keys ids})))
   (map couch->hirop)))

(defn- context-document-id-rev
  [doc]
  (if-let [hrev (hirop/hrev doc)]
    (rest (re-find #"(.*)\#(.*)" hrev))
    [nil nil]))

(defn- document-rev
  [context-doc-id context-doc-rev]
  (str context-doc-id "#" context-doc-rev))

(defn fetch*
  [context]
  (let [external-ids (:external-ids context)
        context-doc-id (json/generate-string external-ids)
        context-doc (with-db (clutch/get-document context-doc-id))
        context-doc-rev (:_rev context-doc)
        external-docs (map #(get-hirop-view-doc %) (vals external-ids))
        external-doctypes (set (hirop/get-external-doctypes context))
        external-docs
        (reduce
         (fn [out rel-map]
           (reduce
            (fn [out [_ rel-ids]]
              (if (coll? rel-ids)
                (concat out (get-hirop-view-docs rel-ids))
                (conj out (get-hirop-view-doc rel-ids))))
            out
            (filter #(contains? external-doctypes (first %)) rel-map)))
         external-docs
         (vals (:rels context-doc)))
        external-docs (distinct external-docs)
        context-doc (update-in context-doc [:docs] #(concat % external-docs))
        docs
        (map
         (fn [doc]
           (->
            (if (:_rev doc) doc (hirop/assoc-hrev doc context-doc-rev))
            (hirop/assoc-hrels (get-in context-doc [:rels (keyword (hirop/hid doc))]))))
         (:docs context-doc))]
    docs))

;; Eventually consider using CouchDB update handlers.
(defn save*
  [context]
  (let [docs (vals (merge (:stored context) (:starred context)))
        external-doctypes (set (hirop/get-external-doctypes context))
        docs (filter #(not (contains? external-doctypes (hirop/htype %))) docs)
        external-ids (:external-ids context)
        tmp-starred (filter hirop/has-temporary-id? (vals (:starred context)))
        uuids (repeatedly (count tmp-starred) uuid)
        tmp-map (zipmap (map hirop/hid tmp-starred) uuids)
        context-doc {:_id (json/generate-string external-ids)}
        context-doc
        (if-let [rev (hirop/hrev (first (vals (:stored context))))]
          (assoc context-doc :_rev rev)
          context-doc)
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
        context-name (:name context)
        doc-data
        {:context-name context-name
         :external-ids external-ids
         :rels rels
         :docs docs}
        context-doc (merge context-doc doc-data)]
    (try
      ;; TODO: catch correct exception (on 409)
      ;;  Analyze when the exception should be triggered
      (with-db
        (clutch/put-document context-doc))
      {:result :success :remap tmp-map}
      (catch Exception e
        {:result :conflict}))))

(defmethod fetch :couchdb
  [backend context]
  (fetch* context))

(defmethod save :couchdb
  [backend context]
  (save* context))

(defmethod history :orientdb
  [backend id]
  #_(with-db
    ))
