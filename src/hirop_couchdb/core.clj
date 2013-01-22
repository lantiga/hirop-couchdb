(ns hirop-couchdb.core
  (:use hirop.backend)
  (:use clojure.pprint)
  (:use [com.ashafa.clutch.http-client :only [couchdb-request]])
  (:use [cemerick.url :only [url map->URL]])
  (:require [hirop.core :as hirop]
            [com.ashafa.clutch :as clutch]
            [cheshire.core :as json]))

(defmacro with-db [backend & forms]
  `(clutch/with-db (map->URL ~backend) (do ~@forms)))

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
  [backend]
  (try
    (with-db backend
      (clutch/get-database)
      (save-views))
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
  [backend id]
  (->
   (first (map :value (with-db backend (clutch/get-view "hirop" :all {:key id}))))
   couch->hirop))

(defn get-hirop-view-docs
  [backend ids]
  (->>
   (map :value (with-db backend (clutch/get-view "hirop" :all {:keys ids})))
   (map couch->hirop)))

(defn- context-document-id-rev
  [doc]
  (if-let [hrev (hirop/hrev doc)]
    (rest (re-find #"(.*)\#(.*)" hrev))
    [nil nil]))

(defn- document-rev
  [context-doc-id context-doc-rev]
  (str context-doc-id "#" context-doc-rev))

(defn- hexify
  [s]
  (format "%x" (new java.math.BigInteger (.getBytes s))))

(defn- md5
  "Generate a md5 checksum for the given string"
  [token]
  (let [hash-bytes
         (doto (java.security.MessageDigest/getInstance "MD5")
               (.reset)
               (.update (.getBytes token)))]
       (.toString
         (new java.math.BigInteger 1 (.digest hash-bytes)) ; Positive and the size of the number
         16))) ; Use base16 i.e. hex

(defn context-doc-id
  [context]
  (str
   "hctx_"
   (->
    {(:name context) (:external-ids context)}
    (json/generate-string)
    ;;(hexify)
    (md5))))

(defn fetch*
  [backend context]
  (let [external-ids (:external-ids context)
        context-doc (with-db backend (clutch/get-document (context-doc-id context)))
        context-doc-rev (:_rev context-doc)
        external-docs (map #(get-hirop-view-doc backend %) (vals external-ids))
        external-doctypes (set (hirop/get-external-doctypes context))
        external-docs
        (reduce
         (fn [out rel-map]
           (reduce
            (fn [out [_ rel-ids]]
              (if (coll? rel-ids)
                (concat out (get-hirop-view-docs backend rel-ids))
                (conj out (get-hirop-view-doc backend rel-ids))))
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
  [backend context]
  (let [docs (vals (merge (:stored context) (:starred context)))
        external-doctypes (set (hirop/get-external-doctypes context))
        docs (filter #(not (contains? external-doctypes (hirop/htype %))) docs)
        external-ids (:external-ids context)
        tmp-starred (filter hirop/has-temporary-id? (vals (:starred context)))
        uuids (repeatedly (count tmp-starred) uuid)
        tmp-map (zipmap (map hirop/hid tmp-starred) uuids)
        ;; context-doc {:_id (json/generate-string external-ids)}
        context-doc {:_id (context-doc-id context)}
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
      (with-db backend
        (clutch/put-document context-doc))
      {:result :success :remap tmp-map}
      (catch Exception e
        {:result :conflict}))))

(defmethod fetch :couchdb
  [backend context]
  (fetch* backend context))

(defmethod save :couchdb
  [backend context]
  (save* backend context))

(defmethod history :orientdb
  [backend id]
  #_(with-db
      backend
    ))
