(ns metabase.sync.sync-metadata
  "Logic responsible for syncing the metadata for an entire database.
   Delegates to different subtasks:

   1.  Sync tables (`metabase.sync.sync-metadata.tables`)
   2.  Sync fields (`metabase.sync.sync-metadata.fields`)
   3.  Sync FKs    (`metabase.sync.sync-metadata.fks`)
   4.  Sync Metabase Metadata table (`metabase.sync.sync-metadata.metabase-metadata`)"
  (:require [metabase.models.database :refer [Database]]
            [metabase.sync
             [interface :as i]
             [util :as sync-util]]
            [metabase.sync.sync-metadata
             [fields :as sync-fields]
             [fks :as sync-fks]
             [metabase-metadata :as metabase-metadata]
             [tables :as sync-tables]]
            [metabase.util :as u]
            [schema.core :as s]
            [toucan.db :as db]))

(s/defn ^:always-validate sync-db-metadata!
  [database :- i/DatabaseInstance]
  (sync-util/sync-operation :sync-metadata database (format "Sync metadata for %s" (sync-util/name-for-logging database))
    ;; record the fact that we're doing a metadata sync right now so later we can determine which Fields belonging to this DB are new
    (db/update! Database (u/get-id database) :metadata_sync_last_started (u/new-sql-timestamp))
    ;; Make sure the relevant table models are up-to-date
    (sync-tables/sync-tables! database)
    ;; Now for each table, sync the fields
    (sync-fields/sync-fields! database)
    ;; Now for each table, sync the FKS. This has to be done after syncing all the fields to make sure target fields exist
    (sync-fks/sync-fks! database)
    ;; finally, sync the metadata metadata table if it exists.
    (metabase-metadata/sync-metabase-metadata! database)))

(s/defn ^:always-validatge sync-table-metadata!
  "Sync the metadata for an individual TABLE -- make sure Fields and FKs are up-to-date."
  [table :- i/TableInstance]
  (sync-fields/sync-fields-for-table! table)
  (sync-fks/sync-fks-for-table! table))
