(ns metabase.sync.analyze.special-types
  "Logic for scanning values of a given field and updating special types as appropriate.
   Also known as 'fingerprinting', 'analysis', or 'classification'.

   (Note: this namespace is sort of a misnomer, since special type isn't the only thing that can get set by
    the functions here. `:preview_display` can also get set to `false` if a Field has on average very
    large (long) values.)"
  (:require [clojure.tools.logging :as log]
            [metabase.models
             [database :refer [Database]]
             [field :refer [Field]]]
            [metabase.sync
             [interface :as i]
             [util :as sync-util]]
            [metabase.sync.analyze.special-types
             [name :as name]
             [values :as values]]
            [metabase.util :as u]
            [schema.core :as s]
            [toucan.db :as db]))

(s/defn ^:private ^:always-validate fields-to-infer-special-types-for :- (s/maybe [i/FieldInstance])
  "Return a sequences of Fields belonging to TABLE for which we should attempt to determine special type.
   This should include fields that are active, visibile, and without an existing special type.
   Only Fields that are NEW (added during the most recent metadata sync cycle) are analyzed for special types."
  [table :- i/TableInstance]
  (seq (db/select Field
         :table_id        (u/get-id table)
         :special_type    nil
         :active          true
         :visibility_type [:not= "retired"]
         :preview_display true
         ;; If the Field was CREATED since the last time metadata sync started then `created_at` timestamp should be greater than Database's `metadata_sync_last_started` timestamp
         ;; if DB doesn't have metadata-sync-last-started set for some reason just compare against 1970 which will always be true
         :created_at      [:> (if-let [metadata-sync-last-started (db/select-one-field :metadata_sync_last_started Database :id (:db_id table))]
                                metadata-sync-last-started
                                (java.sql.Timestamp. 0))])))


(s/defn ^:always-validate infer-special-types-for-table!
  "Infer (and set) the special types and preview display status for Fields
   belonging to TABLE, and mark the fields as recently analyzed."
  [table :- i/TableInstance]
  (sync-util/with-error-handling (format "Error inferring special types for %s" (sync-util/name-for-logging table))
    ;; fetch any fields with no special type. See if we can infer a type from their name.
    (when-let [fields-to-analyze (fields-to-infer-special-types-for table)]
      (name/infer-special-types-by-name! table fields-to-analyze)
      ;; Ok, now fetch fields that *still* don't have a special type. Try to infer a type from a sequence of their values.
      (when-let [fields-to-analyze-by-value (fields-to-infer-special-types-for table)]
        (values/infer-special-types-by-value! table fields-to-analyze-by-value))
      ;; Finally, mark update the `:last_analyzed` timestamp for all the Fields we just analyzed
      (db/update-where! Field {:id [:in (map u/get-id fields-to-analyze)]}
        :last_analyzed (u/new-sql-timestamp)))))


(s/defn ^:always-validate infer-special-types!
  "Infer (and set) the special types and preview display status for all the
   Fields belonging to DATABASE, and mark the fields as recently analyzed."
  [database :- i/DatabaseInstance]
  (let [tables (sync-util/db->sync-tables database)]
    (sync-util/with-emoji-progress-bar [emoji-progress-bar (count tables)]
      (doseq [table tables]
        (infer-special-types-for-table! table)
        (log/info (u/format-color 'blue "%s Analyzed %s" (emoji-progress-bar) (sync-util/name-for-logging table)))))))
