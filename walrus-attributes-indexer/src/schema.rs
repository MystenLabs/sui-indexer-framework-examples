// @generated automatically by Diesel CLI.

diesel::table! {
    walrus_blob (address_owner, file_path) {
        address_owner -> Bytea,
        file_path -> Text,
        blob_id -> Bytea,
        owner_id -> Bytea,
        dynamic_field_id -> Bytea,
        cp_sequence_number -> Int8,
        df_version -> Int8,
        deleted -> Nullable<Bool>,
    }
}

diesel::table! {
    walrus_blob_historical (dynamic_field_id, df_version) {
        dynamic_field_id -> Bytea,
        df_version -> Int8,
        cp_sequence_number -> Int8,
        owner_id -> Nullable<Bytea>,
        address_owner -> Nullable<Bytea>,
        file_path -> Nullable<Text>,
        blob_id -> Nullable<Bytea>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    walrus_blob,
    walrus_blob_historical,
);
