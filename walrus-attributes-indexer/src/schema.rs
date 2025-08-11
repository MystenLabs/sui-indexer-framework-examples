// @generated automatically by Diesel CLI.

diesel::table! {
    blog_post (dynamic_field_id) {
        publisher -> Bytea,
        blob_id -> Bytea,
        owner_id -> Bytea,
        dynamic_field_id -> Bytea,
        df_version -> Int8,
        view_count -> Int8,
        title -> Text,
    }
}
