use diesel_migrations::{embed_migrations, EmbeddedMigrations};

mod schema;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub mod handlers;
