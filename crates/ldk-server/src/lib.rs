pub mod backend;
pub mod error;
pub mod settings;
pub mod uncertain_sends;

#[cfg(test)]
mod tests {
    /// The test build links SQLite through `cdk-sqlite`. `libsqlite3-sys` 0.28.0 of crates.io has
    /// RUSTSEC-2026-0459. Cargo ignores a patch that no longer matches, so check the lock file.
    #[test]
    fn the_locked_sqlite_source_has_the_wal_backport() {
        let packages: Vec<_> = include_str!("../Cargo.lock")
            .split("[[package]]")
            .filter(|package| package.contains("name = \"libsqlite3-sys\"\n"))
            .collect();
        assert_eq!(packages.len(), 1);
        assert!(
            packages[0].contains(
                "source = \"git+https://github.com/joemphilips/cdk?\
                 rev=d8bab862eceff1e9355c6079f1131674637fc450#\
                 d8bab862eceff1e9355c6079f1131674637fc450\""
            ),
            "{}",
            packages[0]
        );
    }
}
