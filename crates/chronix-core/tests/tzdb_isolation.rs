//! The host cannot move a bucket boundary.
//!
//! Zones resolve against the database compiled into the build. If they came
//! from the host, an empty `TZDIR` would make `Europe/Berlin` unknown (Linux,
//! macOS) and a bogus `TZ` could leak into a default. The test re-runs itself
//! with both set, so the environment is the child's from its first instruction.
#![allow(clippy::unwrap_used)]

use chronix_core::timebucket::TimeBucket;

const CHILD: &str = "CHRONIX_TZDB_ISOLATION_CHILD";

#[test]
#[cfg_attr(miri, ignore = "spawns a process and reaches the filesystem")]
fn the_host_time_zone_database_is_never_read() {
    if std::env::var_os(CHILD).is_some() {
        let day = TimeBucket::parse("1d", Some("Europe/Berlin")).unwrap();
        // 2026-03-29 12:00Z: that day began at local midnight, 23:00Z the day
        // before, and the next begins at 22:00Z (CEST).
        let start = day.start_of(1_774_785_600_000_000_000);
        assert_eq!(start, 1_774_738_800_000_000_000);
        assert_eq!(day.next(start), 1_774_821_600_000_000_000);
        return;
    }
    let empty = std::env::temp_dir().join(format!("chronix-empty-tzdir-{}", std::process::id()));
    std::fs::create_dir_all(&empty).unwrap();
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "the_host_time_zone_database_is_never_read",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("TZDIR", &empty)
        .env("TZ", "Mars/Olympus_Mons")
        .output()
        .unwrap();
    std::fs::remove_dir_all(&empty).ok();
    assert!(
        out.status.success(),
        "with an empty TZDIR the child failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
