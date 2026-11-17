+++
title = "Back up and restore"
description = "Take hard-linked checkpoints of a Chronix data directory, schedule them, verify a backup and restore a database or a single measurement."
weight = 70
+++

A Chronix backup is a checkpoint: a consistent copy of the data directory made with hard links, so it is nearly free in time and space. This guide takes one, schedules them, verifies a backup and restores a database or a single measurement.

## Backup & Restore

A backup is a **checkpoint**: the database as of the flush that starts it.
Every write acknowledged before the request is in it; a write accepted while
it runs is not. Segment files are hard-linked when the target is on the same
filesystem, so a checkpoint of a large database is near instant and takes no
extra space until those segments are compacted away.

The directory it produces is a complete database plus a
`backup_manifest.json`. The files are immutable, so `rsync`, `tar` and
object-store sync all work on it — which is not true of a live data
directory.

**Create a backup:**
```
POST /api/v1/admin/backup
Content-Type: application/json

{
  "target_dir": "nightly-2026-09-11"
}
```

Response: `200 OK` with a `BackupManifest` — `version`, `created_at`,
`wal_sequence`, `segments`, `file_count` and `total_bytes`. Paths resolve
inside `backup_root` (see [Configuration](@/reference/configuration.md)).

Backing up repeatedly into one directory is a rolling checkpoint: files the
new one does not name are removed.

**On a schedule:**
```toml
[database.checkpoints]
interval_secs = 21600                    # every six hours
directory     = "/mnt/backup/chronix"
keep          = 4                        # a day of history
```

The maintenance thread takes these, beside the flush, compaction, rollup and
retention passes. The first runs at startup. Each writes
`<directory>/<timestamp>`; the oldest are removed once more than `keep`
remain, and an unfinished run is removed on sight. A failed run leaves the
existing checkpoints alone.

Point `directory` at a **different volume** from `data_dir` if the aim is
surviving that volume: on the same filesystem the segments are hard-linked,
so the checkpoint is not a second copy of the bytes.

**Verify a backup, without restoring it:**
```
POST /api/v1/admin/backup/verify
Content-Type: application/json

{
  "backup_dir": "nightly-2026-09-11"
}
```

Response: `200 OK` with the backup's `BackupManifest`, or `400` naming the
segment that is missing or the wrong size. It reads only, so it is safe
against the backups you are keeping.

**Restore a backup:**
```
POST /api/v1/admin/restore
Content-Type: application/json

{
  "backup_dir": "nightly-2026-09-11",
  "target_dir": "restored"
}
```

Response: `200 OK` with the backup's `BackupManifest`.

The target directory must not exist. Every segment the backup's catalog names
is checked for presence and size before anything is copied, so an incomplete
backup is refused. Segments are hard-linked where possible; the catalog and
WAL directories are copied. The result can be opened on any machine.

Restore does not touch the running server: point a `chronixd` at the restored
directory, or stop the server, swap the directories and start it again.

**Alerting.** `chronix_backup_failures_total`, and
`increase(chronix_backups_total[1d]) == 0` if you take one nightly. Both are
published at zero from startup, so either can fire from the first scrape.

### Object-store durability

Cold-tiered segments carry the bucket's own durability and server-side
encryption. For cross-region safety use the bucket's replication.
