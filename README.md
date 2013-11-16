snapper_sync
============
snapper_sync synchronizes btrfs snapshots created by the snapper daemon to a backup drive.
In this setup both drive are assumed to be formated with btrfs, as the snapshots can then
be efficiently transfered with "btrfs send -p $other_snapshot ..". 

* The script has to be executed as root to create new snapshots
* A sample configuration is supplied in the file backup.cfg
* Some integrity checks are implemented (required free disk space, correct backup medium mounted)

### Usage 
$> snapper_sync.py backup.cfg

Options:
-v, --verbose
--dry-run
