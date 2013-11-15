#!/usr/bin/python
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# snapper_sync
# Transfers all snapper snapshots from the local drive to a backup disk.
# Both drives have to be btrfs drives as this uses btrfs send/receive.
# Example use case: 
# * laptop backup to an external disk
# * you could execute this script on usb attach ( triggered by udev / dbus / whatever)

# Supply a config file with similar content:
# ATT: pathes are relative to the mount point
# [root]
# source_mountpoint = "/"
# source_path = ".snapshots"
# target_mountpoint = "/var/run/media/awerner/btrfs_backup"
# target_path = "backup/root"
# target_uuid = "ed918d69-3e7a-4798-bf83-cb9ad49b6d10"
# target_min_space = 20

# Important notes:
# * This is a script, every information is extracted from the source 
#   and target drives at each run.
# * You need to be root, because it involves btrfs send/receive
# * This script depends on the folder structure created by snapper
# * You need to configure your source drive below - at least for now

# TODO:
# * more safeguards - too many things can happen now
# * backup over the network, involving intermediate files and scp
# * get the snapshot list from snapper instead of btrfs
# * check what happens when a snapshot was modified on the target drive
# * change new snapshot name on the target drive to set up a safe mapping
#   of source snapshot -> target snapshot
# * check what happens when this is interrupted by a power loss/ target disk removal?
# * purge snapshots with a similar mechanism to snapper

import re
import subprocess
import optparse
import ConfigParser

parser = optparse.OptionParser()
parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="be verbose")
parser.add_option("--dry-run", action="store_true", dest="dry_run", help ="do not execute any commands")
(options, args) = parser.parse_args()

if len(args)==0:
	print "Supply one or more valid config file(s) as argument"
	quit(-1)

try:
	config = ConfigParser.ConfigParser()
	configfiles = config.read(args)
	if options.verbose:
		print "Processed config files:",configfiles
except:
	print "Could not parse config file"
	quit(-1)

# FIXME: process multiple section
mysection = config.sections()[0]
source_mountpoint = config.get(mysection, "source_mountpoint")
source_path      = config.get(mysection, "source_path")
target_mountpoint = config.get(mysection, "target_mountpoint")
target_path      = config.get(mysection, "target_path")
target_uuid      = config.get(mysection, "target_uuid")
target_min_space = config.get(mysection, "target_min_space")

if options.verbose:
	print "Setting from config file"
	print "source_mountpoint=",source_mountpoint
	print "source_path=",source_path
	print "target_mountpoint=",target_mountpoint
	print "target_path=",target_path
	print "target_uuid=",target_uuid
	print "target_min_space=",target_min_space

# check if correct backup storage is attached & mounted
# use disk uuid for this when possible to prevent detecting the wrong drive
try:
	mounts_file= open('/proc/mounts')
	mounts = mounts_file.read()
	myre = r"(\S+) "+target_mountpoint
	my_device = re.search(myre,mounts).group(1)
	blkid_out = subprocess.check_output(["/sbin/blkid","/dev/sdc3"])
	uuid = re.search(r"UUID=\"(.*?)\"",blkid_out).group(1)
except:
	print "Could not determine of the correct medium is mounted at target_path="+target_path
	quit(-1)
if uuid!=target_uuid:
	print "Wrong medium is mounted at target_path="+target_path
	quit(-1)

# check if there is enough space on the target disk
try:
	# TODO: there may be a better way to check this
	btrfs_df_output = subprocess.check_output("btrfs fi show "+my_device,shell=True)
	res = re.search(r"size ([\d.]+)\S+ used ([\d.]+)\S+ path (\S+)",btrfs_df_output)
	# TODO: hopefully the units are the same
	freespace_m = float(res.group(1))-float(res.group(2))
	if options.verbose:
		print "Free space on target disk", freespace_m
	if freespace_m < target_min_space:
		print "Not enough spaced left on target disk (target_min_space)"
		quit(-1)
except:
	print "Could not determine free space on target disk"

def parse_btrfs_subvolume_list(raw):
	table = []
	for line in raw.split('\n'):
		if len(line.strip())==0: continue
		match = re.match(r"ID (\S+) .* uuid (\S+) path (\S+)",line.strip())
		if match == None:
			print "Failed to parse btrfs subvolume list output"
			quit(-1)
		snapper_id_ = ""
		try:
			snapper_id_ = int(re.search(r"/(\d+)/snapshot",match.group(3)).group(1))
		except:
			snapper_id_ = -1
		class entry:
			btrfs_id = match.group(1)
			uuid = match.group(2)
			path = match.group(3)
			snapper_id = snapper_id_

		table.append(entry)
	return table

# check which snapshots are present on the source drive
source_snaps_raw = subprocess.check_output(["btrfs","subvolume","list","-s","-u",source_mountpoint])
source_snaps = parse_btrfs_subvolume_list(source_snaps_raw)
source_snaps = filter(lambda x: not x.path.find(source_path),source_snaps)
if options.verbose:
	print "Found snapshots in source_path:"
	for i in source_snaps:
		print i.path,"/",i.snapper_id
#on the source partition it can be checked if the snapshots have the correct parent uuid
#which is the uuid of the source partition?

# check which snapshots are present on the target drive
target_snaps_raw = subprocess.check_output(["btrfs","subvolume","list","-a","-u",target_mountpoint])
target_snaps = parse_btrfs_subvolume_list(target_snaps_raw)
target_snaps = filter(lambda x: not x.path.find(target_path),target_snaps)
if options.verbose:
	print "Found snapshots in target_path:"
	for i in target_snaps:
		print i.path,"/",i.snapper_id

# find common snapshots -> list of clone-sources
# find missing snapshots -> snapshots to be transfered
common_snaps = []
source_only_snaps = []
for snap in source_snaps:
	if snap.snapper_id==-1:
		continue
	is_common = False
	for other_snap in target_snaps:
		if snap.snapper_id == other_snap.snapper_id:
			common_snaps.append(snap.snapper_id)
			is_common = True
			break
	if not is_common:
		source_only_snaps.append(snap.snapper_id)
common_snaps.sort()
source_only_snaps.sort()
if options.verbose:
	print "Common snapshots:"
	print common_snaps
	print "Snapshots to be transfered:"
	print source_only_snaps
# the chain of incremental snapshots on the target partitions can be reconstructed
# but a connection to the source partition is unclear, but one could use a custom name
# probably the snapper snapshot ids can be used.


# use btrfs send/receive to transfer the new snapshot(s) to the target drive
# specifing all existing snapshots as clone sources, if multiple snapshots to transfer, extend list of clone-source after each transfer

for snapper_id in source_only_snaps:
	if options.verbose:
		print "Processing snapshot ",str(snapper_id)
	new_folder = target_mountpoint+"/"+target_path+"/"+str(snapper_id)
	if options.verbose:
		print "Creating folder",new_folder," to store snapshot ",str(snapper_id)
	if not options.dry_run:
		retval = subprocess.call("mkdir "+new_folder,shell=True)
		if retval != 0:
			print "New folder",new_folder, " could not be created"
			quit(-1)
	clone_cmdline = ""
	#for clone in common_snaps:
	#	clone_cmdline = clone_cmdline + "-c " + source_mountpoint + "/" + source_path + "/" + str(clone) + "/snapshot "
	previous_snap = -1
	for clone in common_snaps:
		if clone < snapper_id and clone > previous_snap:
			previous_snap = clone
			clone_cmdline = "-p "+source_mountpoint+"/"+source_path+"/"+str(clone)+"/snapshot "
	if options.verbose:
		if previous_snap != -1:
			print "Found parent snapshot ",str(previous_snap)," for this snapshot"
		else:
			print "Found no common parent snapshot for this snapshot"
	cmdline = "btrfs send " + clone_cmdline + source_mountpoint + "/" + source_path + "/" + str(snapper_id) + "/snapshot | btrfs receive " + target_mountpoint + "/" + target_path + "/" + str(snapper_id)
	if options.verbose:
		print cmdline
	if not options.dry_run:
		retval = subprocess.call(cmdline,shell=True)
		if retval != 0:
			print "Problem detected"
			quit(-1)
	common_snaps.append(snapper_id)
# use kernel io priorities or something like that to prevent locking up the system
# or block the external drive


