## Tapebackup
This program downloads files via SSH/Rsync from remote server, encrypt them using openssl and stores them on tape devices.

It stores all necessary information into a SQLite database. It stores original filenames, encrypted filenames, hashsums, tapedevices and Timestamps of operations

**Caution**: It will not detect changes on same filename. Use it only for non changing files, like mediafiles or pictures.


## Gettings started
### Prerequisites
It is written in Python 3. Install necessary tape software via distribution package manager. Install Python modules via pip.

Python modules:
- pyyaml


Kernel modules: 
- sg
- st

Tools: 
- mt (Archlinux mt-st-git)
- mtx
- LTFS
  - [IBM Drives] OpenLTFS
  - [HP/HPE Drives] HPE StoreOpen und Linear Tape File System (LTFS) Software https://buy.hpe.com/de/de/storage/storage-software/storage-device-management-software/storeever-tape-device-management-software/hpe-storeopen-linear-tape-file-system-ltfs-software/p/4249221
- openssl
- rsync
- sqlite

#### Install LTFS (Linear Tape File System
- [IBM Drives] OpenLTFS
- [HP/HPE Drives] HPE StoreOpen und Linear Tape File System (LTFS) Software https://buy.hpe.com/de/de/storage/storage-software/storage-device-management-software/storeever-tape-device-management-software/hpe-storeopen-linear-tape-file-system-ltfs-software/p/4249221

Follow install instructions from LTFS package
  
#### Install tools via package management
Arch Linux
```
pacman -S mt-st-git rsync 

## Install mtx via AUR, if you are using yay:
yay -S mtx-svn
```
Debian based OS
```
apt install mt-st mtx rsync
```

#### Python venv
```
python3 -m virtualenv venv
venv/bin/pip install -r requirements.txt
```


### Change the configuration file!
In order to use the tapebackup script, you need to modify the config.yml file to your needs.

### Download Files from remote server
To download files via rsync from remote server execute:
```
./main.py get
```

### Encrypt files with openssl
To encrypt files with openssl execute:
```
./main.py pack
```
It will create generated names with will to saved in the database.

### Write encrypted files to tape
To write the encrypted files to tape, execute the following:
```
./main.py write
```
It will find unused tapes in library and write data to it. If there are no free tapes, or tape is full, it will inform you.

### More functions
There are many more function around database or verifying files. Use `./main.py --help` to see all functions.

## Tested with following Devices / OS
- Arch Linux
- TANDBERG StorageLoader with HP Ultrium 5-SCSI Drive

## Known limitations
- Tapelibraries with more than 1 drive possibly not working (I have no media to test with)
- Verify backup by md5sum not yet implemented
- Restoring files automaticaly from tapes not yet implemented

## Howto test tapelib from linux
### List Tape Devices
```
root@wuerfel /home/jonas # lsscsi --generic
[0:0:0:0]    tape    HP       Ultrium 5-SCSI   Z51U  /dev/st0   /dev/sg4 
[0:0:0:1]    mediumx TANDBERG StorageLoader    0495  /dev/sch0  /dev/sg5 
[1:0:0:0]    disk    ATA      WDC WD100EFAX-68 0A83  /dev/sda   /dev/sg0 
[2:0:0:0]    disk    ATA      WDC WD100EFAX-68 0A83  /dev/sdb   /dev/sg1 
[3:0:0:0]    disk    ATA      WDC WD100EFAX-68 0A83  /dev/sdc   /dev/sg2 
[5:0:0:0]    disk    ATA      Samsung SSD 840  DB6Q  /dev/sdd   /dev/sg3 
```
If is does not return the changer and tape device, check if modul `sg` is loaded.

### Show Tape Device Status
```
root@wuerfel /home/jonas # mt-st -f /dev/st0 status                                                                                                                                                                                      :(
SCSI 2 tape drive:
File number=-1, block number=-1, partition=0.
Tape block size 0 bytes. Density code 0x0 (default).
Soft error count since last status=0
General status bits on (50000):
 DR_OPEN IM_REP_EN
```

### Show Tape Library (Changer) Status
```
root@wuerfel /home/jonas # mtx -f /dev/sg5 status                                                                                                                                                                                        :(
mtx: Request Sense: Long Report=yes
mtx: Request Sense: Valid Residual=no
mtx: Request Sense: Error Code=70 (Current)
mtx: Request Sense: Sense Key=Unit Attention
mtx: Request Sense: FileMark=no
mtx: Request Sense: EOM=no
mtx: Request Sense: ILI=no
mtx: Request Sense: Additional Sense Code = 28
mtx: Request Sense: Additional Sense Qualifier = 00
mtx: Request Sense: BPV=no
mtx: Request Sense: Error in CDB=no
mtx: Request Sense: SKSV=no
Mode sense (0x1A) for Page 0x1D failed
  Storage Changer /dev/sg5:1 Drives, 7 Slots ( 0 Import/Export )
Data Transfer Element 0:Empty
      Storage Element 1:Full :VolumeTag=000711L4                        
      Storage Element 2:Full :VolumeTag=A00008L5                        
      Storage Element 3:Full :VolumeTag=A00034L5                        
      Storage Element 4:Full :VolumeTag=A00014L5                        
      Storage Element 5:Full :VolumeTag=A00036L5                        
      Storage Element 6:Full :VolumeTag=A00025L5                        
      Storage Element 7:Full :VolumeTag=000218L5                        
```

### Read Product Information from Drive
```
root@wuerfel /home/jonas # mtx -f /dev/sg4 inquiry                                                                                                                                                                                       :(
Product Type: Tape Drive
Vendor ID: 'HP      '
Product ID: 'Ultrium 5-SCSI  '
Revision: 'Z51U'
Attached Changer API: No
```

## Erase Tape
```
root@wuerfel /home/jonas # mt-st -f /dev/st0 erase
```

## Mount and umount
```
mtx -f /dev/sg5 unload
mtx -f /dev/sg5 load 4
mkltfs -d /dev/st0
ltfs /mnt/lto5
umount /mnt/lto5
```

