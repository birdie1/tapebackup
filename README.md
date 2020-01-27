## Requirements
https://aur.archlinux.org/packages/mt-st-git/

Kernel Modules: 
- sg
- st

Programms: 
- mt (Arch mt-st-git)
- mtx

## Howto use Tapelib from Linux
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



