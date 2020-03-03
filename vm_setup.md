# Compute Engine Virtual Machine Setup  

1. Instance a new persistent disk
2. Instance a new VM and attach the disk to the VM during creation  
3. Follow this guide to format and mount the disk to the VM
	https://cloud.google.com/compute/docs/disks/add-persistent-disk
	Note: for the 2TB persistent disk,
	/dev/sdb: UUID="a081e1db-f7ef-4f3b-b4be-6cc5aae8c8cc" TYPE="ext4"
	thus /etc/fstab is:
	UUID=3aab24ec-a167-40db-a1ab-560fe0875a67 / ext4 defaults 1 1
	UUID=a081e1db-f7ef-4f3b-b4be-6cc5aae8c8cc /mnt/disks/20tb ext4 discard,defaults,nofail 0 2