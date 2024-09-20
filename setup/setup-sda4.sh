#!/bin/bash
CURRENT_USER=$(whoami)

sudo mkfs -t ext4 /dev/sda4
mkdir -p /users/$CURRENT_USER/home
sudo mount -t ext4 /dev/sda4 /users/$CURRENT_USER/home

UUID=$(sudo blkid -s UUID -o value /dev/sda4)
echo "UUID=$UUID /users/$CURRENT_USER/home ext4 defaults 0 2" | sudo tee -a /etc/fstab

sudo chown -R $CURRENT_USER /users/$CURRENT_USER/home