# SoundHouse ETL
Once you have logged in to EMR instance

~]$ sudo yum install git

....
...

Is this ok [y/d/N]: y

....

~]$ sudo pip install s3fs

~]$ cd /home/hadoop/

~]$ mkdir soundhouse_etl

~]$ cd soundhouse_etl



~]$ git clone https://github.com/rahul-tigeranalytics/SoundHouse

~]$  cd SoundHouse

~]$  chmod 775 etl_process.sh

~]$ vim etl_process.sh


#Configure the  /Deal/Vendor/ folder path in this file 

~]$ ./etl_process.sh







