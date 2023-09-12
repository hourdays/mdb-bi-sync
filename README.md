# mdb-bi-sync
Use these two scripts to synchronise a MongoDB collection bi-directionally between two MongoDB clusters.
This was tested with a MongoDB Atlas cluster running 4.4 and a self-manged MongoDB single node replica set cluster running 4.0 on an AWS EC2 instance.
For now this script listens to inserts only.
