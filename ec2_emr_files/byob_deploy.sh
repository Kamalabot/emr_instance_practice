tar xzf /softwares/hadoop-3.3.0.tar.gz -C /softwares
rm -f /softwares/hadoop-3.3.0.tar.gz
sudo mv -f /softwares/hadoop-3.3.0 /opt
sudo chown ${USER}:${USER} -R /opt/hadoop-3.3.0
sudo ln -s /opt/hadoop-3.3.0 /opt/hadoop
cp -rf /configs/opt/hadoop/etc/hadoop/* /opt/hadoop/etc/hadoop/.
cp -f /configs/.profile /home/ubuntu/.profile
. ~/.profile
/opt/hadoop/bin/hdfs namenode -format

#ls -ltr /opt/hadoop/dfs 
#find /opt/hadoop/dfs
#check ssh ${USER}@localhost
#check if the namenodes can be started using start-dfs.sh
#Check with jps
#hdfs dfs -ls / (check the hdfs is open)
#hdfs dfs -mkdir -p /user/${USER}
#start-dfs.sh
#start-yarn.sh
#stop-yarn.sh
#stop-dfs.sh

tar xzf /softwares/apache-hive-3.1.2-bin.tar.gz -C /softwares
rm -f /softwares/apache-hive-3.1.2-bin.tar.gz
sudo mv -f /softwares/apache-hive-3.1.2-bin /opt
sudo chown ${USER}:${USER} -R /opt/apache-hive-3.1.2-bin
sudo ln -s /opt/apache-hive-3.1.2-bin /opt/hive

cp -rf /configs/opt/hive/conf/* /opt/hive/conf/.

rm /opt/hive/lib/guava-19.0.jar

cp /opt/hadoop/share/hadoop/hdfs/lib/guava-27.0-jre.jar /opt/hive/lib/

#Need to setup docker and pull the postgres database
#The same can be done using the RDS also, will check that later
#Once postgres is ready, create metastore database, user hive 
# and then give permission to that user
#Ensure postgres-client is installed, psql command can be used for 
#connecting with the database
#Then next step can start
#Configure the hive-site.xml file with correct details of the 
#database
#Ensure the following GRANT commands are executed from the Postgres user
#ALTER DATABASE metastore OWNER TO hive;
#GRANT ALL ON DATABASE metastore TO hive;
#GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO hive
#GRANT ALL PRIVILEGES ON SCHEMA public TO hive;
#GRANT USAGE ON SCHEMA public TO hive;
#GRANT CREATE ON SCHEMA public TO hive;
#GRANT CREATE ON DATABASE metastore TO hive;

schematool -dbType postgres -initSchema

tar xzf /softwares/spark-2.4.8-bin-hadoop2.7.tgz -C /softwares
rm -rf /softwares/spark-2.4.8-bin-hadoop2.7.tgz
sudo mv -f /softwares/spark-2.4.8-bin-hadoop2.7 /opt
sudo ln -s /opt/spark-2.4.8-bin-hadoop2.7 /opt/spark2
sudo ln -s /opt/hive/conf/hive-site.xml /opt/spark2/conf/
cp -rf /configs/opt/spark2/conf/* /opt/spark2/conf/.
sudo mkdir -p /opt/spark2/jars/ 
sudo cp -rf /softwares/postgresql-42.2.19.jar /opt/spark2/jars/postgresql-42.2.19.jar
sudo chown ${USER}:${USER} -R /opt/spark-2.4.8-bin-hadoop2.7

tar xzf /softwares/spark-3.1.2-bin-hadoop3.2.tgz -C /softwares
rm -rf /softwares/spark-3.1.2-bin-hadoop3.2.tgz
sudo mv -f /softwares/spark-3.1.2-bin-hadoop3.2 /opt
sudo ln -s /opt/spark-3.1.2-bin-hadoop3.2 /opt/spark3
sudo ln -s /opt/hive/conf/hive-site.xml /opt/spark3/conf/
cp -rf /configs/opt/spark3/conf/* /opt/spark3/conf/.
sudo mkdir -p /opt/spark3/jars/ 
sudo mv -f /softwares/postgresql-42.2.19.jar /opt/spark3/jars/postgresql-42.2.19.jar
sudo chown ${USER}:${USER} -R /opt/spark-3.1.2-bin-hadoop3.2

/opt/hadoop/sbin/start-dfs.sh
/opt/hadoop/sbin/start-yarn.sh

hdfs dfs -mkdir -p /user/ubuntu

hdfs dfs -mkdir -p /spark2-jars
hdfs dfs -mkdir -p /spark2-logs

hdfs dfs -put -f /opt/spark2/jars/* /spark2-jars

hdfs dfs -mkdir -p /spark3-jars
hdfs dfs -mkdir -p /spark3-logs

hdfs dfs -put -f /opt/spark3/jars/* /spark3-jars

ln -s /usr/bin/python3 /usr/bin/python

sudo chown -R ubuntu:ubuntu /home/ubuntu/ubuntu-material
/home/ubuntu/.local/bin/jupyter lab --ip 0.0.0.0

python -m venv byob_venv

Jupyter lab needs to be installed in virtual environment

integrating Spark3 with Jupyter lab kernel.json by creating folder in location below 
/home/ubuntu/byob_venv/share/jupyter/kernels/pyspark3

{
      "argv": [
        "python",
        "-m",
        "ipykernel_launcher",
        "-f",
        "{connection_file}"
      ],
      "display_name": "Pyspark 3",
      "language": "python",
      "env": {
        "PYSPARK_PYTHON": "/usr/bin/python3",
        "SPARK_HOME": "/opt/spark3/",
        "SPARK_OPTS": "--master yarn --conf spark.ui.port=0",
        "PYTHONPATH": "/opt/spark3/python/lib/py4j-0.10.9-src.zip:/opt/spark3/python/"
      }
}

Then execute

jupyter kernelspec \
	install /home/ubuntu/byob_venv/share/jupyter/kernels/pyspark3 \
	--user