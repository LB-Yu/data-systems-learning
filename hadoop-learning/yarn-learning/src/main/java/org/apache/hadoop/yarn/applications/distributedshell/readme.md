This simple example shows the way to implement new Applications for YARN. It will run the Shell command or script 
provided by the user in the YARN Container

To run the application you need to compile it first.
```shell
cd hadoop-learning
mvn clean package -DskipTests
cd yarn-learning/target/
```

Then you can run it by the following command.
```shell
yarn jar yarn-learning-1.0-SNAPSHOT.jar \
  org.apache.hadoop.yarn.applications.distributedshell.Client \
  -jar yarn-learning-1.0-SNAPSHOT.jar \
  -shell_script shell.sh \
  -num_containers 3
```