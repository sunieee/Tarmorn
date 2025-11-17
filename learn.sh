export MAVEN_OPTS="-Xms48g -Xmx48g -XX:MaxMetaspaceSize=2g"
mvn clean compile
mvn exec:java -Dexec.mainClass="tarmorn.Learn"
mvn exec:java -Dexec.mainClass="tarmorn.TLearn"