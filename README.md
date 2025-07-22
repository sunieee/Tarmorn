# Top-down Associate Rule Mining via Ontological Rule Network


编译运行：
```bash
mvn clean package
java -Xmx256G -cp target/Tarmorn-1.0-SNAPSHOT-jar-with-dependencies.jar tarmorn.Learn
```

直接运行：
```bash
mvn exec:java -Dexec.mainClass="tarmorn.Learn"
```

PlantUML 生成类图：
- 在 pom.xml 添加 PlantUML Maven 插件：https://github.com/davidmoten/plantuml-maven-plugin
- 基于JAVA源代码生成PlantUML类图：`mvn clean com.github.davidmoten:plantuml-maven-plugin:generate`
- 安装PlantUML插件并渲染