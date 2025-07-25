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

## Project Structure
The project is organized as follows:

```
Tarmorn
├── src/main/java/tarmorn/       # Java source files
├── data/
|   └── <dataset_name>/         # Directory for each dataset
├── out/
|   └── <dataset_name>/         # Output directory for each dataset
├── PyClause/                 # PyClause submodule for rule validation
├── scripts/                  # Scripts for running the project
|   ├── validate.sh                  # Shell script to run the project
|   └── search.sh               # Shell script to search for rules
├── pom.xml                     # Maven project file
├── README.md                   # Project documentation
├── config.yaml                # Configuration file for the project
└── requirements.txt           # Python dependencies for validation
```
    

## 验证

按照 https://github.com/symbolic-kg/PyClause 说明，安装 PyClause 和依赖库:
```
git clone https://github.com/symbolic-kg/PyClause
cd PyClause
pip install -e .
```

运行验证脚本: `./scripts/eval.sh`