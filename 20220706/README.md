## 构建历史

### jar-basic-archetype 1.0-RELEASE

- lombok
- junit5 / junit4 / mockito / assertJ
- slf4j/logback 日志打印到控制台

## maven 常用命令

- mvn clean compile
- mvn clean package
- mvn clean install

- mvn source:jar 源码包
- mvn dependency:tree 显示依赖树
- mvn shade:shade 构建 uber-jar

## 项目环境

1. 配置maven-resources-plugin的编码为`UTF-8`

   ```xml
    <properties>
    	<!-- maven-resources-plugin 会自动读取此配置 -->
    	<project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
   ```

2. 配置maven-compiler-plugin（IDE 编译时会读取此配置）

   ```xml
   <properties>
   	<!-- maven-compiler-plugin 会自动读取此配置 -->
   	<maven.compiler.source>1.8</maven.compiler.source>
   	<maven.compiler.target>1.8</maven.compiler.target>
   </properties>
   ```
