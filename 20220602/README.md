## 构建历史

### jar-basic-archetype 1.1-RELEASE

- 添加spring-context/spring-test依赖

### jar-basic-archetype 1.0-RELEASE

- lombok
- junit5 / junit4 / mockito / powermock just for junit4 / assertJ
- slf4j/logback 日志打印到控制台
- plugin: maven-source-plugin / maven-surefire-plugin / maven-surefire-report-plugin / jacoco-maven-plugin



### 如何创建本地 archtype

1. `mvn archetype:create-from-project` 
2. `cd target/generated-sources/archetype/` 修改相关文件（别忘记在target下添加 .gitignore）
3. `cd  target/generated-sources/archetype/ && mvn compile && mvn install`
4. `mvn archetype:crawl`

## 项目搭建

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

3. 配置maven-source-plugin 打包源码

   ```xml
    <plugin>
     <groupId>org.apache.maven.plugins</groupId>
     <artifactId>maven-source-plugin</artifactId>
     <version>3.1.0</version>
     <executions>
       <execution>
         <id>attach-sources</id>
         <phase>verify</phase>
         <goals>
         <goal>jar-no-fork</goal>
         </goals>
       </execution>
     </executions>
   </plugin>
   ```
   
   
   
