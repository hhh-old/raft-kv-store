# Git Ignore 配置说明

## 一、文件位置

`.gitignore` 文件已创建在项目根目录：[.gitignore](.gitignore)

---

## 二、忽略的内容

### 2.1 Java & Maven 构建产物

```gitignore
target/                    # Maven 编译输出
*.class                    # Java 编译后的 class 文件
*.jar                      # 打包的 JAR 文件
dependency-reduced-pom.xml # Maven 依赖缩减配置
```

**原因**：这些是构建产物，可以从源码重新生成。

### 2.2 IDE 配置文件

```gitignore
# Eclipse
.classpath
.project
.settings/
bin/

# IntelliJ IDEA
.idea/
*.iws
*.iml
*.ipr
out/

# VS Code
.vscode/
```

**原因**：每个开发者的 IDE 配置不同，不应该共享。

### 2.3 运行时数据（重要）

```gitignore
data/                      # Raft 数据目录
**/log/                    # Raft 日志
**/raft_meta/              # Raft 元数据
**/snapshot/               # Raft 快照
*.log                      # 应用日志
logs/
```

**原因**：
- Raft 数据是运行时生成的，不应该提交
- 日志文件会频繁变化，不适合版本控制
- 每个节点的数据不同

### 2.4 操作系统文件

```gitignore
# Windows
Thumbs.db
Desktop.ini
$RECYCLE.BIN/

# macOS
.DS_Store
._*

# Linux
*~
.directory
```

**原因**：操作系统自动生成的文件，与项目无关。

### 2.5 临时文件

```gitignore
*.tmp
*.temp
*.bak
*.backup
*.swp
tmp/
temp/
```

**原因**：临时文件不应该提交到版本库。

### 2.6 客户端构建产物

```gitignore
client/target/
client/**/*.class
```

**原因**：客户端的构建产物同样不需要提交。

### 2.7 敏感信息

```gitignore
*.jks
*.p12
*.keystore
*.pem
*.key
*.crt
.env
```

**原因**：包含密钥、证书等敏感信息，绝对不能提交。

### 2.8 测试和覆盖率报告

```gitignore
coverage/
test-reports/
surefire-reports/
```

**原因**：测试报告是临时生成的。

---

## 三、保留的内容（不忽略）

### 3.1 源代码

```
src/
client/src/
```

✅ **必须提交**：项目的核心代码

### 3.2 配置文件

```
config/*.yml
config/*.yaml
src/main/resources/application*.yml
client/pom.xml
```

✅ **必须提交**：项目配置和依赖声明

### 3.3 脚本文件

```
scripts/*.sh
scripts/*.bat
client/test-client.bat
```

✅ **必须提交**：启动和测试脚本

### 3.4 文档

```
*.md
README.md
SOFAJRaft学习指南.md
幂等性实现总结.md
客户端使用指南.md
```

✅ **必须提交**：项目文档和学习资料

### 3.5 Maven 配置

```
pom.xml
client/pom.xml
```

✅ **必须提交**：项目依赖和构建配置

---

## 四、Git 使用建议

### 4.1 初始化仓库

```bash
cd d:\project\Claudecode\raft-kv-store
git init
git add .
git commit -m "Initial commit: Raft KV Store with client"
```

### 4.2 查看状态

```bash
git status
```

你会看到以下文件被忽略：
- `target/` 目录
- `data/` 目录
- `.idea/` 目录
- `*.log` 文件
- 等等...

### 4.3 查看被忽略的文件

```bash
git status --ignored
```

### 4.4 强制添加被忽略的文件（不推荐）

```bash
# 只有在特殊情况下才这样做
git add -f data/backup.log
```

---

## 五、常见场景

### 5.1 需要提交配置文件

```bash
# 配置文件不在 .gitignore 中，可以直接提交
git add config/node1.yml
git add config/node2.yml
git add config/node3.yml
git commit -m "Add cluster configuration"
```

### 5.2 需要提交新代码

```bash
# 添加新的 Java 文件
git add src/main/java/com/raftkv/service/NewService.java
git commit -m "Add new service"
```

### 5.3 需要提交文档

```bash
# 文档文件不在 .gitignore 中
git add 新功能说明.md
git commit -m "Add feature documentation"
```

### 5.4 忽略已跟踪的文件

如果某个文件之前已经被提交，但现在想忽略：

```bash
# 从 Git 缓存中删除（但保留文件）
git rm --cached data/node1.log

# 更新 .gitignore 后
git add .gitignore
git commit -m "Update gitignore"
```

---

## 六、验证 .gitignore

### 6.1 测试哪些文件会被忽略

```bash
# 查看被忽略的文件
git status --ignored

# 测试特定文件是否被忽略
git check-ignore -v target/classes
git check-ignore -v data/node1
git check-ignore -v src/main/java  # 这个不应该被忽略
```

### 6.2 验证输出示例

```bash
$ git check-ignore -v target/classes
.gitignore:10:target/       target/classes

$ git check-ignore -v src/main/java
# 无输出（说明不会被忽略）
```

---

## 七、.gitignore 规则详解

### 7.1 基本语法

```gitignore
# 注释
target/          # 忽略目录（包括子目录）
*.log            # 忽略所有 .log 文件
*.class          # 忽略所有 .class 文件
```

### 7.2 通配符

```gitignore
*        # 匹配任意字符（不包括路径分隔符）
**       # 匹配任意字符（包括路径分隔符）
?        # 匹配单个字符
[abc]    # 匹配括号内的任意字符
```

**示例**：

```gitignore
*.log           # 忽略所有 .log 文件
**/log/         # 忽略所有名为 log 的目录
data/node*/     # 忽略 data/node1, data/node2 等
```

### 7.3 否定规则

```gitignore
# 忽略所有 .log 文件
*.log

# 但不忽略 important.log
!important.log
```

### 7.4 目录匹配

```gitignore
target/         # 只忽略名为 target 的目录
/target         # 只忽略根目录下的 target
**/target       # 忽略所有名为 target 的目录
```

---

## 八、项目专属规则

### 8.1 Raft 数据目录

```gitignore
data/
**/log/
**/raft_meta/
**/snapshot/
```

**说明**：
- `data/`：主项目的数据目录
- `**/log/`：所有层级的 log 目录（Raft 日志）
- `**/raft_meta/`：所有层级的 raft_meta 目录（Raft 元数据）
- `**/snapshot/`：所有层级的 snapshot 目录（Raft 快照）

### 8.2 配置文件保留

```gitignore
# 以下文件在 .gitignore 中没有被忽略，会被 Git 跟踪：
config/node1.yml
config/node2.yml
config/node3.yml
src/main/resources/application.yml
src/main/resources/application-node1.yml
src/main/resources/application-node2.yml
src/main/resources/application-node3.yml
```

### 8.3 脚本文件保留

```gitignore
# 以下脚本文件会被 Git 跟踪：
scripts/start-node1.sh
scripts/start-node2.sh
scripts/start-node3.sh
scripts/demo.sh
client/test-client.bat
```

---

## 九、最佳实践

### 9.1 提交前检查

```bash
# 1. 查看状态
git status

# 2. 查看差异
git diff

# 3. 查看将被提交的文件
git diff --cached --name-only

# 4. 确认提交
git commit -m "Your message"
```

### 9.2 清理未跟踪文件

```bash
# 查看将被删除的文件
git clean -n

# 删除未跟踪的文件（谨慎使用）
git clean -f

# 删除未跟踪的文件和目录
git clean -fd
```

### 9.3 更新 .gitignore

```bash
# 1. 编辑 .gitignore
# 2. 从缓存中删除已跟踪的文件
git rm -r --cached target/
git rm -r --cached data/

# 3. 提交更新
git add .gitignore
git commit -m "Update .gitignore"
```

---

## 十、常见问题

### Q1: 为什么 data/ 目录被忽略了？

**A**: `data/` 目录包含 Raft 运行时数据（日志、元数据、快照），这些数据：
- 是运行时自动生成的
- 每个节点的数据不同
- 可以通过重启节点重新生成
- 文件可能很大，不适合版本控制

### Q2: 如何提交配置文件？

**A**: 配置文件（`config/*.yml`）不在 `.gitignore` 中，可以直接提交：

```bash
git add config/
git commit -m "Update configuration"
```

### Q3: 日志文件被忽略了怎么办？

**A**: 日志文件不应该提交到版本库。如果需要查看日志：
- 在运行时查看
- 使用日志聚合工具（如 ELK）
- 本地保留，不提交

### Q4: 如何忽略特定的 Markdown 文件？

**A**: 默认情况下，所有 `.md` 文件都会被提交。如果想忽略某些文档：

```gitignore
# 在 .gitignore 中添加
draft-*.md
temp-notes.md
```

### Q5: .idea/ 被忽略了，如何共享 IDE 配置？

**A**: IDE 配置不应该共享，因为：
- 每个开发者的环境不同
- 路径可能不同
- 插件可能不同

如果需要共享代码风格，可以使用：
- EditorConfig（`.editorconfig` 文件）
- Checkstyle 配置
- 代码格式化规则

---

## 十一、Git 工作流建议

### 11.1 功能分支工作流

```bash
# 1. 创建功能分支
git checkout -b feature/idempotency

# 2. 开发功能
# ... 编写代码 ...

# 3. 提交更改
git add src/main/java/com/raftkv/service/
git commit -m "Implement idempotency with RequestId"

# 4. 推送到远程
git push origin feature/idempotency

# 5. 创建 Pull Request
```

### 11.2 提交规范

```bash
# 功能
git commit -m "feat: add idempotency support"

# 修复
git commit -m "fix: resolve port conflict"

# 文档
git commit -m "docs: add client usage guide"

# 重构
git commit -m "refactor: simplify RaftKVService"

# 测试
git commit -m "test: add unit tests for client"
```

---

## 十二、总结

### 12.1 已配置的内容

✅ Java & Maven 构建产物  
✅ IDE 配置文件  
✅ 运行时数据（Raft 数据、日志）  
✅ 操作系统文件  
✅ 临时文件  
✅ 敏感信息  
✅ 测试报告  

### 12.2 保留的内容

✅ 源代码（`src/`）  
✅ 配置文件（`config/`, `application*.yml`）  
✅ 脚本文件（`scripts/`）  
✅ 文档（`*.md`）  
✅ 构建配置（`pom.xml`）  

### 12.3 快速开始

```bash
# 初始化 Git 仓库
cd d:\project\Claudecode\raft-kv-store
git init

# 添加所有未被忽略的文件
git add .

# 提交
git commit -m "Initial commit: Raft KV Store with client"
```

---

`.gitignore` 文件已创建完成，可以直接使用！🎉
