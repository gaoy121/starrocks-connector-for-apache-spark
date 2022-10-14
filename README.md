通过如下命令进行 Spark 连接器的编译：

如果 Spark 版本是 v2.x，则执行如下命令，默认编译的是配套 Spark v2.3.4 的连接器：

sh build.sh 2
如果 Spark 版本是 v3.x，则执行如下命令，默认编译的是配套 Spark v3.1.2 的连接器：

sh build.sh 3
编译完成后，output/ 路径下生成 starrocks-spark2_2.11-1.0.0.jar 文件。将该文件拷贝至 Spark 的类文件路径 (Classpath) 下：

如果您的 Spark 以 Local 模式运行，需要把该文件放在 jars/ 路径下。
如果您的 Spark 以 Yarn 模式运行，需要把该文件放在预安装程序包 (Pre-deployment Package) 里。
