## Ubuntu单机安装
Ubuntu中可使用APT安装MySQL. 在[下载页面](https://dev.mysql.com/downloads/repo/apt/)下载`.deb`安装包. 运行如下命令.
```shell
sudo dpkg -i mysql-apt-config*
```
运行后根据需求选择所需配置, 运行完成后再运行如下命令.
```shell
sudo apt update
sudo apt install mysql-server
```
注意在安装过程中需要输入密码.

如果需要安装Workbench， 运行如下命令即可.
```shell
sudo apt install mysql-workbench
```