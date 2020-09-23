# 提供一个手动打包并发送到nginx节点的script
if [ -z "$1" ] ;then
    echo "please input a valid tag"
else
    cd ../command && mvn clean package -DskipTests
    scp ../command/assemble/target/hercules-command.tar.gz root@ops-nginx-midware-prod-sh4-qc01:/data/nginx/html/deploy/hercules-command-"$1".tar.gz
fi