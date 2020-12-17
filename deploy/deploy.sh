if [ -z "$1" ] ;then
    echo "please input a valid tag"
elif [ -z "$2" ] ;then
    echo "please input a valid host"
elif [ -z "$3" ] ;then
    echo "please input a valid dest"
elif [ -z "$4" ] ;then
    echo "please input a valid owner"
else
    /bin/cp -f ./.ssh/id_rsa ~/.ssh/id_rsa_to_emrs
    ansible-playbook -i hosts ansible-playbook.yml --extra-vars="tag=$1 host=$2 dest=$3 owner=$4" --key-file ~/.ssh/id_rsa_to_emrs
fi

# example
# ./deploy.sh {{tag}} {{host}} {{dest}} {{owner}}
# ./deploy.sh v0.0.4 SH4_Data_HDFS /home/data/hercules_temp_test_oplogJson data
# ./deploy.sh v0.0.7 HW_OBS_ETL /home/data/hercules data