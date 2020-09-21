if [ -z "$1" ] ;then
    echo "please input a valid tag"
else
    ansible-playbook -i hosts ansible-playbook.yml --extra-vars="tag=$1"
fi