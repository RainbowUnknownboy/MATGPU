yum -y update
yum -y install epel-release
yum -y install ganglia
yum -y install ganglia-gmond
yum -y install ganglia-gmond-python

cp /centos_7/gmond.conf /etc/ganglia/
cp /centos_7/python_modules/nvidia.py /usr/lib64/ganglia/python_modules/
cp /centos_7/conf.d/nvidia.pyconf /etc/ganglia/conf.d/

python /centos_7/nvidia-ml/setup.py build
python /centos_7/nvidia-ml/setup.py install

systemctl restart gmond.service
