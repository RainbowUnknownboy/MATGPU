apt-get update
apt-get install -y ganglia-monitor
apt-get install -y ganglia-monitor-python

cp /ubuntu/gmond.conf /etc/ganglia/
cp /ubuntu/python_modules/nvidia.py /usr/lib/ganglia/python_modules/
cp /ubuntu/conf.d/nvidia.pyconf /etc/ganglia/conf.d/

python /ubuntu/nvidia-ml/setup.py build
python /ubuntu/nvidia-ml/setup.py install

service ganglia-monitor restart