sudo yum -y install libaio
sudo yum -y install libnsl
sudo yum -y install gcc-c++ python3-devel unixODBC-devel

export LD_LIBRARY_PATH=/usr/local/airflow/plugins/instantclient_19_25
export DPI_DEBUG_LEVEL=64