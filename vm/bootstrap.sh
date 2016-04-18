#!/bin/bash -x

# Setup puppet client
if [ ! -f /etc/yum.repos.d/puppetlabs.repo ]; then
	sudo rpm -ivh https://yum.puppetlabs.com/puppetlabs-release-el-7.noarch.rpm
fi

if [ ! -f /usr/bin/puppet ]; then
	sudo yum -y install puppet	
fi
