

package {'mesosphere-el-repo-7-1.noarch':
  ensure => present,
  source => "http://repos.mesosphere.com/el/7/noarch/RPMS/mesosphere-el-repo-7-1.noarch.rpm",
  provider => rpm
}


package { 'jdk':
  name   => 'java-1.8.0-openjdk',
  ensure => installed
}

package { 'mesosphere-zookeeper':
  require => [ Package['jdk'], Package['mesosphere-el-repo-7-1.noarch'] ],
  name => 'mesosphere-zookeeper',
  ensure => installed
}

package { 'mesos':
  require => [ Package['mesosphere-zookeeper'] ],
  name => 'mesos',
  ensure => '0.28.1-2.0.20.centos701406',
} 

service { 'zookeeper':
  require => [ Package['mesosphere-zookeeper'] ],
  ensure => running
}

service { 'mesos-master':
  require => [ Package['mesos'], Service['zookeeper'] ],
  ensure => running
}

service { 'mesos-slave':
  require => [ Package['mesos'], Service['zookeeper'] ],
  ensure => running
}
