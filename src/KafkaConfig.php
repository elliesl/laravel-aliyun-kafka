<?php
namespace Rdkafka;
use RdKafka\Conf;

class KafkaConfig
{

    // 初始化配置
    public function bootstrapConfig($config)
    {
        $conf = new Conf();
        $conf->set('sasl.mechanisms', 'PLAIN');
        $conf->set('api.version.request', 'true');
        $conf->set('sasl.username', $config['sasl_plain_username']);
        $conf->set('sasl.password', $config['sasl_plain_password']);
        $conf->set('security.protocol', 'SASL_SSL');
        $conf->set('ssl.ca.location', $config['ssl.ca.location']);
        $conf->set('message.send.max.retries', 5);
        $conf->set('group.id', $config['consumer_id']);
        $conf->set('metadata.broker.list', $config['bootstrap_servers']);
        return $conf;
    }


}