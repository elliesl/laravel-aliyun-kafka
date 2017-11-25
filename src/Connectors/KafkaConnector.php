<?php
namespace LaravelAliYunKafka\Connectors;

use LaravelAliYunKafka\KafkaConsumer;
use LaravelAliYunKafka\KafKaProducer;
use LaravelAliYunKafka\KafkaQueue;
use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    /**
     * 建立一个连接
     *
     * @param  array  $config
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        return new KafkaQueue($config, $this->kafkaProducer($config), $this->kafkaConsumer($config), $config['queue']);
    }

    /**
     * 消息消费者
     *
     * @param $config
     * @return KafkaConsumer
     */
    protected function kafkaConsumer($config)
    {
        return new KafkaConsumer($config);
    }

    /**
     * kafka生产者实例
     *
     * @param $config
     * @return KafKaProducer
     */
    protected function kafkaProducer($config)
    {
        return new KafKaProducer($config);
    }
}