<?php
namespace RdKafka\Connectors;

use RdKafka\KafkaConsumer;
use RdKafka\KafKaProducer;
use Rdkafka\KafkaQueue;
use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    /**
     * Establish a queue connection.
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