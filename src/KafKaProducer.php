<?php
namespace LaravelAliYunKafka;
use LaravelAliYunKafka\Exception\KafkaMaxPollException;
use RdKafka\Producer;
class KafKaProducer
{

    protected $config;

    protected $producer;

    /**
     * KafKaProducer constructor.
     * @param array $config
     */
    public function __construct(array $config)
    {
        $this->config = $config;
    }


    /**
     * 生成消息
     *
     * @param $message
     * @param $queue
     * @param null $key
     */
    public function produce($message, $queue, $key = null)
    {
        $topic = $this->producer()->newTopic($queue);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, $key);
    }

    /**
     * kafka配置
     *
     * @return KafkaConfig
     */
    protected function kafkaConfig()
    {
        return new KafkaConfig();
    }


    /**
     * RD-KAFKA实例
     *
     * @return Producer
     */
    public function producer()
    {
        if (!isset($this->producer))  {
            $conf = $this->kafkaConfig()->bootstrapConfig($this->config);
            $this->producer = new Producer($conf);
            $this->producer->setLogLevel($this->config['log_level']); // set log level
            $this->producer->addBrokers($this->config['bootstrap_servers']); // 初始化broker
        }
        return $this->producer;
    }
}
