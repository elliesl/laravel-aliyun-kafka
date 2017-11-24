<?php
namespace Rdkafka;
use Rdkafka\Exception\KafkaMaxPollException;
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
        $loop = 0;
        $rk = $this->producer();
        $rk->setLogLevel($this->config['log_level']);
        $rk->addBrokers($this->config['bootstrap_servers']);
        $topic = $rk->newTopic($queue);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, $key);

        $rk->poll(0);
        while ($rk->getOutQLen() > 0) { // The out queue contains messages waiting to be sent to, or ackknownledged by, the broker.
            if ($loop > 100) {
                throw new KafkaMaxPollException('Kafka producer exec too many times');
            }
            $rk->poll(50);
            $loop++;
        }
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
        }
        return $this->producer;
    }
}