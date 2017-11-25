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
        $loop = 0;
        $producer = $this->producer();
        $topic = $producer->newTopic($queue);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, $key);
        $producer->poll(0);

        // 取出还未推送至broker里面的数据
        while ($producer->getOutQLen() > 0) {
            if ($loop > 100) {
                throw new KafkaMaxPollException('Kafka producer exec too many times');
            }
            $producer->poll(50);
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
            $this->producer->setLogLevel($this->config['log_level']); // set log level
            $this->producer->addBrokers($this->config['bootstrap_servers']); // 初始化服务器
        }
        return $this->producer;
    }
}