<?php
namespace LaravelAliYunKafka;
use LaravelAliYunKafka\Exception\KafkaMaxPollException;
use Illuminate\Support\Facades\Log;
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
        $producer = $this->producer();
        $topic = $producer->newTopic($queue);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, $key);
        $retry = 0;
        // kafka 重试消息 最大不超过3秒钟
        while ($producer->getOutQLen() > 0 && $retry < 50) {
            if ($retry == 45) {
                Log::info('kafka可能会失败的消息', [
                    'key' => $key,
                    'message' => $message
                ]);
            }
            ++$retry;
            $producer->poll(60);
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
            $this->producer->addBrokers($this->config['bootstrap_servers']); // 初始化broker
        }
        return $this->producer;
    }
}
