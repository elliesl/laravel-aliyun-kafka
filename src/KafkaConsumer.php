<?php
namespace LaravelAliYunKafka;
use RdKafka\TopicConf;
use RdKafka\KafkaConsumer as Consumer;

class KafkaConsumer
{

    /**
     * 消费者实例
     *
     * @var
     */
    protected $consumer;

    /**
     * config实例
     *
     * @var array
     */
    protected $config;


    /**
     * 订阅的topic列表
     * @var array
     */
    protected $subscribeTopicList = [];

    /**
     * KafKaProducer constructor.
     * @param array $config
     */
    public function __construct(array $config)
    {
        $this->config = $config;
    }

    /**
     * 消费kafka消息， 基于 HIGH LEVEL Consumer
     * @link http://orchome.com/10
     *
     * @param $queue
     * @return null
     * @throws \Exception
     */
    public function consume($queue)
    {
        $consumer = $this->getConsumer($queue);
        // 消费个1秒
        if ($message = $consumer->consume(1000)) {
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $consumer->commit($message);
                    return $message;
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }

    /**
     * kafka配置
     *
     * @return \RdKafka\Conf
     */
    protected function getKafkaConfig()
    {
         $kafConfig = new KafkaConfig();
         return $kafConfig->bootstrapConfig($this->config);
    }


    /**
     * RD-KAFKA 消费者 实例
     *
     * @param $queue
     * @return Consumer
     */
    public function getConsumer($queue)
    {
        if (!isset($this->consumer))  {

            // topic conf的设置
            $topicConf = new TopicConf();
            $topicConf->set('auto.offset.reset', 'smallest'); // 重置位置

            $conf = $this->getKafkaConfig();
            $conf->setDefaultTopicConf($topicConf);

            // 实例化一个Consumer
            $this->consumer = new Consumer($conf);
        }
        return $this->consumer;
    }

    /**
     * 订阅
     *
     * @param $queue
     * @return mixed
     */
    public function subscribe($queue)
    {
       // 如果在topic中没有则加进去
        if (!in_array($queue, $this->subscribeTopicList)) {
            $this->subscribeTopicList[] = $queue;
            $this->consumer->subscribe([$queue]);
        }
        return $this->consumer;
    }


}
