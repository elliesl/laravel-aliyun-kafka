<?php
namespace Rdkafka;
use RdKafka\TopicConf;
use RdKafka\KafkaConsumer as Consumer;
use Illuminate\Support\Facades\Log;
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


    protected $topicList = [];

    /**
     * KafKaProducer constructor.
     * @param array $config
     */
    public function __construct(array $config)
    {
        $this->config = $config;
    }

    /**
     * 消费消息
     *
     * @param $queue
     * @return null
     * @throws \Exception
     */
    public function consume($queue)
    {
        $consumer = $this->consumer($queue);
        echo '开始消费'."\n";
        $message = $consumer->consume(20 * 1000);
        echo '结束消费'."\n";
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                print_r($message);
                return $message;
//                $consumer->commit($message);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                return null;
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                return null;
                break;
            default:
                throw new \Exception($message->errstr(), $message->err);
                break;
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
     * RD-KAFKA 消费者 实例
     *
     * @param $queue
     * @return Consumer
     */
    public function consumer($queue)
    {
        if (!isset($this->consumer))  {
            $conf = $this->kafkaConfig()->bootstrapConfig($this->config);
            $topicConf = new TopicConf();
//            $topicConf->set('enable.auto.commit', 'false');
            $topicConf->set('auto.offset.reset', 'smallest');
            $conf->setDefaultTopicConf($topicConf);
            $this->consumer = new Consumer($conf);
            $this->topicList[] = $queue;
            $this->consumer->subscribe([$queue]);
            Log::info('进来了，厉害');
        }

        // 如果在topic中没有则加进去
        if (!in_array($queue, $this->topicList)) {
            $this->topicList[] = $queue;
            $this->consumer->subscribe([$queue]);
        }
        return $this->consumer;
    }
}