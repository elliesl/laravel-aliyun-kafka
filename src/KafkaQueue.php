<?php
namespace LaravelAliYunKafka;

use LaravelAliYunKafka\Jobs\KafkaJob;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use LaravelAliYunKafka\KafkaConsumer;
use LaravelAliYunKafka\KafKaProducer;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
class KafkaQueue extends Queue implements QueueContract
{

    /**
     * kafka 消息生成者
     *
     * @var
     */
    protected $producer;

    /**
     * kafka 消息消费者
     *
     * @var
     */
    protected $consumer;

    /**
     * The name of the default topic.
     *
     * @var string
     */
    protected $default;

    protected $key;

    protected $randomId;

    public function __construct(array $config, KafKaProducer $producer, KafkaConsumer $consumer, $default)
    {
        $this->producer = $producer;

        $this->default = $default;

        $this->consumer = $consumer;
    }


    /**
     * @param null $queue
     * @return int
     */
    public function size($queue = null)
    {
        // kafka 不带这个玩意
        return 1;
    }


    /**
     * 向kafka发送消息
     *
     * @param object|string $job
     * @param string $data
     * @param null $queue
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue);
    }

    /**
     * 向kafka消息队列发送消息
     *
     * @param string $payload
     * @param null $queue
     * @param array $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // 获取队列
        $queue = $this->getQueue($queue);
        // 生产消息
        $randomId  = $this->getRandomId();
        $this->producer->produce($payload, $queue, $randomId);
        return $randomId;
    }

    /**
     * 取出数据
     *
     * @param null $queue
     * @return null | mixed
     */
    public function pop($queue = null)
    {
        $queue = $this->getQueue($queue);

        if ($job = $this->getNextAvailableJob($queue)) {
            return new KafkaJob($this->container, $job, $this, $queue);
        }
    }

    /**
     * Release a reserved job back onto the queue.
     *
     * @param  string  $queue
     * @param  \Rdkafka\Jobs\KafkaJob  $job
     * @param  int  $delay
     * @return mixed
     */
    public function release($queue, $job, $delay)
    {
        if ($delay > 0) {
            return $this->later($delay, $job, $job->payload, $queue);
        } else {
            return $this->pushRaw($job->payload, $job);
        }
    }

    /**
     * create a payload
     *
     * @param  string  $job
     * @param  mixed   $data
     * @return string
     */
    protected function createPayloadArray($job, $data = '')
    {
        return array_merge(parent::createPayloadArray($job, $data), [
            'attempts' => 0,
            'id'       => $this->getRandomId()
        ]);
    }

    /**
     * @param $queue
     * @return null
     */
    protected function getNextAvailableJob($queue)
    {
        $message = $this->consumer->consume($queue);
        if ($message) {
            $payload = json_decode($message->payload, true);
            $payload['attempts']++;
            $this->randomId = $payload['id'];
            $message->payload = json_encode($payload);
        }
        return $message ?? null;
    }

    /**
     * 延迟执行
     *
     * @param \DateInterval|\DateTimeInterface|int $delay
     * @param object|string $job
     * @param string $data
     * @param null $queue
     * @throws \Exception
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        // The Kafka does not support later
        throw new \Exception('kafka not support delay until now');
    }

    /**
     * Get the queue or return the default.
     *
     * @param  string|null  $queue
     * @return string
     */
    public function getQueue($queue)
    {
        return $queue ?: $this->default;
    }

    /**
     * 创建一个随机ID
     *
     * @return string
     */
    protected function getRandomId()
    {
        return $this->randomId ?? Str::random(32);
    }

    /**
     * @return \LaravelAliYunKafka\KafkaConsumer
     */
    public function getConsumer()
    {
        return $this->consumer;
    }

}
