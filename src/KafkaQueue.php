<?php
namespace Rdkafka;

use Rdkafka\Jobs\KafkaJob;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Rdkafka\KafkaConsumer;
use Rdkafka\KafKaProducer;
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

    public function __construct(array $config, KafKaProducer $producer, KafkaConsumer $consumer, $default)
    {
        $this->producer = $producer;

        $this->default = $default;

        $this->consumer = $consumer;
    }

    public function size($queue = null)
    {
        // The kafka does not support size
    }


    /**
     * 向kafka发送消息
     *
     * @param object|string $job
     * @param string $data
     * @param null $queue
     * @return void
     */
    public function push($job, $data = '', $queue = null)
    {
        $this->producer->produce($this->createPayload($job, $data), $queue, $this->getRandomId());
    }

    /**
     * 向kafka消息队列发送消息
     *
     * @param string $payload
     * @param null $queue
     * @param array $options
     * @return void
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $this->producer->produce($payload, $queue, $this->getRandomId());
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
     * @return void
     */
    public function release($queue, $job, $delay)
    {
        $this->producer->produce($job->payload, $queue, $job->key);
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
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        // The Kafka does not support later
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
        return Str::random(32);
    }

}
