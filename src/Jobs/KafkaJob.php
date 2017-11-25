<?php
namespace LaravelAliYunKafka\Jobs;
use LaravelAliYunKafka\KafkaQueue;
use Illuminate\Container\Container;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Contracts\Queue\Job as JobContract;

class KafkaJob extends Job implements JobContract
{

    protected $job;

    protected $kafka;

    protected $container;

    public function __construct(Container $container, $job, KafkaQueue $kafka, $queue)
    {
        $this->job = $job;
        $this->container = $container;
        $this->kafka = $kafka;
        $this->queue = $queue;
    }

    /**
     * 获取原始数据
     *
     * @return mixed
     */
    public function getRawBody()
    {
        return $this->job->payload;
    }


    /**
     * Release the job back into the queue.
     *
     * @param  int  $delay
     * @return mixed
     */
    public function release($delay = 0)
    {
        parent::release($delay);
        $this->delete();
        return $this->kafka->release($this->queue, $this->job, $delay);
    }


    /**
     * 删除队列消息
     */
    public function delete()
    {
        parent::delete();
        $this->kafka->getConsumer()->commitAsync($this->job);
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->job->key ?? null;
    }

    /**
     * @return int
     */
    public function attempts()
    {
        return ($this->payload()['attempts'] ?? null) + 1;
    }
}