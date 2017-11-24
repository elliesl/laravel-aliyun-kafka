<?php

namespace Rdkafka;

use Illuminate\Support\ServiceProvider;
use Rdkafka\Connectors\KafkaConnector;
use Illuminate\Support\Facades\Queue;
class KafkaServiceProvider extends ServiceProvider
{

    /**
     *
     * @var bool
     */
    protected $defer = true;

    /**
     * Bootstrap the application services.
     *
     * @return void
     */
    public function boot()
    {
        Queue::extend('kafka', function (){
            return new KafkaConnector;
        });

        if ($this->app->runningInConsole()) {
            $this->commands([
                KafkaQueueCommand::class,
            ]);
        }
    }

    /**
     * Register the application services.
     *
     * @return void
     */
    public function register()
    {
        // register the command
    }
}
