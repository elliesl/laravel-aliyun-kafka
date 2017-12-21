## 安装 librdkafka

    git clone https://github.com/edenhill/librdkafka.git     
    cd librdkafka && ./configure && make && make install

## 安装 php-rdkafka

    pecl install rdkafka    
    add extension=rdkafka.so to your php.ini
   
##  安装 laravel-aliyun-rdkafka
   
    composer require elliesl/laravel-aliyun-rdkafka:dev-master
    // config/app.php    
    \LaravelAliYunKafka\LaravelKafkaServiceProvider::class
    // config/queue.php
    'kafka' => [
        'driver' => 'kafka',
        'sasl_plain_username' => env('KAFKA_SASL_PLAIN_USERNAME', 'YOUR AK'), // 阿里云 ak
        'sasl_plain_password' => env('KAFKA_SASL_PLAIN_PASSWORD', 'YOUR AC'),,// 阿里云 ac后10位
        'bootstrap_servers' => "kafka-ons-internet.aliyun.com:8080", // broker
        'ssl.ca.location' => storage_path('config/ca-cert'), // cr 证书 下载 https://help.aliyun.com/document_detail/52376.html
        'message.send.max.retries' => 5, 
        'queue' => env('KAFKA_TOPIC', 'YOUR TOPIC'),  // 这里填入你在阿里云控制台配置的topic
        'consumer_id' => env('KAFKA_CONSUMER_ID', 'YOU CONSUMER ID'), // 消费者ID，你在阿里云控制台配置的消费之ID
        'log_level' => LOG_DEBUG // 日志等级
    ],
    
### 使用方法:  
   php artisan queue:work kafka --tries=3