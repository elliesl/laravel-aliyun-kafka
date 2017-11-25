<?php

return [

    'driver' => 'kafka',

    // your appkey
    'sasl_plain_username' => env('KAFKA_SASL_PLAIN_USERNAME', 'aliyun_user_name'),

    //your app_secret 后10位
    'sasl_plain_password' => env('KAFKA_SASL_PLAIN_PASSWORD', 'aliyun_password'),

    // 初始化服务器
    'bootstrap_servers' => env('KAFKA_BOOTSTRAP_SERVERS', 'bootstrap_servers'),

    'queue' => env('KAFKA_QUEUE', 'default'),

    'ssl.ca.location' => 'the ca-cert path',

    // 最大尝试次数
    'max.tries' => '5',

    // 消费者ID
    'consumer_id' => ENV('KAFKA_CONSUMER_ID', 'your consumer id'),




];
