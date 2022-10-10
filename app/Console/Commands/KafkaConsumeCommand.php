<?php

namespace App\Console\Commands;

use App\Models\Contact;
use Illuminate\Console\Command;
use Junges\Kafka\Facades\Kafka;
use Junges\Kafka\Contracts\KafkaConsumerMessage;
class KafkaConsumeCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'kafka:consume {topic}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        $topic = $this->argument('topic');
        $consumer = Kafka::createConsumer([$topic])->withSasl(new \Junges\Kafka\Config\Sasl(
            password: config('kafka.secret'),
            username: config('kafka.key'),
            mechanisms: 'PLAIN',
            securityProtocol: 'SASL_SSL',
        ))
            ->withAutoCommit()
            ->withHandler(function (KafkaConsumerMessage $message) {
                $body =  $message->getBody();
              Contact::create([
                'name'=>$body['name'],
                'email'=>$body['email'],
              ]);
            })->build();
        $message = $consumer->consume();
        return Command::SUCCESS;
    }
}
