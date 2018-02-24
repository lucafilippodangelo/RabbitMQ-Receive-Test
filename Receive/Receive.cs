using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace Receive
{
    class Receive
    {
        public static void Main()
        {
            //LD STEP001
            //receiveVersion1();

            //LD STEP002
            receiveVersion2();
        }

        #region region //LD STEP001
        /// <summary>
        /// unlike the publisher which publishes a single message, we'll keep the consumer running 
        /// //continuously to listen for messages and print them out.
        /// </summary>
        /// //LD STEP001
        private static void receiveVersion1()
        {
            //Setting up is the same as the publisher; we open a connection and a channel, and declare the queue 
            //from which we're going to consume. Note this matches up with the queue that send publishes to.
            //Note that we declare the queue here as well. Because we might start the consumer before the publisher, 
            //we want to make sure the queue exists before we try to consume messages from it.
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "hello", durable: false, exclusive: false, autoDelete: false, arguments: null);

                //We're about to tell the server to deliver us the messages from the queue. Since it will push us messages 
                //asynchronously, we provide a callback. That is what EventingBasicConsumer.Received event handler does.
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);
                };

                //don't know what exactly this code is doing
                channel.BasicConsume(queue: "hello", autoAck: true, consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
                }
            }
        #endregion

        #region region // LD STEP002
        private static void receiveVersion2()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "task_queue",
                                     durable: true, //LD STEP002B
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                //LD STEP002D
                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                Console.WriteLine(" [*] Waiting for messages in task_queue");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);

                    //  it needs to fake a second of work for every dot in the message body. It will handle messages 
                    //delivered by RabbitMQ and perform the task
                    int dots = message.Split('.').Length - 1;
                    Thread.Sleep(dots * 1000);

                    Console.WriteLine(" [x] Done");

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };

                //LD STEP002A
                channel.BasicConsume(queue: "task_queue", autoAck: true, consumer: consumer);
                //channel.BasicConsume(queue: "task_queue", autoAck: false, consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
        #endregion

    }
}




