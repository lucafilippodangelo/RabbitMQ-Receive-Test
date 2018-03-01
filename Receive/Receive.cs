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
            //receiveVersion1(); //LD STEP001
            //receiveVersion2(); //LD STEP002
            //receiveVersion3(); //LD STEP003
            //receiveVersion4(); //LD STEP004
            receiveVersion5(); //LD STEP005
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

        #region region // LD STEP003
        /// <summary>
        /// this method simulate a consumer getting messages in broadcast
        /// HERE WE INTRODUCE THE QUEUE BIND
        /// </summary>
        private static void receiveVersion3()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                //LD STEP003B
                channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                //LD STEP003C
                var queueName = channel.QueueDeclare().QueueName;

                //LD STEP003D - the "logs" exchange declared in the "sender" 
                // will append messages to the queue declared in the "receiver"
                channel.QueueBind(queue: queueName,
                                  exchange: "logs",
                                  routingKey: "");

                Console.WriteLine(" [*] Waiting for logs");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] {0}", message);
                };

                channel.BasicConsume(queue: queueName,
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
        #endregion

        #region region // LD STEP004
        /// <summary>
        /// Adding a feature to //LD STEP003, we're going to make it possible to subscribe only to a subset of the messages.
        /// </summary>
        private static void receiveVersion4()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                //LD STEP004C
                channel.ExchangeDeclare(exchange: "direct_logs",
                                        type: "direct");

                //LD queue declaration just in "receiver"
                var queueName = channel.QueueDeclare().QueueName;

                //LD STEP004A
                // the "direct_logs" exchange declared in the "sender" 
                // will append onlt the messages with "routingKey: "black" 
                // to the "queueName" declared in the "receiver"
                channel.QueueBind(queue: queueName,
                                  exchange: "direct_logs",
                                  routingKey: "orange"); //severity

                Console.WriteLine(" [*] Waiting for logs");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;
                    Console.WriteLine(" [x] Received '{0}':'{1}'", routingKey, message);
                };

                channel.BasicConsume(queue: queueName,
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
        #endregion

        #region region // LD STEP005
        /// <summary>
        /// Use of Topic Exchange. Implementation of updates to the code in order to subscribe
        /// to not only logs based on severity, but also based on the source which emitted the log./// </summary>
        private static void receiveVersion5()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");
                var queueName = channel.QueueDeclare().QueueName;

                //LD STEP005B
                channel.QueueBind(queue: queueName,
                                  exchange: "topic_logs",
                                  routingKey: "*.orange"); 

                Console.WriteLine(" [*] Waiting for logs");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;
                    Console.WriteLine(" [x] Received '{0}':'{1}'", routingKey, message);
                };

                channel.BasicConsume(queue: queueName,
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
        #endregion
    }
}




