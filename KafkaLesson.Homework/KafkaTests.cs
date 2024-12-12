using System.Text;
using Dodo.Kafka.Configuration;
using Dodo.Kafka.Producer;
using ProducerConfig = Dodo.Kafka.Producer.ProducerConfig;

namespace KafkaLesson.Homework;

public class KafkaTests
{
    private const string TopicName = "kafka.homework.v1";

    [Test]
    public async Task ConsumedEventsOrderTest()
    {
        var kafkaContainer = await KafkaContainerFactory.CreateAndStart();
        var kafkaConnectionString = kafkaContainer.GetConnectionString();

        var eventsForProduce = InitialData.GetEvents();

        var producer = CreateProducer(TopicName, kafkaConnectionString);
        foreach (var ev in eventsForProduce)
        {
            await producer.Publish(ev, CancellationToken.None);
            Console.WriteLine($"Produced event: {ev}");
        }

        using var consumerHost = new ConsumerHost(TopicName, kafkaConnectionString);
        await consumerHost.StartAsync();

        await WaitForConsume(eventsForProduce.Count);

        Asserts.AssertProcessedEvents();
    }

    private IProducer CreateProducer(string topicName, string kafkaConnectionString)
    {
        var config = new ProducerConfig { ConnectionString = kafkaConnectionString };
        config.ApplyKafkaOptions(config);

        return new ProducerBuilder(
                config: config,
                c => c.AddEventMapping<TestEvent>(
                    getKey: e => Encoding.ASCII.GetBytes(e.Id.ToString()),
                    topic: topicName))
            .Build();
    }

    private async Task WaitForConsume(int expectedConsumedEventCount)
    {
        while (true)
        {
            var consumedEvents = EventStorage.Instance.GetConsumedEvents();
            if (consumedEvents.Count >= expectedConsumedEventCount)
            {
                Console.WriteLine($"All {expectedConsumedEventCount} events consumed.");
                break;
            }

            await Task.Delay(10);
        }

        await Task.CompletedTask;
    }
}
